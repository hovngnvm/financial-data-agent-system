import asyncio
import json
from collections import defaultdict
from datetime import datetime
from typing import Any
import pandas as pd
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from redis.asyncio import Redis

from src.tools import tool_calculate_technical_indicators
from src.database import db_manager
from src.vector_db import vector_db_manager
from src.utils.chunking import advanced_parent_child_chunker
from src.config import settings
from src.utils.logger import get_logger

PRICE_WINDOW_SIZE: int = 30
DEFAULT_BATCH_TIMEOUT_MS: int = 1000
DEFAULT_BATCH_MAX_RECORDS: int = 100
RETRY_BACKOFF_SECONDS: int = 5
PARENT_CHUNK_SIZE: int = 1200
CHILD_CHUNK_SIZE: int = 250

logger = get_logger(__name__)

class PriceCacheManager:
    def __init__(self):
        self.redis = Redis(host=settings.redis_host, port=settings.redis_port)
        self._in_memory_cache = {}
        self.use_redis = True
        self._pinged = False

    async def _check_redis(self) -> None:
        if not self._pinged:
            try:
                await asyncio.wait_for(self.redis.ping(), timeout=1.0)
                self.use_redis = True
                logger.info("[Price Cache] Successfully connected to Redis. Using Redis Sorted Sets for caching.")
            except Exception as e:
                self.use_redis = False
                logger.warning(f"[Price Cache] Redis connection failed ({e}). Falling back to in-memory caching.")
            self._pinged = True

    async def _load_from_clickhouse(self, symbol: str) -> list[dict]:
        logger.debug(f"[Price Cache] Cache miss for symbol {symbol}. Warming up from ClickHouse...")
        loop = asyncio.get_running_loop()
        history_df = await loop.run_in_executor(
            None,
            lambda: db_manager.client.query_df(
                "SELECT timestamp, symbol, price, volume FROM prices WHERE symbol = %(symbol)s ORDER BY timestamp DESC LIMIT %(limit)s",
                parameters={"symbol": symbol, "limit": PRICE_WINDOW_SIZE}
            )
        )
        if history_df.empty:
            return []
        history_df = history_df.iloc[::-1].reset_index(drop=True)
        records = []
        for _, row in history_df.iterrows():
            ts_str = row['timestamp'].strftime("%Y-%m-%d %H:%M:%S.%f") if isinstance(row['timestamp'], datetime) else str(row['timestamp'])
            records.append({
                "timestamp": ts_str,
                "symbol": row['symbol'],
                "price": float(row['price']),
                "volume": float(row['volume'])
            })
        return records

    async def get_and_update_window(self, symbol: str, new_records: list[dict]) -> list[dict]:
        """
        Gets sliding window of prices for a symbol, updates it with new records,
        trims to PRICE_WINDOW_SIZE, and returns the full window.
        """
        await self._check_redis()

        if self.use_redis:
            try:
                count = await self.redis.zcard(f"prices:window:{symbol}")
                if count == 0:
                    history = await self._load_from_clickhouse(symbol)
                    if history:
                        for record in history:
                            score = datetime.strptime(record["timestamp"], "%Y-%m-%d %H:%M:%S.%f").timestamp()
                            await self.redis.zadd(f"prices:window:{symbol}", {json.dumps(record): score})
                
                for record in new_records:
                    score = datetime.strptime(record["timestamp"], "%Y-%m-%d %H:%M:%S.%f").timestamp()
                    await self.redis.zadd(f"prices:window:{symbol}", {json.dumps(record): score})
                
                await self.redis.zremrangebyrank(f"prices:window:{symbol}", 0, -(PRICE_WINDOW_SIZE + 1))
                cached_raw = await self.redis.zrange(f"prices:window:{symbol}", 0, -1)
                return [json.loads(x.decode('utf-8')) for x in cached_raw]
            except Exception as e:
                logger.warning(f"[Price Cache] Redis error: {e}. Falling back to in-memory caching.")
                self.use_redis = False

        if symbol not in self._in_memory_cache:
            history = await self._load_from_clickhouse(symbol)
            self._in_memory_cache[symbol] = history

        self._in_memory_cache[symbol].extend(new_records)
        self._in_memory_cache[symbol].sort(key=lambda x: x["timestamp"])
        
        seen = set()
        unique = []
        for r in self._in_memory_cache[symbol]:
            if r["timestamp"] not in seen:
                seen.add(r["timestamp"])
                unique.append(r)
                
        self._in_memory_cache[symbol] = unique[-PRICE_WINDOW_SIZE:]
        return self._in_memory_cache[symbol]

price_cache = PriceCacheManager()

async def process_market_batch(messages: list[Any], producer: AIOKafkaProducer) -> None:
    if not messages:
        return
        
    records = []
    dlq_promises = []
    for msg in messages:
        try:
            val = json.loads(msg.value.decode('utf-8'))
            if "payload" not in val or "symbol" not in val["payload"] or "price" not in val["payload"] or "volume" not in val["payload"]:
                raise KeyError("Missing required data fields (symbol/price/volume) in payload.")
                
            raw_ts = val.get("ingest_timestamp")
            try:
                datetime.strptime(raw_ts, "%Y-%m-%d %H:%M:%S.%f")
                ts_str = raw_ts
            except ValueError:
                try:
                    dt = datetime.strptime(raw_ts, "%Y-%m-%d %H:%M:%S")
                    ts_str = dt.strftime("%Y-%m-%d %H:%M:%S.000000")
                except ValueError:
                    dt = datetime.now()
                    ts_str = dt.strftime("%Y-%m-%d %H:%M:%S.%f")

            record = {
                "timestamp": ts_str,
                "symbol": val["payload"]["symbol"],
                "price": float(val["payload"]["price"]),
                "volume": float(val["payload"]["volume"])
            }
            records.append(record)
        except Exception as e:
            logger.error(f"Failed to parse market message at offset {msg.offset}: {e}. Routing to DLQ...")
            dlq_payload = {
                "error_message": str(e),
                "fail_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                "original_topic": msg.topic,
                "partition": msg.partition,
                "offset": msg.offset,
                "raw_payload": msg.value.decode('utf-8', errors='replace')
            }
            dlq_promises.append(producer.send_and_wait(settings.topic_market_dlq, dlq_payload))
            
    if dlq_promises:
        await asyncio.gather(*dlq_promises)
            
    if not records:
        return
        
    grouped_new_records = defaultdict(list)
    for r in records:
        grouped_new_records[r["symbol"]].append(r)
        
    enriched_records = []
    
    try:
        for symbol, new_sym_records in grouped_new_records.items():
            window_records = await price_cache.get_and_update_window(symbol, new_sym_records)
            if not window_records:
                continue
                
            combined_df = pd.DataFrame(window_records)
            cols_to_drop = [c for c in ['sma_5', 'sma_20', 'rsi', 'macd', 'macd_signal', 'SMA_5', 'SMA_20', 'RSI', 'MACD', 'MACD_Signal'] if c in combined_df.columns]
            combined_df = combined_df.drop(columns=cols_to_drop)
                
            calculated_df = tool_calculate_technical_indicators(combined_df)
            new_calculated_rows = calculated_df.tail(len(new_sym_records))
            enriched_records.append(new_calculated_rows)
            
        if enriched_records:
            final_df = pd.concat(enriched_records, ignore_index=True)
            db_manager.ingest_df(
                dataframe=final_df, 
                table_name="prices", 
                mode="append"
            )
            logger.info(f"Enriched and stored {len(final_df)} records to ClickHouse.")
    except Exception as e:
        logger.error(f"Market consumer enrichment processing failed: {e}")
        raise e

async def process_news_batch(messages: list[Any], producer: AIOKafkaProducer) -> None:
    if not messages:
        return
        
    records = []
    dlq_promises = []
    for msg in messages:
        try:
            val = json.loads(msg.value.decode('utf-8'))
            if "payload" not in val or "title" not in val["payload"] or "summary" not in val["payload"] or "link" not in val["payload"]:
                raise KeyError("Missing required data fields (title/summary/link) in payload.")
                
            record = {
                "ingest_timestamp": val.get("ingest_timestamp"),
                "title": val["payload"]["title"],
                "summary": val["payload"]["summary"],
                "link": val["payload"]["link"]
            }
            records.append(record)
        except Exception as e:
            logger.error(f"Failed to parse news message at offset {msg.offset}: {e}. Routing to DLQ...")
            dlq_payload = {
                "error_message": str(e),
                "fail_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                "original_topic": msg.topic,
                "partition": msg.partition,
                "offset": msg.offset,
                "raw_payload": msg.value.decode('utf-8', errors='replace')
            }
            dlq_promises.append(producer.send_and_wait(settings.topic_news_dlq, dlq_payload))
            
    if dlq_promises:
        await asyncio.gather(*dlq_promises)
            
    if not records:
        return
        
    all_structured_chunks = []
    for item in records:
        full_text_content = f"Title: {item['title']}\nSummary: {item['summary']}"
        formatted_chunks = advanced_parent_child_chunker(
            text=full_text_content,
            source_link=item['link'],
            parent_size=PARENT_CHUNK_SIZE,
            child_size=CHILD_CHUNK_SIZE
        )
        
        for chunk in formatted_chunks:
            chunk["timestamp"] = item['ingest_timestamp']
            
        all_structured_chunks.extend(formatted_chunks)
        
    if all_structured_chunks:
        try:
            vector_db_manager.ingest_data(chunks_data=all_structured_chunks)
            logger.info(f"Chunked and stored {len(all_structured_chunks)} news blocks to Qdrant.")
        except Exception as e:
            logger.error(f"News consumer Qdrant storage failure: {e}")
            raise e

async def run_market_consumer(producer: AIOKafkaProducer) -> None:
    consumer = AIOKafkaConsumer(
        settings.topic_market,
        bootstrap_servers=settings.kafka_broker,
        group_id="finagent_market_group",
        auto_offset_reset="earliest",
        enable_auto_commit=False
    )
    await consumer.start()
    logger.info("[Market Consumer] Price data consumer started (Manual Commit)...")
    try:
        while True:
            try:
                result = await consumer.getmany(timeout_ms=DEFAULT_BATCH_TIMEOUT_MS, max_records=DEFAULT_BATCH_MAX_RECORDS)
                if not result:
                    continue
                    
                for tp, msgs in result.items():
                    if msgs:
                        await process_market_batch(msgs, producer)
                        last_offset = msgs[-1].offset
                        await consumer.commit({tp: last_offset + 1})
            except Exception as e:
                logger.error(f"Error in Market Consumer loop (Commit rejected): {e}")
                await asyncio.sleep(RETRY_BACKOFF_SECONDS)
    finally:
        await consumer.stop()

async def run_news_consumer(producer: AIOKafkaProducer) -> None:
    consumer = AIOKafkaConsumer(
        settings.topic_news,
        bootstrap_servers=settings.kafka_broker,
        group_id="finagent_news_group",
        auto_offset_reset="earliest",
        enable_auto_commit=False
    )
    await consumer.start()
    logger.info("[News Consumer] News consumer started (Manual Commit)...")
    try:
        while True:
            try:
                result = await consumer.getmany(timeout_ms=DEFAULT_BATCH_TIMEOUT_MS, max_records=10)
                if not result:
                    continue
                    
                for tp, msgs in result.items():
                    if msgs:
                        await process_news_batch(msgs, producer)
                        last_offset = msgs[-1].offset
                        await consumer.commit({tp: last_offset + 1})
            except Exception as e:
                logger.error(f"Error in News Consumer loop (Commit rejected): {e}")
                await asyncio.sleep(RETRY_BACKOFF_SECONDS)
    finally:
        await consumer.stop()

async def main() -> None:
    logger.info("[Consumer Pipeline] Starting consumer pipeline...")
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_broker,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    await producer.start()
    try:
        await asyncio.gather(
            run_market_consumer(producer),
            run_news_consumer(producer)
        )
    finally:
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(main())
