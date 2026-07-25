import asyncio
import os
import json
import pandas as pd
from datetime import datetime
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from src.tools import tool_calculate_technical_indicators
from src.database import ingest_data_to_db, db_manager
from src.vector_db import ingest_data_to_qdrant
from src.chunking import advanced_parent_child_chunker
from src.config import settings
from src.logger import get_logger

logger = get_logger(__name__)

class PriceCacheManager:
    def __init__(self):
        from redis.asyncio import Redis
        self.redis = Redis(host=settings.redis_host, port=settings.redis_port)
        self._in_memory_cache = {}
        self.use_redis = True
        self._pinged = False

    async def _check_redis(self):
        if not self._pinged:
            try:
                # Set a short timeout for the ping
                await asyncio.wait_for(self.redis.ping(), timeout=1.0)
                self.use_redis = True
                logger.info("-> [Price Cache]: Successfully connected to Redis. Using Redis Sorted Sets for caching.")
            except Exception as e:
                self.use_redis = False
                logger.warning(f"-> [Price Cache]: Redis connection failed ({str(e)}). Falling back to in-memory caching.")
            self._pinged = True

    async def get_and_update_window(self, symbol: str, new_records: list[dict]) -> list[dict]:
        """
        Gets the sliding window of last 30 prices for a symbol, updates it with new records,
        trims to 30, and returns the full window. Performs lazy database loading on cache miss.
        """
        await self._check_redis()
        
        # Helper to query historical data from ClickHouse
        async def load_from_clickhouse():
            logger.info(f"-> [Price Cache]: Cache miss for symbol {symbol}. Warming up from ClickHouse...")
            # Running synchronous database query in executor to avoid blocking event loop
            loop = asyncio.get_running_loop()
            history_df = await loop.run_in_executor(
                None,
                lambda: db_manager.client.query_df(
                    "SELECT timestamp, symbol, price, volume FROM prices WHERE symbol = %(symbol)s ORDER BY timestamp DESC LIMIT 30",
                    parameters={"symbol": symbol}
                )
            )
            if history_df.empty:
                return []
            # Reverse history to be in ascending chronological order
            history_df = history_df.iloc[::-1].reset_index(drop=True)
            records = []
            for _, row in history_df.iterrows():
                # Format timestamp string consistently
                ts_str = row['timestamp'].strftime("%Y-%m-%d %H:%M:%S.%f") if isinstance(row['timestamp'], datetime) else str(row['timestamp'])
                records.append({
                    "timestamp": ts_str,
                    "symbol": row['symbol'],
                    "price": float(row['price']),
                    "volume": float(row['volume'])
                })
            return records

        if self.use_redis:
            try:
                # Check if key exists (zcard)
                count = await self.redis.zcard(f"prices:window:{symbol}")
                if count == 0:
                    history = await load_from_clickhouse()
                    if history:
                        for record in history:
                            score = datetime.strptime(record["timestamp"], "%Y-%m-%d %H:%M:%S.%f").timestamp()
                            await self.redis.zadd(f"prices:window:{symbol}", {json.dumps(record): score})
                
                # Add new records
                for record in new_records:
                    score = datetime.strptime(record["timestamp"], "%Y-%m-%d %H:%M:%S.%f").timestamp()
                    await self.redis.zadd(f"prices:window:{symbol}", {json.dumps(record): score})
                
                # Trim to last 30
                await self.redis.zremrangebyrank(f"prices:window:{symbol}", 0, -31)
                
                # Fetch full window
                cached_raw = await self.redis.zrange(f"prices:window:{symbol}", 0, -1)
                return [json.loads(x.decode('utf-8')) for x in cached_raw]
            except Exception as e:
                logger.warning(f"-> [Price Cache]: Redis error: {str(e)}. Falling back to in-memory caching.")
                self.use_redis = False
                # Fall through to in-memory logic

        # In-memory fallback logic
        if symbol not in self._in_memory_cache:
            history = await load_from_clickhouse()
            self._in_memory_cache[symbol] = history

        # Append new records
        self._in_memory_cache[symbol].extend(new_records)
        
        # Sort by timestamp
        self._in_memory_cache[symbol].sort(key=lambda x: x["timestamp"])
        
        # Deduplicate on timestamp
        seen = set()
        unique = []
        for r in self._in_memory_cache[symbol]:
            if r["timestamp"] not in seen:
                seen.add(r["timestamp"])
                unique.append(r)
                
        # Keep last 30
        self._in_memory_cache[symbol] = unique[-30:]
        return self._in_memory_cache[symbol]

# Initialize global PriceCacheManager instance
price_cache = PriceCacheManager()

async def process_market_batch(messages, producer):
    if not messages:
        return
        
    records = []
    dlq_promises = []
    for msg in messages:
        try:
            val = json.loads(msg.value.decode('utf-8'))
            # Perform basic validation of the Data Contract structure
            if "payload" not in val or "symbol" not in val["payload"] or "price" not in val["payload"] or "volume" not in val["payload"]:
                raise KeyError("Missing required data fields (symbol/price/volume) in payload.")
                
            # Format timestamp consistently (ensure standard microsecond format)
            raw_ts = val.get("ingest_timestamp")
            try:
                # Validate the parsing format
                datetime.strptime(raw_ts, "%Y-%m-%d %H:%M:%S.%f")
                ts_str = raw_ts
            except ValueError:
                # If timestamp lacks microseconds, convert it to fit the pattern
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
            logger.error(f"Failed to parse market message at offset {msg.offset}: {str(e)}. Routing to DLQ...")
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
        
    # Group new records by symbol so we can process each symbol's sliding window
    from collections import defaultdict
    grouped_new_records = defaultdict(list)
    for r in records:
        grouped_new_records[r["symbol"]].append(r)
        
    enriched_records = []
    
    try:
        for symbol, new_sym_records in grouped_new_records.items():
            # Get the full 30-item sliding window (includes new records, deduplicated and sorted)
            window_records = await price_cache.get_and_update_window(symbol, new_sym_records)
            if not window_records:
                continue
                
            combined_df = pd.DataFrame(window_records)
 
            # Drop old indicator columns before recalculating to prevent duplicate columns upon converting to lowercase
            cols_to_drop = [c for c in ['sma_5', 'sma_20', 'rsi', 'macd', 'macd_signal', 'SMA_5', 'SMA_20', 'RSI', 'MACD', 'MACD_Signal'] if c in combined_df.columns]
            combined_df = combined_df.drop(columns=cols_to_drop)
                
            calculated_df = tool_calculate_technical_indicators(combined_df)
            
            # We only want to append the new records to PostgreSQL, which correspond to the tail of the calculated_df
            new_calculated_rows = calculated_df.tail(len(new_sym_records))
            enriched_records.append(new_calculated_rows)
            
        if enriched_records:
            final_df = pd.concat(enriched_records, ignore_index=True)
            # Store enriched records into PostgreSQL
            ingest_data_to_db(
                dataframe=final_df, 
                table_name="prices", 
                mode="append"
            )
            logger.info(f"-> [Market Consumer]: Enriched and stored {len(final_df)} records to PostgreSQL.")
    except Exception as e:
        logger.error(f"-> [Market Consumer] Enrichment processing failed: {str(e)}")
        raise e

async def process_news_batch(messages, producer):
    if not messages:
        return
        
    records = []
    dlq_promises = []
    for msg in messages:
        try:
            val = json.loads(msg.value.decode('utf-8'))
            if "payload" not in val or "title" not in val["payload"] or "summary" not in val["payload"] or "link" not in val["payload"]:
                raise KeyError("Thiếu trường dữ liệu bắt buộc (title/summary/link) trong payload.")
                
            record = {
                "ingest_timestamp": val.get("ingest_timestamp"),
                "title": val["payload"]["title"],
                "summary": val["payload"]["summary"],
                "link": val["payload"]["link"]
            }
            records.append(record)
        except Exception as e:
            logger.error(f"Failed to parse news message at offset {msg.offset}: {str(e)}. Routing to DLQ...")
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
        
    pandas_df = pd.DataFrame(records)
    all_structured_chunks = []
    
    for _, row in pandas_df.iterrows():
        full_text_content = f"Tiêu đề: {row['title']}\nTóm tắt: {row['summary']}"
        formatted_chunks = advanced_parent_child_chunker(
            text=full_text_content,
            source_link=row['link'],
            parent_size=1200,
            child_size=250
        )
        
        for chunk in formatted_chunks:
            chunk["timestamp"] = row['ingest_timestamp']
            
        all_structured_chunks.extend(formatted_chunks)
        
    if all_structured_chunks:
        try:
            ingest_data_to_qdrant(chunks_data=all_structured_chunks)
            logger.info(f"-> [News Consumer]: Chunked and stored {len(all_structured_chunks)} news blocks to Qdrant.")
        except Exception as e:
            logger.error(f"-> [News Consumer] Qdrant storage failure: {str(e)}")
            raise e

async def run_market_consumer(producer):
    consumer = AIOKafkaConsumer(
        settings.topic_market,
        bootstrap_servers=settings.kafka_broker,
        group_id="finagent_market_group",
        auto_offset_reset="earliest",
        enable_auto_commit=False  # Disable auto-commit
    )
    await consumer.start()
    logger.info("-> [Market Consumer]: Price data consumer started (Manual Commit)...")
    try:
        while True:
            try:
                result = await consumer.getmany(timeout_ms=1000, max_records=100)
                if not result:
                    continue
                    
                for tp, msgs in result.items():
                    if msgs:
                        # Process batch, will raise Exception on DB write error
                        await process_market_batch(msgs, producer)
                        # Commit offset only after successful database persistence
                        last_offset = msgs[-1].offset
                        await consumer.commit({tp: last_offset + 1})
            except Exception as e:
                logger.error(f"Error in Market Consumer loop (Commit rejected): {str(e)}")
                await asyncio.sleep(5)
    finally:
        await consumer.stop()

async def run_news_consumer(producer):
    consumer = AIOKafkaConsumer(
        settings.topic_news,
        bootstrap_servers=settings.kafka_broker,
        group_id="finagent_news_group",
        auto_offset_reset="earliest",
        enable_auto_commit=False  # Disable auto-commit
    )
    await consumer.start()
    logger.info("-> [News Consumer]: News consumer started (Manual Commit)...")
    try:
        while True:
            try:
                result = await consumer.getmany(timeout_ms=1000, max_records=10)
                if not result:
                    continue
                    
                for tp, msgs in result.items():
                    if msgs:
                        await process_news_batch(msgs, producer)
                        last_offset = msgs[-1].offset
                        await consumer.commit({tp: last_offset + 1})
            except Exception as e:
                logger.error(f"Error in News Consumer loop (Commit rejected): {str(e)}")
                await asyncio.sleep(5)
    finally:
        await consumer.stop()

async def main():
    logger.info("-> [Consumer Pipeline]: Starting consumer pipeline...")
    # Initialize shared Kafka Producer to route error messages to the DLQ
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
