import asyncio
import json
from collections import deque
from datetime import datetime
import websockets
import feedparser
from aiokafka import AIOKafkaProducer

from src.config import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

KAFKA_BROKER = settings.kafka_broker
TOPIC_MARKET = settings.topic_market
TOPIC_NEWS = settings.topic_news

# Maximum number of GUIDs retained in memory to prevent memory leaks during long-running streaming
MAX_SEEN_NEWS_CACHE = 5000

async def get_producer() -> AIOKafkaProducer:
    """Initializes and connects an asynchronous Kafka Producer."""
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    await producer.start()
    return producer

async def stream_binance_websocket(producer: AIOKafkaProducer) -> None:
    """Connects directly to the Binance trade stream via WebSockets."""
    streams = "btcusdt@trade/ethusdt@trade"
    socket_url = f"wss://stream.binance.com:9443/stream?streams={streams}"
    logger.info("Opening live WebSocket connection to Binance Trade Stream...")
    
    while True:
        try:
            async with websockets.connect(socket_url) as ws:
                while True:
                    msg = await ws.recv()
                    data = json.loads(msg)
                    
                    trade_data = data.get('data', data)
                    payload = {
                        "ingest_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                        "data_source": "binance_websocket_live",
                        "asset_class": "CRYPTO",
                        "payload": {
                            "symbol": trade_data['s'],
                            "price": float(trade_data['p']),
                            "volume": float(trade_data['q'])
                        }
                    }
                    await producer.send_and_wait(TOPIC_MARKET, payload)
                    
        except websockets.exceptions.ConnectionClosed:
            logger.warning("Binance WS connection lost. Reconnecting in 5s...")
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"Binance WS stream error: {e}")
            await asyncio.sleep(5)

async def stream_vnstock_polling(producer: AIOKafkaProducer) -> None:
    """Polls real-time price boards for selected stocks using vnstock Quote API with safe rate-limiting."""
    logger.info("Initializing REST API Polling stream via vnstock...")
    symbols = ["HPG", "FPT", "SSI", "VND", "MBB", "TCB"]
    
    # Auto-setup API key from environment if configured
    try:
        import os
        from vnstock.core import setup_api_key
        api_key = os.environ.get("VNSTOCK_API_KEY", "")
        if api_key:
            setup_api_key(api_key)
            logger.info("Configured Vnstock API Key from environment.")
    except Exception as e:
        logger.debug(f"Vnstock setup_api_key skipped: {e}")
    
    sym_idx = 0
    while True:
        sym = symbols[sym_idx % len(symbols)]
        sym_idx += 1
        try:
            from vnstock import Quote
            q = Quote(symbol=sym, source='kbs')
            df = q.intraday()
            if df is not None and not df.empty:
                latest_row = df.iloc[0]
                payload = {
                    "ingest_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                    "data_source": "vnstock_kbs_intraday",
                    "asset_class": "STOCK",
                    "payload": {
                        "symbol": sym,
                        "price": float(latest_row['price']),
                        "volume": float(latest_row['volume'])
                    }
                }
                await producer.send_and_wait(TOPIC_MARKET, payload)
                logger.debug(f"[Vnstock] Streamed live tick for {sym}: {latest_row['price']}")
            # Throttle 4 seconds per request -> 15 req/min, safely under Guest quota (20 req/min)
            await asyncio.sleep(4)
        except (Exception, SystemExit, BaseException) as e:
            # Handle rate limit without crashing producer
            logger.warning(f"[Vnstock] API rate limit or warning encountered for {sym}: {e}. Backing off 30s...")
            await asyncio.sleep(30)

async def stream_rss_news_feeds(producer: AIOKafkaProducer) -> None:
    """Scrapes financial news stories from macro feeds (Cafef / Vietstock / VnExpress)."""
    logger.info("Starting live listener for financial RSS feeds...")
    rss_urls = [
        "https://cafef.vn/thi-truong-chung-khoan.rss",
        "https://cafef.vn/kinh-te-vi-mo.rss",
        "https://cafef.vn/doanh-nghiep.rss",
        "https://vietstock.vn/rss/tin-moi-nhat.rss",
        "https://vnexpress.net/rss/kinh-doanh.rss"
    ]
    
    # Bounded cache using deque + set to eliminate memory leaks during 24/7 ingestion
    seen_guid_set = set()
    seen_guid_order = deque()
    
    while True:
        try:
            for url in rss_urls:
                feed = feedparser.parse(url)
                for entry in feed.entries[:3]:
                    guid = entry.get('id', entry.get('link', ''))
                    if guid and guid not in seen_guid_set:
                        seen_guid_set.add(guid)
                        seen_guid_order.append(guid)
                        
                        if len(seen_guid_order) > MAX_SEEN_NEWS_CACHE:
                            oldest_guid = seen_guid_order.popleft()
                            seen_guid_set.discard(oldest_guid)
                        
                        payload = {
                            "ingest_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                            "data_source": "rss_financial_feed",
                            "payload": {
                                "title": entry.get('title', ''),
                                "summary": entry.get('summary', entry.get('description', '')),
                                "link": entry.get('link', ''),
                                "published": entry.get('published', datetime.now().strftime("%Y-%m-%d"))
                            }
                        }
                        await producer.send_and_wait(TOPIC_NEWS, payload)
            await asyncio.sleep(15)  # Scan feeds periodically
        except Exception as e:
            logger.error(f"RSS news stream error: {e}")
            await asyncio.sleep(10)

async def main() -> None:
    producer = await get_producer()
    try:
        await asyncio.gather(
            stream_binance_websocket(producer),
            stream_vnstock_polling(producer),
            stream_rss_news_feeds(producer)
        )
    finally:
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(main())