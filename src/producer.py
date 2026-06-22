import asyncio
import json
from datetime import datetime
import websockets
import feedparser
from aiokafka import AIOKafkaProducer

from vnstock.core import setup_api_key
from vnstock import Quote

from src.config import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

KAFKA_BROKER: str = settings.kafka_broker
TOPIC_MARKET: str = settings.topic_market
TOPIC_NEWS: str = settings.topic_news

MAX_SEEN_NEWS_CACHE: int = 5000
VNSTOCK_POLL_INTERVAL_SECONDS: int = 4
VNSTOCK_RATE_LIMIT_BACKOFF_SECONDS: int = 30
RSS_POLL_INTERVAL_SECONDS: int = 15
RETRY_BACKOFF_SECONDS: int = 5

TRACKED_STOCK_SYMBOLS: list[str] = ["HPG", "FPT", "SSI", "VND", "MBB", "TCB"]
BINANCE_STREAMS: str = "btcusdt@trade/ethusdt@trade"
BINANCE_WS_URL: str = f"wss://stream.binance.com:9443/stream?streams={BINANCE_STREAMS}"

FINANCIAL_RSS_FEEDS: list[str] = [
    "https://cafef.vn/thi-truong-chung-khoan.rss",
    "https://cafef.vn/kinh-te-vi-mo.rss",
    "https://cafef.vn/doanh-nghiep.rss",
    "https://vietstock.vn/rss/tin-moi-nhat.rss",
    "https://vnexpress.net/rss/kinh-doanh.rss"
]

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
    logger.info("Opening live WebSocket connection to Binance Trade Stream...")
    
    while True:
        try:
            async with websockets.connect(BINANCE_WS_URL) as ws:
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
            logger.warning("Binance WS connection lost. Reconnecting in %ss...", RETRY_BACKOFF_SECONDS)
            await asyncio.sleep(RETRY_BACKOFF_SECONDS)
        except Exception as e:
            logger.error(f"Binance WS stream error: {e}")
            await asyncio.sleep(RETRY_BACKOFF_SECONDS)

async def stream_vnstock_polling(producer: AIOKafkaProducer) -> None:
    """Polls real-time price boards for selected stocks using vnstock Quote API with safe rate-limiting."""
    logger.info("Initializing REST API Polling stream via vnstock...")
    
    if setup_api_key and settings.vnstock_api_key:
        try:
            setup_api_key(settings.vnstock_api_key)
            logger.info("Configured Vnstock API Key from environment.")
        except Exception as e:
            logger.debug(f"Vnstock setup_api_key skipped: {e}")
    
    sym_idx = 0
    while True:
        sym = TRACKED_STOCK_SYMBOLS[sym_idx % len(TRACKED_STOCK_SYMBOLS)]
        sym_idx += 1
        try:
            if Quote is not None:
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
            await asyncio.sleep(VNSTOCK_POLL_INTERVAL_SECONDS)
        except Exception as e:
            logger.warning(f"[Vnstock] API rate limit or error for {sym}: {e}. Backing off {VNSTOCK_RATE_LIMIT_BACKOFF_SECONDS}s...")
            await asyncio.sleep(VNSTOCK_RATE_LIMIT_BACKOFF_SECONDS)

async def stream_rss_news_feeds(producer: AIOKafkaProducer) -> None:
    """Scrapes financial news stories from macro feeds."""
    logger.info("Starting live listener for financial RSS feeds...")
    
    seen_guids: dict[str, bool] = {}
    
    while True:
        try:
            for url in FINANCIAL_RSS_FEEDS:
                feed = feedparser.parse(url)
                for entry in feed.entries[:3]:
                    guid = entry.get('id', entry.get('link', ''))
                    if guid and guid not in seen_guids:
                        seen_guids[guid] = True
                        if len(seen_guids) > MAX_SEEN_NEWS_CACHE:
                            seen_guids.pop(next(iter(seen_guids)))
                        
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
            await asyncio.sleep(RSS_POLL_INTERVAL_SECONDS)
        except Exception as e:
            logger.error(f"RSS news stream error: {e}")
            await asyncio.sleep(RETRY_BACKOFF_SECONDS)

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