import asyncio
import json
import websockets
import feedparser
import aiohttp
from datetime import datetime
from aiokafka import AIOKafkaProducer
from vnstock import Vnstock # Real-time stock data feed for Vietnam stocks

from src.config import settings
from src.logger import get_logger

logger = get_logger(__name__)

KAFKA_BROKER = settings.kafka_broker
TOPIC_MARKET = settings.topic_market
TOPIC_NEWS = settings.topic_news

# Initialize stock client
stock_client = Vnstock()

async def get_producer():
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    await producer.start()
    return producer

async def stream_binance_websocket(producer):
    """Connects directly to the Binance trade stream via WebSockets"""
    streams = "btcusdt@trade/ethusdt@trade"
    socket_url = f"wss://stream.binance.com:9443/stream?streams={streams}"
    logger.info("-> [Bronze Ingestion]: Opening live WebSocket connection to Binance Trade Stream...")
    
    while True:
        try:
            async with websockets.connect(socket_url) as ws:
                while True:
                    msg = await ws.recv()
                    data = json.loads(msg)
                    
                    # Map to standard Data Contract format
                    trade_data = data.get('data', data)  # Support both raw single stream and combined streams format
                    payload = {
                        "ingest_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                        "data_source": "binance_websocket_live",
                        "asset_class": "CRYPTO",
                        "payload": {
                            "symbol": trade_data['s'],              # e.g., BTCUSDT
                            "price": float(trade_data['p']),         # real-time price
                            "volume": float(trade_data['q'])         # real-time quantity
                        }
                    }
                    # Send to Kafka
                    await producer.send_and_wait(TOPIC_MARKET, payload) 
                    
        except websockets.exceptions.ConnectionClosed:
            logger.warning("-> [Bronze Ingestion]: Binance WS connection lost. Reconnecting in 5s...")
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"Binance WS stream error: {str(e)}")
            await asyncio.sleep(5)

async def stream_vnstock_polling(producer):
    """Polls real-time price boards for selected stocks SSI, VND, etc. using vnstock"""
    logger.info("-> [Bronze Ingestion]: Initializing REST API Polling stream via vnstock...")
    symbols = [
            # Steel & Tech
            "HPG", "FPT", 
            # Securities
            "VIX", "SSI", "VND", 
            # Banking
            "SHB", "STB", "VPB", "MBB", "TCB",
            # Real Estate & Index Heavyweights
            "VIC", "VHM"
    ]
    
    while True:
        try:
            for sym in symbols:
                # Fetch real-time quotes via vnstock API
                stock_data = stock_client.stock(symbol=sym, source='VCI').trading.price_board()
                if not stock_data.empty:
                    latest_row = stock_data.iloc[0]
                    payload = {
                        "ingest_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                        "data_source": "vnstock_api_vci",
                        "asset_class": "STOCK",
                        "payload": {
                            "symbol": sym,
                            "price": float(latest_row['matchPrice']), # Match price
                            "volume": float(latest_row['matchVolume']) # Match volume
                        }
                    }
                    await producer.send_and_wait(TOPIC_MARKET, payload)
            await asyncio.sleep(2) # Polling rate 2 seconds to respect API rate limits
        except Exception as e:
            logger.error(f"vnstock stream error: {str(e)}")
            await asyncio.sleep(5)

async def stream_rss_news_feeds(producer):
    """Scrapes financial news stories from macro feeds (Cafef / Vietstock / VnExpress)"""
    logger.info("-> [Bronze Ingestion]: Starting live listener for financial RSS feeds...")
    rss_urls = [
        "https://cafef.vn/thi-truong-chung-khoan.rss",
        "https://cafef.vn/kinh-te-vi-mo.rss",
        "https://cafef.vn/doanh-nghiep.rss",
        "https://vietstock.vn/rss/tin-moi-nhat.rss",
        "https://vnexpress.net/rss/kinh-doanh.rss"
    ]
    seen_guid = set() # Local cache to filter duplicate news items
    
    while True:
        try:
            for url in rss_urls:
                feed = feedparser.parse(url)
                for entry in feed.entries[:3]: # Retrieve top 3 news items per cycle
                    guid = entry.get('id', entry.get('link', ''))
                    if guid not in seen_guid:
                        seen_guid.add(guid)
                        
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
            await asyncio.sleep(15) # Scan feeds every 15 seconds
        except Exception as e:
            logger.error(f"RSS news stream error: {str(e)}")
            await asyncio.sleep(10)
            
async def main():
    producer = await get_producer()
    try:
        # Asynchronous gather: launch parallel real-time ingestion streams concurrently
        await asyncio.gather(
            stream_binance_websocket(producer),
            stream_vnstock_polling(producer),
            stream_rss_news_feeds(producer)
        )
    finally:
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(main())