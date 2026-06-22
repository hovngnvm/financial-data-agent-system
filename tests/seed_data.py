import datetime
import random
import pandas as pd
from src.database import db_manager
from src.tools import tool_calculate_technical_indicators
from src.vector_db import vector_db_manager
from src.utils.logger import get_logger

logger = get_logger(__name__)

def seed_clickhouse() -> None:
    """Seeds initial historical price and indicator records into ClickHouse."""
    raw_data = []
    base_time = datetime.datetime(2026, 6, 1) - datetime.timedelta(days=20)
    
    for i in range(35):
        current_date = base_time + datetime.timedelta(days=i)
        hpg_price = 28000.0 + (i * 150) + random.uniform(-200, 200)
        btc_price = 95000.0 + (i * 400) + random.uniform(-600, 600)
        
        raw_data.append({
            "timestamp": current_date, "symbol": "HPG", 
            "price": hpg_price, "volume": 500000.0 + (i * 1000) + random.randint(-5000, 5000)
        })
        raw_data.append({
            "timestamp": current_date, "symbol": "BTC", 
            "price": btc_price, "volume": 12000.0 + (i * 50) + random.randint(-100, 100)
        })
        
    df_raw = pd.DataFrame(raw_data)
    df_final = tool_calculate_technical_indicators(df_raw)
    df_final = df_final.groupby("symbol", group_keys=False).tail(15)
    df_final = df_final.sort_values(by=["timestamp", "symbol"]).reset_index(drop=True)
    df_final.columns = df_final.columns.str.lower()
    
    db_manager.ingest_df(df_final, mode="replace")
    logger.info("ClickHouse seed data successfully ingested.")

def seed_qdrant() -> None:
    """Seeds foundational macro-financial qualitative knowledge chunks into Qdrant."""
    knowledge_chunks = [
        {
            "text": "Ngành thép Việt Nam năm 2026 ghi nhận mức tăng trưởng mạnh mẽ do nhu cầu đầu tư công và xuất khẩu phục hồi. Tập đoàn Hòa Phát (HPG) dẫn đầu thị phần nhờ tối ưu chi phí lò cao và chuỗi cung ứng khép kín.", 
            "source": "Báo cáo thép 2026",
            "chunk_hierarchy": "seed-p0-c0"
        },
        {
            "text": "Xu hướng dòng vốn tiền mã hóa (Crypto) chuyển dịch mạnh mẽ sau khi các quỹ Bitcoin Spot ETF liên tục thu hút dòng tiền ròng. Khối lượng giao dịch Bitcoin (BTC) đạt ngưỡng kỷ lục lịch sử do nhà đầu tư tổ chức tham gia.", 
            "source": "Crypto Macro Review",
            "chunk_hierarchy": "seed-p0-c1"
        },
        {
            "text": "Bối cảnh vĩ mô toàn cầu duy trì áp lực lạm phát giảm, thúc đẩy Ngân hàng Trung ương hạ lãi suất điều hành. Việc nới lỏng chính sách tiền tệ hỗ trợ dòng vốn đầu tư mạo hiểm chảy mạnh vào các kênh tài sản như Crypto và Chứng khoán.", 
            "source": "Kinh tế vĩ mô Q2",
            "chunk_hierarchy": "seed-p0-c2"
        },
        {
            "text": "Hòa Phát (HPG) dự kiến đưa dự án Dung Quất 2 vào vận hành thương mại giúp nâng công suất thép HRC lên đáng kể, gia tăng biên lợi nhuận gộp toàn ngành thép giai đoạn 2026 - 2028.", 
            "source": "Phân tích doanh nghiệp HPG",
            "chunk_hierarchy": "seed-p0-c3"
        },
        {
            "text": "Mức độ khan hiếm nguồn cung Bitcoin tăng cao sau chu kỳ Halving lần trước kết hợp với việc các thợ đào hạn chế thanh lý tài sản đẩy giá trị BTC bước vào cấu trúc chu kỳ tăng trưởng dài hạn.", 
            "source": "Blockchain Data Intelligence",
            "chunk_hierarchy": "seed-p0-c4"
        }
    ]
    
    vector_db_manager.ingest_data(knowledge_chunks)
    logger.info("Qdrant Vector DB seed data successfully ingested.")

if __name__ == "__main__":
    seed_clickhouse()
    seed_qdrant()