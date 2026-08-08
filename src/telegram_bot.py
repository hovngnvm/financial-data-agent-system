import os
import asyncio
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from langchain_core.messages import HumanMessage
from src.config import settings
from src.logger import get_logger
from src.agent.graph import app

logger = get_logger(__name__)

TELEGRAM_BOT_TOKEN = settings.telegram_bot_token
CHART_FILE_PATH = settings.chart_file_path

async def command_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the /start command - Welcomes users to the platform"""
    welcome_text = (
        "🏦 *Chào mừng bạn đến với Enterprise FinAgent Platform!*\n\n"
        "Tôi là một Hệ thống Đa tác tử (Multi-Agent) thông minh kết nối hạ tầng dữ liệu thời gian thực "
        "ClickHouse và Qdrant Vector DB.\n\n"
        "📊 *Bạn có thể ra lệnh cho tôi dưới dạng câu thoại tự nhiên hoặc cấu trúc sau:*\n"
        "• `/analyze HPG` - Phân tích chuyên sâu mã cổ phiếu/crypto\n"
        "• Hoặc chat trực tiếp: _'Mã BTC hôm nay có biến động gì không và vẽ đồ thị giúp tôi'_"
    )
    await update.message.reply_text(welcome_text, parse_mode="Markdown")

async def process_agent_workflow(user_message: str, chat_id: int) -> str:
    """
    Asynchronously invokes the LangGraph workflow without blocking the Telegram event loop.
    """
    config = {"configurable": {"thread_id": f"telegram_user_{chat_id}"}}
    inputs = {"messages": [HumanMessage(content=user_message)]}
    
    # Asynchronous graph execution via ainvoke
    final_state = await app.ainvoke(inputs, config=config)
    
    if final_state and "messages" in final_state and final_state["messages"]:
        return final_state["messages"][-1].content
    return "Agent workflow did not respond. Please try again."

async def handle_user_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Processes natural language text queries received from the user"""
    chat_id = update.effective_chat.id
    user_text = update.message.text
    
    await context.bot.send_chat_action(chat_id=chat_id, action="typing")
    
    try:
        if os.path.exists(CHART_FILE_PATH):
            os.remove(CHART_FILE_PATH)
            
        ai_response = await process_agent_workflow(user_text, chat_id)
        await update.message.reply_text(ai_response)
        
        if os.path.exists(CHART_FILE_PATH):
            await context.bot.send_chat_action(chat_id=chat_id, action="upload_photo")
            with open(CHART_FILE_PATH, "rb") as chart_img:
                await update.message.reply_photo(
                    photo=chart_img,
                    caption=f"📈 Real-time analysis chart generated automatically for the query."
                )
                
    except Exception as e:
        logger.error(f"Error processing message: {str(e)}")
        await update.message.reply_text(f"❌ Error occurred during data alignment: {str(e)}")

async def handle_analyze_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Shorthand /analyze command handler for prompt targets"""
    chat_id = update.effective_chat.id
    if not context.args:
        await update.message.reply_text("⚠️ Vui lòng nhập kèm mã tài sản. Ví dụ: `/analyze BTC`", parse_mode="Markdown")
        return
        
    ticker = context.args[0].upper()
    simulated_message = f"Phân tích chuyên sâu mã {ticker} và vẽ biểu đồ biến động giá của nó."
    
    await context.bot.send_chat_action(chat_id=chat_id, action="typing")
    try:
        if os.path.exists(CHART_FILE_PATH):
            os.remove(CHART_FILE_PATH)
            
        ai_response = await process_agent_workflow(simulated_message, chat_id)
        await update.message.reply_text(ai_response)
        
        if os.path.exists(CHART_FILE_PATH):
            with open(CHART_FILE_PATH, "rb") as chart_img:
                await update.message.reply_photo(photo=chart_img, caption=f"📈 Technical analysis chart: {ticker}")
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)}")

def main():
    """Starts the Telegram Bot Polling Engine"""
    if not TELEGRAM_BOT_TOKEN:
        logger.error("❌ SETUP FAILED: Missing configuration TELEGRAM_BOT_TOKEN in settings")
        return

    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", command_start))
    application.add_handler(CommandHandler("analyze", handle_analyze_command))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_user_message))

    logger.info("-> [Telegram Automation]: Bot Agent is running and listening in Polling mode...")
    application.run_polling()