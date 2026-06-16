import asyncio
from pathlib import Path
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.error import NetworkError
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes
)
from langchain_core.messages import HumanMessage
from src.config import settings
from src.utils.logger import get_logger
from src.agent.graph import app, redis_checkpointer

logger = get_logger(__name__)

TELEGRAM_BOT_TOKEN = settings.telegram_bot_token
CHART_FILE_PATH = settings.chart_file_path

# Per-chat user model provider preferences (defaulting to configured settings)
USER_MODEL_PREFERENCES: dict[int, str] = {}

PROVIDER_LABELS = {
    "local": "🖥️ Local Ollama (Qwen 1.5B)",
    "openai": "🌐 OpenAI (GPT-4o-mini)",
    "gemini": "⚡ Google Gemini (1.5 Flash)",
    "deepseek": "🚀 DeepSeek (V3)"
}

def get_model_keyboard(current_provider: str) -> InlineKeyboardMarkup:
    """Constructs interactive Inline Keyboard buttons for LLM model selection."""
    buttons = [
        [
            InlineKeyboardButton(
                f"{'✅ ' if current_provider == 'local' else ''}🖥️ Local (Qwen 1.5B)",
                callback_data="set_model:local"
            ),
            InlineKeyboardButton(
                f"{'✅ ' if current_provider == 'openai' else ''}🌐 OpenAI (GPT-4o)",
                callback_data="set_model:openai"
            )
        ],
        [
            InlineKeyboardButton(
                f"{'✅ ' if current_provider == 'gemini' else ''}⚡ Google Gemini",
                callback_data="set_model:gemini"
            ),
            InlineKeyboardButton(
                f"{'✅ ' if current_provider == 'deepseek' else ''}🚀 DeepSeek",
                callback_data="set_model:deepseek"
            )
        ]
    ]
    return InlineKeyboardMarkup(buttons)

async def command_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /start command - Welcomes users to the platform."""
    chat_id = update.effective_chat.id
    current_p = USER_MODEL_PREFERENCES.get(chat_id, settings.analyst_llm_provider)
    current_label = PROVIDER_LABELS.get(current_p, current_p)
    
    welcome_text = (
        "🏦 *Welcome to FinAgent Multi-Agent Platform!*\n\n"
        "I am an intelligent Financial Multi-Agent System connected in real-time to "
        "ClickHouse and Qdrant Vector DB.\n\n"
        f"🤖 *Active Analyst Model:* `{current_label}`\n\n"
        "📊 *How to interact:*\n"
        "• `/analyze HPG` - In-depth technical & fundamental analysis\n"
        "• `/model` - Switch between Local Ollama and Cloud AI (OpenAI, Gemini, DeepSeek)\n"
        "• Or ask directly: _'What is the current BTC trend and show me the RSI chart'_"
    )
    await update.message.reply_text(welcome_text, parse_mode="Markdown")

async def command_model(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /model command - Displays interactive UI buttons for LLM switching."""
    chat_id = update.effective_chat.id
    current_p = USER_MODEL_PREFERENCES.get(chat_id, settings.analyst_llm_provider)
    current_label = PROVIDER_LABELS.get(current_p, current_p)
    
    reply_text = (
        "🤖 *Select Analyst AI Model (LLM Switcher)*\n\n"
        f"Current Active Provider: *{current_label}*\n\n"
        "Click a button below to switch your AI model in real-time:"
    )
    keyboard = get_model_keyboard(current_p)
    await update.message.reply_text(reply_text, reply_markup=keyboard, parse_mode="Markdown")

async def handle_model_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Processes interactive button clicks for switching LLM model provider."""
    query = update.callback_query
    await query.answer()
    
    chat_id = update.effective_chat.id
    callback_data = query.data
    
    if callback_data.startswith("set_model:"):
        new_provider = callback_data.split("set_model:")[1]
        USER_MODEL_PREFERENCES[chat_id] = new_provider
        
        provider_name = PROVIDER_LABELS.get(new_provider, new_provider)
        updated_text = (
            "🤖 *Select Analyst AI Model (LLM Switcher)*\n\n"
            f"✅ *Successfully switched model to:* `{provider_name}`\n\n"
            "All subsequent queries in this session will use this model."
        )
        new_keyboard = get_model_keyboard(new_provider)
        await query.edit_message_text(text=updated_text, reply_markup=new_keyboard, parse_mode="Markdown")
        logger.info(f"User {chat_id} switched analyst LLM provider to: {new_provider}")

async def process_agent_workflow(user_message: str, chat_id: int) -> str:
    """
    Asynchronously invokes the LangGraph workflow with the user's selected LLM provider and traces via Langfuse.
    """
    provider = USER_MODEL_PREFERENCES.get(chat_id, settings.analyst_llm_provider)
    from src.agent.callbacks import get_langfuse_handler
    handler = get_langfuse_handler()
    callbacks = [handler] if handler else []
    
    config = {
        "configurable": {"thread_id": f"telegram_user_{chat_id}"},
        "callbacks": callbacks
    }
    inputs = {
        "messages": [HumanMessage(content=user_message)],
        "analyst_provider": provider
    }
    
    final_state = await app.ainvoke(inputs, config=config)
    
    # Ensure immediate delivery of telemetry traces to Langfuse
    if handler and hasattr(handler, "langfuse") and handler.langfuse:
        try:
            handler.langfuse.flush()
        except Exception:
            pass
            
    if final_state and "messages" in final_state and final_state["messages"]:
        return final_state["messages"][-1].content
    return "Agent workflow did not produce a response. Please try again."

async def handle_user_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Processes natural language text queries received from the user."""
    chat_id = update.effective_chat.id
    user_text = update.message.text
    
    await context.bot.send_chat_action(chat_id=chat_id, action="typing")
    
    try:
        Path(CHART_FILE_PATH).unlink(missing_ok=True)
            
        ai_response = await process_agent_workflow(user_text, chat_id)
        await update.message.reply_text(ai_response)
        
        if Path(CHART_FILE_PATH).exists():
            await context.bot.send_chat_action(chat_id=chat_id, action="upload_photo")
            with open(CHART_FILE_PATH, "rb") as chart_img:
                await update.message.reply_photo(
                    photo=chart_img,
                    caption="Technical analysis chart generated for your query."
                )
                
    except Exception as e:
        logger.error(f"Error processing Telegram user message: {e}")
        await update.message.reply_text(f"Error during agent execution: {e}")

async def handle_analyze_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Shorthand /analyze command handler supporting specific ticker and extra queries."""
    chat_id = update.effective_chat.id
    if not context.args:
        await update.message.reply_text("Please provide a ticker symbol. Example: `/analyze BTC`", parse_mode="Markdown")
        return
        
    ticker = context.args[0].upper()
    extra_details = " ".join(context.args[1:]).strip() if len(context.args) > 1 else ""
    
    if extra_details:
        simulated_message = f"Phân tích chuyên sâu mã {ticker}, vẽ biểu đồ kỹ thuật và giải đáp yêu cầu: {extra_details}"
    else:
        simulated_message = f"Phân tích chuyên sâu mã {ticker} và vẽ biểu đồ kỹ thuật tổng quan của nó."
    
    await context.bot.send_chat_action(chat_id=chat_id, action="typing")
    try:
        Path(CHART_FILE_PATH).unlink(missing_ok=True)
            
        ai_response = await process_agent_workflow(simulated_message, chat_id)
        await update.message.reply_text(ai_response)
        
        if Path(CHART_FILE_PATH).exists():
            with open(CHART_FILE_PATH, "rb") as chart_img:
                await update.message.reply_photo(photo=chart_img, caption=f"Technical analysis chart: {ticker}")
    except Exception as e:
        logger.error(f"Error executing /analyze for {ticker}: {e}")
        await update.message.reply_text(f"Error: {e}")

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles transient exceptions in the telegram bot polling loop gracefully."""
    if isinstance(context.error, NetworkError):
        logger.warning(f"Telegram polling transient network drop ({context.error}). Auto-reconnecting...")
    else:
        logger.error(f"Telegram Bot error encountered: {context.error}", exc_info=context.error)

async def post_init(application: Application) -> None:
    """Performs async setup for the LangGraph Redis checkpointer."""
    await redis_checkpointer.asetup()
    logger.info("LangGraph AsyncRedisSaver setup completed.")

def main() -> None:
    """Starts the Telegram Bot Polling Engine."""
    if not TELEGRAM_BOT_TOKEN:
        logger.error("Missing TELEGRAM_BOT_TOKEN in settings. Bot cannot start.")
        return

    application = Application.builder().token(TELEGRAM_BOT_TOKEN).post_init(post_init).build()

    application.add_error_handler(error_handler)
    application.add_handler(CommandHandler("start", command_start))
    application.add_handler(CommandHandler("model", command_model))
    application.add_handler(CommandHandler("analyze", handle_analyze_command))
    application.add_handler(CallbackQueryHandler(handle_model_callback))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_user_message))

    logger.info("FinAgent Telegram Bot is running in polling mode...")
    application.run_polling()