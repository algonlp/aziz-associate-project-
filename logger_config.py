import sys
import os

from loguru import logger

# Remove the default handler
logger.remove()

# Add a new handler with INFO level
logger.add(
    sys.stderr,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan> - <level>{message}</level>",
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    colorize=True
)

def get_logger(name):
    return logger.bind(name=name)
