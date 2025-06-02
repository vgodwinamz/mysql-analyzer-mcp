import logging
import asyncio
from contextlib import asynccontextmanager
from session_handler import SessionHandler

def configure_logging():
    """Configure logging for the application"""
    logger = logging.getLogger("mysql-analyzer")
    logger.setLevel(logging.INFO)
    
    # Create console handler
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    
    # Add handler to logger
    logger.addHandler(handler)
    
    return logger

# Create a session handler instance
session_handler = SessionHandler()

@asynccontextmanager
async def server_lifespan(app):
    """
    Lifespan context manager for the FastMCP server.
    This runs when the server starts and stops.
    """
    # Server startup
    logger = logging.getLogger("mysql-analyzer")
    logger.info("Starting MySQL Performance Analyzer server")
    
    # Start the session handler
    await session_handler.start()
    
    yield
    
    # Server shutdown
    logger.info("Shutting down MySQL Performance Analyzer server")
    
    # Stop the session handler
    await session_handler.stop()