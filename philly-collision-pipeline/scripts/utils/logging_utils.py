"""
Logging utilities for the Philadelphia Collision Pipeline.
Provides consistent logging across all pipeline stages.
"""

import sys
from pathlib import Path
from loguru import logger
from scripts.config import LOGS_DIR, LOG_LEVEL


def setup_logger(script_name: str):
    """
    Set up logger with both file and console output.
    
    Args:
        script_name: Name of the script (used for log filename)
    """
    # Remove default logger
    logger.remove()
    
    # Add console handler
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>",
        level=LOG_LEVEL,
        colorize=True
    )
    
    # Add file handler
    log_file = LOGS_DIR / f"{script_name}.log"
    logger.add(
        log_file,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function} - {message}",
        level=LOG_LEVEL,
        rotation="10 MB",
        retention="30 days",
        compression="zip"
    )
    
    logger.info(f"Logger initialized for {script_name}")
    logger.info(f"Log file: {log_file}")
    
    return logger


def log_dataframe_info(df, name: str):
    """Log basic information about a DataFrame."""
    logger.info(f"{name} - Shape: {df.shape}")
    logger.info(f"{name} - Columns: {list(df.columns)}")
    logger.info(f"{name} - Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
    # Log missing data
    missing = df.isnull().sum()
    if missing.any():
        logger.warning(f"{name} - Missing values:\n{missing[missing > 0]}")
