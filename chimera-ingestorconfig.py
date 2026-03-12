"""
Configuration management for Chimera Ingestor Service
"""
import os
from typing import Dict, List, Optional
from pydantic import BaseSettings, Field, validator
from dataclasses import dataclass
import logging
from enum import Enum


class ExchangeType(str, Enum):
    BINANCE = "binance"
    COINBASE = "coinbase"
    KRAKEN = "kraken"


@dataclass
class ExchangeConfig:
    """Configuration for individual exchange connections"""
    name: ExchangeType
    websocket_url: str
    symbols: List[str]
    max_reconnect_attempts: int = 5
    reconnect_delay: float = 1.0
    heartbeat_interval: int = 30


class Settings(BaseSettings):
    """Application settings with validation"""
    
    # Service Configuration
    service_name: str = "chimera-ingestor"
    log_level: str = "INFO"
    health_check_port: int = 8000
    
    # Firebase Configuration
    firebase_project_id: str = Field(..., env="FIREBASE_PROJECT_ID")
    firebase_credentials_path: str = Field(..., env="GOOGLE_APPLICATION_CREDENTIALS")
    
    # Exchange Configuration
    exchanges: List[ExchangeType] = [
        ExchangeType.BINANCE,
        ExchangeType.COINBASE
    ]
    
    # Trading Symbols to Monitor
    symbols: List[str] = Field(
        default=["BTC/USDT", "ETH/USDT", "SOL/USDT", "ADA/USDT"],
        description="Symbols to track across exchanges"
    )
    
    # WebSocket Configuration
    websocket_timeout: int = 30
    max_message_queue: int = 1000
    data_validation_enabled: bool = True
    
    # Monitoring
    heartbeat_interval_seconds: int = 30
    stale_data_threshold_seconds: int = 60
    
    # NTP Configuration
    ntp_servers: List[str] = Field(
        default=["pool.ntp.org", "time.google.com", "time.windows.com"],
        description="NTP servers for clock synchronization"
    )
    
    # Firebase Collections
    firestore_raw_ticks_collection: str = "raw_ticks"
    firestore_health_collection: str = "service_health"
    realtime_db_root: str = "/market_data"
    
    class Config:
        env_file = ".env"
        case_sensitive = False
    
    @validator("log_level")
    def validate_log_level(cls, v):
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in valid_levels:
            raise ValueError(f"Log level must be one of {valid_levels}")
        return v.upper()
    
    @validator("symbols")
    def validate_symbols(cls, v):
        if not v:
            raise ValueError("At least one symbol must be specified")
        return v


# Exchange-specific configurations
EXCHANGE_CONFIGS: Dict[ExchangeType, ExchangeConfig] = {
    ExchangeType.BINANCE: ExchangeConfig(
        name=ExchangeType.BINANCE,
        websocket_url="wss://stream.binance.com:9443/ws",
        symbols=["btcusdt", "ethusdt", "solusdt", "adausdt"],
        max_reconnect_attempts=10,
        reconnect_delay=2.0
    ),
    ExchangeType.COINBASE: ExchangeConfig(
        name=ExchangeType.COINBASE,
        websocket_url="wss://ws-feed.exchange.coinbase.com",
        symbols=["BTC-USD", "ETH-USD", "SOL-USD", "ADA-USD"],
        max_reconnect_attempts=8,
        reconnect_delay=1.5
    ),
    ExchangeType.KRAKEN: ExchangeConfig(
        name=ExchangeType.KRAKEN,
        websocket_url="wss://ws.kraken.com",
        symbols=["XBT/USD", "ETH/USD", "SOL/USD", "ADA/USD"],
        max_reconnect_attempts=6,
        reconnect_delay=3.0
    )
}


def get_logger(name: str = None) -> logging.Logger:
    """Get configured logger instance"""
    logger = logging.getLogger(name or __name__)
    
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger


# Initialize settings
try:
    settings = Settings()
except Exception as e:
    print(f"Error loading settings: {e}")
    raise