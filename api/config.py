"""
Configuration for FastAPI application
"""
import os
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings"""
    
    # Database
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL",
        "postgresql://admin:REDACTED@postgres:5432/clickstream"
    )
    
    # API
    API_TITLE: str = "Clickstream Guardian API"
    API_VERSION: str = "1.0.0"
    API_DESCRIPTION: str = "Real-time anomaly detection and batch analytics API"
    
    # CORS
    CORS_ORIGINS: list = ["*"]
    
    # Pagination
    DEFAULT_PAGE_SIZE: int = 100
    MAX_PAGE_SIZE: int = 1000
    
    class Config:
        env_file = ".env"


settings = Settings()
