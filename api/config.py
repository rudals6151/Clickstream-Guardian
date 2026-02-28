"""Configuration for FastAPI application"""
import os
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""
    
    # Database (constructed from individual env vars for flexibility)
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL",
        "postgresql://{user}:{password}@{host}:{port}/{db}".format(
            user=os.getenv("POSTGRES_USER", "admin"),
            password=os.getenv("POSTGRES_PASSWORD", ""),
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            db=os.getenv("POSTGRES_DB", "clickstream"),
        )
    )
    
    # API
    API_TITLE: str = "Clickstream Guardian API"
    API_VERSION: str = "1.0.0"
    API_DESCRIPTION: str = "Real-time anomaly detection and batch analytics API"
    
    # CORS
    CORS_ORIGINS: list[str] = [
        origin.strip()
        for origin in os.getenv("CORS_ORIGINS", "*").split(",")
        if origin.strip()
    ]
    
    # Pagination
    DEFAULT_PAGE_SIZE: int = 100
    MAX_PAGE_SIZE: int = 1000
    
    model_config = {"env_file": ".env", "extra": "ignore"}


settings = Settings()
