"""
Clickstream Guardian API

FastAPI application for serving clickstream analytics and anomaly detection results
"""
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import logging

from config import settings
from routers import anomaly, metrics, sessions
from models.database import get_db_connection


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Initialize FastAPI app
app = FastAPI(
    title=settings.API_TITLE,
    version=settings.API_VERSION,
    description=settings.API_DESCRIPTION,
    docs_url="/docs",
    redoc_url="/redoc"
)


# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)


# Include routers
app.include_router(anomaly.router)
app.include_router(metrics.router)
app.include_router(sessions.router)


@app.get("/", tags=["root"])
def root():
    """Root endpoint"""
    return {
        "message": "Clickstream Guardian API",
        "version": settings.API_VERSION,
        "docs": "/docs",
        "redoc": "/redoc"
    }


@app.get("/health", tags=["health"])
def health_check():
    """
    Health check endpoint
    
    Verifies database connectivity and API status
    """
    try:
        # Test database connection
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                result = cur.fetchone()
        
        return {
            "status": "healthy",
            "database": "connected",
            "version": settings.API_VERSION
        }
    
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(
            status_code=503,
            detail=f"Service unhealthy: {str(e)}"
        )


@app.get("/stats", tags=["stats"])
def get_system_stats():
    """Get system statistics"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Get table counts
                stats = {}
                
                cur.execute("SELECT COUNT(*) as count FROM anomaly_sessions")
                stats['total_anomalies'] = cur.fetchone()['count']
                
                cur.execute("SELECT COUNT(*) as count FROM daily_metrics")
                stats['days_processed'] = cur.fetchone()['count']
                
                cur.execute("SELECT COUNT(*) as count FROM popular_items")
                stats['total_popular_items'] = cur.fetchone()['count']
                
                cur.execute("SELECT MAX(detected_at) as max_date FROM anomaly_sessions")
                last_anomaly = cur.fetchone()['max_date']
                stats['last_anomaly_detected'] = str(last_anomaly) if last_anomaly else None
                
                cur.execute("SELECT MAX(metric_date) as max_date FROM daily_metrics")
                last_metrics = cur.fetchone()['max_date']
                stats['last_metrics_date'] = str(last_metrics) if last_metrics else None
        
        return stats
    
    except Exception as e:
        logger.error(f"Error fetching stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch stats")


@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler"""
    logger.error(f"Unhandled exception: {exc}")
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"}
    )


# Startup event
@app.on_event("startup")
async def startup_event():
    """Startup event handler"""
    logger.info("="*60)
    logger.info(f"Starting {settings.API_TITLE} v{settings.API_VERSION}")
    logger.info(f"Database: {settings.DATABASE_URL.split('@')[1]}")
    logger.info("="*60)
    
    # Test database connection
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT version()")
                version = cur.fetchone()[0]
                logger.info(f"✅ Database connected: {version}")
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")


# Shutdown event
@app.on_event("shutdown")
async def shutdown_event():
    """Shutdown event handler"""
    logger.info("Shutting down Clickstream Guardian API")


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
