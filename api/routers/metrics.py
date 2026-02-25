"""
Daily metrics endpoints
"""
from fastapi import APIRouter, HTTPException, Query, Depends
from typing import List
from datetime import date
from pydantic import BaseModel
from models.database import get_db
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/metrics", tags=["metrics"])


class DailyMetric(BaseModel):
    """Daily metric response model"""
    metric_date: date
    total_clicks: int
    total_purchases: int
    unique_sessions: int
    unique_items: int
    conversion_rate: float
    avg_session_duration_sec: float
    avg_clicks_per_session: float
    total_revenue: int
    avg_order_value: float


class FunnelStage(BaseModel):
    """Funnel stage response model"""
    funnel_stage: str
    session_count: int
    percentage: float
    drop_rate: float


@router.get("/daily", response_model=List[DailyMetric])
def get_daily_metrics(
    start_date: date = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: date = Query(..., description="End date (YYYY-MM-DD)"),
    db=Depends(get_db)
):
    """
    Get daily metrics for a date range
    
    Returns aggregated clickstream metrics for each day in the specified range.
    """
    try:
        if start_date > end_date:
            raise HTTPException(status_code=400, detail="start_date must be before end_date")
        
        query = """
            SELECT 
                metric_date,
                total_clicks,
                total_purchases,
                unique_sessions,
                unique_items,
                ROUND(conversion_rate::numeric, 4) as conversion_rate,
                ROUND(avg_session_duration_sec::numeric, 2) as avg_session_duration_sec,
                ROUND(avg_clicks_per_session::numeric, 2) as avg_clicks_per_session,
                total_revenue,
                ROUND(avg_order_value::numeric, 2) as avg_order_value
            FROM daily_metrics
            WHERE metric_date BETWEEN %s AND %s
            ORDER BY metric_date
        """
        
        with db.cursor() as cur:
            cur.execute(query, (start_date, end_date))
            results = cur.fetchall()
        
        return results
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching daily metrics: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch daily metrics")


@router.get("/daily/{metric_date}", response_model=DailyMetric)
def get_daily_metric_by_date(metric_date: date, db=Depends(get_db)):
    """Get daily metrics for a specific date"""
    try:
        query = """
            SELECT 
                metric_date,
                total_clicks,
                total_purchases,
                unique_sessions,
                unique_items,
                ROUND(conversion_rate::numeric, 4) as conversion_rate,
                ROUND(avg_session_duration_sec::numeric, 2) as avg_session_duration_sec,
                ROUND(avg_clicks_per_session::numeric, 2) as avg_clicks_per_session,
                total_revenue,
                ROUND(avg_order_value::numeric, 2) as avg_order_value
            FROM daily_metrics
            WHERE metric_date = %s
        """
        
        with db.cursor() as cur:
            cur.execute(query, (metric_date,))
            result = cur.fetchone()
        
        if not result:
            raise HTTPException(status_code=404, detail=f"No metrics found for {metric_date}")
        
        return result
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching daily metric: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch daily metric")


@router.get("/funnel/{metric_date}", response_model=List[FunnelStage])
def get_funnel_by_date(metric_date: date, db=Depends(get_db)):
    """Get conversion funnel for a specific date"""
    try:
        query = """
            SELECT 
                funnel_stage,
                session_count,
                ROUND(percentage::numeric, 2) as percentage,
                ROUND(drop_rate::numeric, 4) as drop_rate
            FROM session_funnel
            WHERE metric_date = %s
            ORDER BY 
                CASE funnel_stage
                    WHEN 'VIEW' THEN 1
                    WHEN 'MULTI_VIEW' THEN 2
                    WHEN 'ADD_TO_CART' THEN 3
                    WHEN 'PURCHASE' THEN 4
                END
        """
        
        with db.cursor() as cur:
            cur.execute(query, (metric_date,))
            results = cur.fetchall()
        
        if not results:
            raise HTTPException(status_code=404, detail=f"No funnel data found for {metric_date}")
        
        return results
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching funnel: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch funnel")


@router.get("/summary", response_model=dict)
def get_summary(days: int = Query(7, ge=1, le=90), db=Depends(get_db)):
    """Get summary statistics for the last N days"""
    try:
        query = """
            SELECT 
                COUNT(*)::int as days_processed,
                SUM(total_clicks)::bigint as total_clicks,
                SUM(total_purchases)::bigint as total_purchases,
                SUM(unique_sessions)::bigint as total_sessions,
                ROUND(AVG(conversion_rate)::numeric, 4)::float as avg_conversion_rate,
                SUM(total_revenue)::bigint as total_revenue,
                ROUND(AVG(avg_clicks_per_session)::numeric, 2)::float as avg_clicks_per_session
            FROM daily_metrics
            WHERE metric_date >= CURRENT_DATE - (%s * INTERVAL '1 day')
        """
        
        with db.cursor() as cur:
            cur.execute(query, (days,))
            result = cur.fetchone()
        
        return result
    
    except Exception as e:
        logger.error(f"Error fetching summary: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch summary")
