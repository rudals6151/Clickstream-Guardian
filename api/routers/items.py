"""
Session and popular items endpoints
"""
from fastapi import APIRouter, HTTPException, Query, Depends
from typing import List
from datetime import date
from pydantic import BaseModel
from models.database import get_db
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/items", tags=["items"])


class PopularItem(BaseModel):
    """Popular item response model"""
    item_id: int
    category: str
    click_count: int
    purchase_count: int
    revenue: int
    rank: int
    click_to_purchase_ratio: float


class PopularCategory(BaseModel):
    """Popular category response model"""
    category: str
    click_count: int
    purchase_count: int
    unique_items: int
    revenue: int
    rank: int


@router.get("/popular/{metric_date}", response_model=List[PopularItem])
def get_popular_items(
    metric_date: date,
    limit: int = Query(50, ge=1, le=500),
    db=Depends(get_db)
):
    """Get popular items for a specific date"""
    try:
        query = """
            SELECT 
                item_id,
                category,
                click_count,
                purchase_count,
                revenue,
                rank,
                ROUND(click_to_purchase_ratio::numeric, 2) as click_to_purchase_ratio
            FROM popular_items
            WHERE metric_date = %s
            ORDER BY rank
            LIMIT %s
        """
        
        with db.cursor() as cur:
            cur.execute(query, (metric_date, limit))
            results = cur.fetchall()
        
        if not results:
            raise HTTPException(status_code=404, detail=f"No popular items found for {metric_date}")
        
        return results
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching popular items: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch popular items")


@router.get("/categories/{metric_date}", response_model=List[PopularCategory])
def get_popular_categories(
    metric_date: date,
    limit: int = Query(20, ge=1, le=100),
    db=Depends(get_db)
):
    """Get popular categories for a specific date"""
    try:
        query = """
            SELECT 
                category,
                click_count,
                purchase_count,
                unique_items,
                revenue,
                rank
            FROM popular_categories
            WHERE metric_date = %s
            ORDER BY rank
            LIMIT %s
        """
        
        with db.cursor() as cur:
            cur.execute(query, (metric_date, limit))
            results = cur.fetchall()
        
        if not results:
            raise HTTPException(status_code=404, detail=f"No popular categories found for {metric_date}")
        
        return results
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching popular categories: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch popular categories")


@router.get("/trending", response_model=List[dict])
def get_trending_items(
    days: int = Query(7, ge=1, le=30),
    limit: int = Query(20, ge=1, le=100),
    db=Depends(get_db)
):
    """Get trending items over the last N days"""
    try:
        query = """
            SELECT 
                item_id,
                category,
                SUM(click_count) as total_clicks,
                SUM(purchase_count) as total_purchases,
                SUM(revenue) as total_revenue,
                ROUND(AVG(rank)::numeric, 1) as avg_rank
            FROM popular_items
            WHERE metric_date >= CURRENT_DATE - (%s * INTERVAL '1 day')
            GROUP BY item_id, category
            ORDER BY total_revenue DESC
            LIMIT %s
        """
        
        with db.cursor() as cur:
            cur.execute(query, (days, limit))
            results = cur.fetchall()
        
        return results
    
    except Exception as e:
        logger.error(f"Error fetching trending items: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch trending items")
