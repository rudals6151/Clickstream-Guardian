"""
Anomaly detection endpoints
"""
from fastapi import APIRouter, HTTPException, Query, Depends
from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel
from models.database import get_db
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/anomalies", tags=["anomalies"])


class AnomalySession(BaseModel):
    """Anomaly session response model"""
    id: int
    session_id: int
    detected_at: datetime
    window_start: datetime
    window_end: datetime
    click_count: int
    unique_items: int
    anomaly_score: float
    anomaly_type: str


@router.get("", response_model=List[AnomalySession])
def get_recent_anomalies(
    limit: int = Query(100, ge=1, le=1000, description="Number of anomalies to return"),
    anomaly_type: Optional[str] = Query(None, description="Filter by anomaly type"),
    min_score: Optional[float] = Query(None, description="Minimum anomaly score"),
    db=Depends(get_db)
):
    """
    Get recent anomaly sessions
    
    Returns the most recently detected anomalous sessions with optional filtering.
    """
    try:
        query = """
            SELECT 
                id, session_id, detected_at, window_start, window_end,
                click_count, unique_items, anomaly_score, anomaly_type
            FROM anomaly_sessions
            WHERE 1=1
        """
        params = []
        
        if anomaly_type:
            query += " AND anomaly_type = %s"
            params.append(anomaly_type)
        
        if min_score is not None:
            query += " AND anomaly_score >= %s"
            params.append(min_score)
        
        query += " ORDER BY detected_at DESC LIMIT %s"
        params.append(limit)
        
        with db.cursor() as cur:
            cur.execute(query, params)
            results = cur.fetchall()
        
        return results
    
    except Exception as e:
        logger.error(f"Error fetching anomalies: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch anomalies")


@router.get("/types", response_model=List[dict])
def get_anomaly_types(db=Depends(get_db)):
    """Get list of anomaly types with counts"""
    try:
        query = """
            SELECT 
                anomaly_type,
                COUNT(*) as count,
                AVG(anomaly_score) as avg_score,
                MAX(detected_at) as last_detected
            FROM anomaly_sessions
            GROUP BY anomaly_type
            ORDER BY count DESC
        """
        
        with db.cursor() as cur:
            cur.execute(query)
            results = cur.fetchall()
        
        return results
    
    except Exception as e:
        logger.error(f"Error fetching anomaly types: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch anomaly types")


@router.get("/timeline", response_model=List[dict])
def get_anomaly_timeline(
    hours: int = Query(24, ge=1, le=168, description="Number of hours to look back"),
    db=Depends(get_db)
):
    """Get anomaly detection timeline (hourly aggregates)"""
    try:
        query = """
            SELECT 
                DATE_TRUNC('hour', detected_at) as hour,
                COUNT(*) as anomaly_count,
                COUNT(DISTINCT session_id) as unique_sessions,
                AVG(anomaly_score) as avg_score
            FROM anomaly_sessions
            WHERE detected_at >= NOW() - (%s * INTERVAL '1 hour')
            GROUP BY DATE_TRUNC('hour', detected_at)
            ORDER BY hour DESC
        """
        
        with db.cursor() as cur:
            cur.execute(query, (hours,))
            results = cur.fetchall()
        
        return results
    
    except Exception as e:
        logger.error(f"Error fetching timeline: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch timeline")


@router.get("/{session_id}", response_model=List[AnomalySession])
def get_anomaly_by_session(session_id: int, db=Depends(get_db)):
    """Get anomaly details for a specific session (may have multiple detections across different windows)"""
    try:
        query = """
            SELECT 
                id, session_id, detected_at, window_start, window_end,
                click_count, unique_items, anomaly_score, anomaly_type
            FROM anomaly_sessions
            WHERE session_id = %s
            ORDER BY detected_at DESC
        """
        
        with db.cursor() as cur:
            cur.execute(query, (session_id,))
            results = cur.fetchall()
        
        if not results:
            raise HTTPException(status_code=404, detail=f"Session {session_id} not found")
        
        return results
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching session anomaly: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch session anomaly")
