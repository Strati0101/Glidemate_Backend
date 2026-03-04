"""
backend_api_diagnostics_routes.py

FastAPI router for system diagnostics endpoints.
Provides admin-only access to system health checks.
"""

import logging
from fastapi import APIRouter, HTTPException, Depends, Header
from datetime import datetime
import os
from typing import Optional

from diagnostics.system_check import run_full_diagnostics

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/diagnostics", tags=["diagnostics"])


def verify_admin_key(x_admin_key: Optional[str] = Header(None)) -> bool:
    """Verify admin key from environment"""
    admin_key = os.getenv("ADMIN_API_KEY", "")
    if not admin_key or x_admin_key != admin_key:
        raise HTTPException(status_code=403, detail="Unauthorized")
    return True


@router.get("/full")
async def get_full_diagnostics(admin: bool = Depends(verify_admin_key)):
    """
    Run full system diagnostic check.
    
    Returns detailed results of all 18 system checks.
    
    Admin only - requires X-Admin-Key header
    
    Example:
      curl -H "X-Admin-Key: your_secret_key" http://localhost:8001/api/diagnostics/full
    """
    try:
        logger.info("Running full system diagnostics")
        results = run_full_diagnostics()
        return {
            "status": "success",
            "diagnostic_results": results
        }
    except Exception as e:
        logger.error(f"Diagnostic error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Diagnostic failed: {str(e)}")


@router.get("/quick")
async def get_quick_diagnostics(admin: bool = Depends(verify_admin_key)):
    """
    Quick health check (subset of full diagnostics).
    Returns just essential status checks.
    """
    try:
        # Import just the infrastructure checks for quick status
        from diagnostics.system_check import (
            DiagnosticRunner,
            check_redis,
            check_postgresql,
            check_celery,
            check_disk_space,
        )
        
        runner = DiagnosticRunner()
        
        # Run only core infrastructure checks
        check_redis(runner)
        check_postgresql(runner)
        check_celery(runner)
        check_disk_space(runner)
        
        summary = runner.get_summary()
        
        return {
            "status": "success",
            "quick_check": {
                "timestamp": datetime.utcnow().isoformat(),
                "overall_status": summary['status'],
                "checks": {
                    "redis": next((r.passed for r in runner.results if "Redis" in r.name), None),
                    "postgresql": next((r.passed for r in runner.results if "PostgreSQL" in r.name), None),
                    "celery": next((r.passed for r in runner.results if "Celery" in r.name), None),
                    "disk": next((r.passed for r in runner.results if "Disk" in r.name), None),
                }
            }
        }
    except Exception as e:
        logger.error(f"Quick diagnostic error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Quick check failed: {str(e)}")


@router.get("/status")
async def get_system_status(admin: bool = Depends(verify_admin_key)):
    """
    Get current system status summary without running new checks.
    Returns the last diagnostic results from the database.
    """
    try:
        # In a full implementation, this would query the diagnostic_runs table
        return {
            "status": "success",
            "message": "Status check endpoint - run /api/diagnostics/full for detailed report"
        }
    except Exception as e:
        logger.error(f"Status check error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Status check failed: {str(e)}")
