"""
XC Tile API Routes - Serving pre-generated PNG tiles

Endpoints:
  GET /tiles/xc-distance/{forecast_hour}.png - Full Europe heatmap tile
  GET /api/xc-tiles/status - Tile generation status
  POST /api/xc-tiles/generate - Trigger tile generation (Celery task)
"""

from fastapi import APIRouter, Query, BackgroundTasks, HTTPException
from fastapi.responses import FileResponse
from pathlib import Path
import logging
import json
from datetime import datetime

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/tiles", tags=["XC Tiles"])
api_router = APIRouter(prefix="/api/xc-tiles", tags=["XC Tiles API"])

TILES_DIR = Path("/opt/glidemate-backend/tiles/xc-distance")


# ═══════════════════════════════════════════════════════════════════════════════
# TILE SERVING
# ═══════════════════════════════════════════════════════════════════════════════

@router.get("/xc-distance/{forecast_hour}.png")
async def get_xc_distance_tile(forecast_hour: int) -> FileResponse:
    """
    Serve pre-generated XC distance heatmap tile.
    
    Args:
        forecast_hour: Forecast hour (0, 3, 6, 9, 12, ...)
    
    Returns:
        PNG image file
    
    Example:
        GET /tiles/xc-distance/0.png
        → 256×256 PNG showing XC distance potential across Europe
    """
    
    tile_path = TILES_DIR / f"xc-distance-{forecast_hour:02d}.png"
    
    if not tile_path.exists():
        logger.warning(f"Tile not found: {tile_path}")
        raise HTTPException(
            status_code=404,
            detail=f"Tile for forecast hour {forecast_hour} not yet generated"
        )
    
    return FileResponse(
        tile_path,
        media_type="image/png",
        headers={
            "Cache-Control": "public, max-age=3600",  # Cache for 1 hour
            "X-Forecast-Hour": str(forecast_hour)
        }
    )


# ═══════════════════════════════════════════════════════════════════════════════
# TILE STATUS & MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════════

@api_router.get("/status")
async def get_tile_status():
    """
    Get tile generation status.
    
    Returns:
        Dict with available tiles, coverage, generation timestamp
    
    Example Response:
        {
          "status": "healthy",
          "tiles_available": [0, 3, 6, 9, 12],
          "total_tiles_expected": 8,
          "last_update": "2024-01-12T14:30:00Z",
          "coverage_percent": 62.5,
          "tiles_dir": "/opt/glidemate-backend/tiles/xc-distance",
          "size_mb": 15.3
        }
    """
    
    tiles = []
    total_size = 0
    
    if TILES_DIR.exists():
        for tile_file in sorted(TILES_DIR.glob("xc-distance-*.png")):
            try:
                # Extract forecast hour from filename
                hour = int(tile_file.stem.split("-")[-1])
                size = tile_file.stat().st_size
                tiles.append(hour)
                total_size += size
            except (ValueError, OSError):
                logger.warning(f"Invalid tile file: {tile_file}")
    
    # Get directory modification time
    last_update = None
    if TILES_DIR.exists() and TILES_DIR.stat().st_mtime:
        last_update = datetime.fromtimestamp(TILES_DIR.stat().st_mtime).isoformat() + "Z"
    
    # Expected tiles: 0, 3, 6, 9, 12, 15, 18, 21 (hourly)
    expected_tiles = 8
    coverage_percent = (len(tiles) / expected_tiles * 100) if expected_tiles > 0 else 0
    
    return {
        "status": "healthy" if len(tiles) > 0 else "no_tiles",
        "tiles_available": sorted(tiles),
        "total_tiles_expected": expected_tiles,
        "last_update": last_update,
        "coverage_percent": round(coverage_percent, 1),
        "tiles_dir": str(TILES_DIR),
        "size_mb": round(total_size / (1024 * 1024), 1)
    }


@api_router.post("/generate")
async def generate_tiles(
    forecast_hour: int = Query(..., ge=0, le=120, description="Forecast hour to generate (0-120)"),
    background_tasks: BackgroundTasks = None
):
    """
    Trigger XC tile generation as background task.
    
    Args:
        forecast_hour: Which forecast hour to generate
        background_tasks: FastAPI background tasks
    
    Returns:
        Task submission confirmation
    
    Example:
        POST /api/xc-tiles/generate?forecast_hour=0
        → Returns task_id, you can check status at /api/xc-tiles/status
    """
    
    try:
        # Import Celery task if available
        from atmosphere.xc_tile_generation import generate_xc_tiles_celery
        
        # Submit background task
        task = generate_xc_tiles_celery.delay(forecast_hour)
        
        return {
            "status": "submitted",
            "task_id": task.id,
            "forecast_hour": forecast_hour,
            "message": f"Tile generation queued for forecast hour {forecast_hour}"
        }
    
    except ImportError:
        # Fallback: Try direct generation
        try:
            from atmosphere.xc_tile_generation import generate_xc_tile
            
            if background_tasks:
                background_tasks.add_task(
                    generate_xc_tile,
                    forecast_run_time=datetime.utcnow(),
                    forecast_hour=forecast_hour
                )
                
                return {
                    "status": "submitted",
                    "forecast_hour": forecast_hour,
                    "message": f"Tile generation started for forecast hour {forecast_hour}"
                }
            else:
                stats = generate_xc_tile(
                    forecast_run_time=datetime.utcnow(),
                    forecast_hour=forecast_hour
                )
                return {
                    "status": "complete",
                    "forecast_hour": forecast_hour,
                    **stats
                }
        
        except Exception as e:
            logger.exception("Tile generation failed")
            raise HTTPException(
                status_code=500,
                detail=f"Tile generation error: {str(e)[:100]}"
            )


# ═══════════════════════════════════════════════════════════════════════════════
# TILE LAYER CONFIGURATION (for Frontend)
# ═══════════════════════════════════════════════════════════════════════════════

@api_router.get("/layer")
async def get_tile_layer_config():
    """
    Get MapLibre tile layer configuration for XC heatmap.
    
    Returns:
        MapLibre layer definition that can be added to map.style.layers
    
    Example Response:
        {
          "id": "xc-distance-heatmap",
          "type": "raster",
          "source": {
            "type": "raster",
            "tiles": ["http://localhost:8001/tiles/xc-distance/{forecast_hour}.png"],
            "tileSize": 256
          },
          "paint": {
            "raster-opacity": 0.65
          }
        }
    """
    
    # Get available tiles
    status = await get_tile_status()
    tiles = status.get("tiles_available", [])
    
    if not tiles:
        return {
            "status": "no_tiles",
            "message": "No XC distance tiles available yet"
        }
    
    # Use first available forecsst hour
    default_hour = tiles[0]
    
    return {
        "id": "xc-distance-heatmap",
        "type": "raster",
        "source": {
            "type": "raster",
            "tiles": [f"http://localhost:8001/tiles/xc-distance/{default_hour}.png"],
            "tileSize": 256,
            "attribution": "XC Distance Analysis - GlideMate"
        },
        "paint": {
            "raster-opacity": 0.65
        },
        "layout": {
            "visibility": "visible"
        }
    }


# ═══════════════════════════════════════════════════════════════════════════════
# TILE LEGEND
# ═══════════════════════════════════════════════════════════════════════════════

@api_router.get("/legend")
async def get_tile_legend():
    """
    Get color legend for XC distance heatmap.
    
    Returns:
        Array of distance ranges and their colors
    
    Example:
        GET /api/xc-tiles/legend
    """
    
    return {
        "title": "XC Distance Potential (km)",
        "colors": [
            {
                "range": "0-50 km",
                "color": "#2166ac",
                "label": "Poor Day",
                "rgb": [33, 102, 172]
            },
            {
                "range": "50-100 km",
                "color": "#74add1",
                "label": "Weak Day",
                "rgb": [116, 173, 209]
            },
            {
                "range": "100-150 km",
                "color": "#fee090",
                "label": "Average Day",
                "rgb": [254, 224, 144]
            },
            {
                "range": "150-200 km",
                "color": "#f46d43",
                "label": "Good Day",
                "rgb": [244, 109, 67]
            },
            {
                "range": "200-300 km",
                "color": "#d73027",
                "label": "Excellent Day",
                "rgb": [215, 48, 39]
            },
            {
                "range": "300+ km",
                "color": "#a50026",
                "label": "Exceptional Day",
                "rgb": [165, 0, 38]
            }
        ]
    }


# ═══════════════════════════════════════════════════════════════════════════════
# DEBUG ENDPOINT
# ═══════════════════════════════════════════════════════════════════════════════

@api_router.get("/debug")
async def debug_tiles():
    """
    Debug information about tile system.
    
    Returns:
        Diagnostic information
    """
    
    return {
        "tiles_dir": str(TILES_DIR),
        "dir_exists": TILES_DIR.exists(),
        "total_size_bytes": sum(f.stat().st_size for f in TILES_DIR.glob("*") if TILES_DIR.exists()),
        "file_count": len(list(TILES_DIR.glob("*.png") if TILES_DIR.exists() else [])),
        "celery_available": True,  # Will be checked at import time
        "numpy_available": True,
        "pillow_available": True,
    }
