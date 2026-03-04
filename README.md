# GlideMate Backend

Production-ready backend for the GlideMate soaring forecasting and flight planning application.

## Project Structure

```
glidemate-backend/
├── app/                          # Main application entry points
│   ├── main.py                   # Primary application server
│   ├── main_startup.py           # Startup with initialization
│   └── arome_startup.py          # AROME weather model startup
│
├── config/                       # Configuration management
│   ├── config.py                 # Core configuration
│   └── config_extensions.py      # Phase 3 configuration extensions
│
├── integrations/                 # External service integrations
│   ├── dwd.py                    # German Weather Service (DWD)
│   ├── knmi.py                   # Royal Netherlands Meteorological Institute
│   ├── meteofrance.py            # Météo-France integration
│   └── geosphere_austria.py      # Austrian Geosphere integration
│
├── processors/                   # Weather data processing
│   ├── grib_processor.py         # GRIB data parsing and conversion
│   ├── dem_processor.py          # Digital Elevation Model processing
│   ├── dem_analysis.py           # DEM analysis and calculations
│   ├── atmosphere.py             # Atmospheric calculations
│   └── atmosphere_parcel.py      # Parcel theory calculations
│
├── data/                         # Data management and pipelines
│   ├── data_providers.py         # Weather data provider interfaces
│   └── pipeline_connector.py     # Data pipeline orchestration
│
├── models/                       # Database and data models
│   └── database.py               # SQLAlchemy models and schemas
│
├── cache/                        # Caching layer
│   └── cache_manager.py          # Distributed cache management (Redis)
│
├── forecast/                     # Forecast services
│   └── forecast_service.py       # Core forecasting engine
│
├── routes/                       # REST API endpoints
│   ├── forecast.py               # Forecast API routes
│   ├── weather.py                # Weather data routes
│   ├── parcel.py                 # Parcel theory routes
│   ├── soaring.py                # Soaring condition routes
│   ├── xc.py                     # Cross-country flight routes
│   ├── xc_tiles.py               # XC tile generation routes
│   ├── elevation.py              # Elevation data routes
│   ├── nowcast.py                # Nowcast/real-time routes
│   ├── safety.py                 # Safety analysis routes
│   ├── diagnostics.py            # Diagnostic and debugging routes
│   └── forecast_enhancement.py   # Forecast enhancement routes
│
├── celery/                       # Distributed task processing
│   ├── app.py                    # Celery application setup
│   ├── app_fix.py                # Celery app fixes/patches
│   ├── config.py                 # Celery configuration
│   ├── worker.py                 # Worker entrypoint
│   ├── tasks/                    # Celery task definitions
│   │   ├── tasks.py              # General async tasks
│   │   ├── data_ingestion.py     # Weather data ingestion tasks
│   │   ├── data_ingestion_phase3.py  # Phase 3 enhancements
│   │   ├── data_handlers.py      # Data processing handlers
│   │   ├── diagnostic.py         # Diagnostic tasks
│   │   ├── nowcast.py            # Real-time nowcasting tasks
│   │   ├── safety.py             # Safety analysis tasks
│   │   └── phase3_extensions.py  # Phase 3 task extensions
│   └── schedules/                # Scheduled periodic tasks
│       └── nowcast.py            # Nowcast scheduling
│
├── generators/                   # Data generation and synthesis
│   └── tile_generator.py         # Map tile generation
│
├── algorithms/                   # Meteorological algorithms
│   └── weather.py                # Weather calculation algorithms
│
├── api/                          # API extensions and utilities
│   └── phase3_extensions.py      # Phase 3 API enhancements
│
├── requirements.txt              # Python dependencies
├── requirements_phase3.txt       # Phase 3 additional dependencies
└── README.md                     # This file
```

## Installation

### Prerequisites
- Python 3.9+
- PostgreSQL 12+
- Redis 6.0+
- GRIB data access (DWD, Météo-France, KNMI)

### Setup

```bash
# Clone repository
git clone https://github.com/Strati0101/Glidemate_Backend.git
cd Glidemate_Backend

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
pip install -r requirements_phase3.txt  # For Phase 3 features

# Configure environment
cp .env.example .env
# Edit .env with your configuration
```

## Key Components

### Weather Data Integration
- **DWD** (German Weather Service) - ICON model data
- **Météo-France** - ARPEGE/AROME model data
- **KNMI** (Royal Netherlands Met Institute) - HARMONIE-AROME
- **Geosphere Austria** - Austrian GFS/ALARO data

### Data Processing Pipeline
1. **Ingest** - Download GRIB data from weather providers
2. **Process** - Convert GRIB to usable format, extract parameters
3. **Analyze** - Calculate derived parameters (lift, wind shear, etc.)
4. **Cache** - Store in Redis for fast retrieval
5. **Serve** - REST APIs for mobile/web clients

### Core Features
- **Parcel Theory** - Atmospheric stability analysis
- **Soaring Conditions** - Thermal and ridge lift identification
- **Cross-Country Planning** - Flight route optimization
- **Real-time Nowcasting** - Current weather updates
- **Safety Analysis** - Thunderstorm and hazard detection

## Configuration

Key environment variables:
```
FLASK_ENV=production
DATABASE_URL=postgresql://user:pass@localhost/glidematev2
REDIS_URL=redis://localhost:6379/0
DWD_API_KEY=xxx
METEOFRANCE_API_KEY=xxx
KNMI_API_KEY=xxx
```

## Development

### Running the Application
```bash
python app/main.py
```

### Running Celery Worker
```bash
celery -A celery.app worker --loglevel=info
```

### Running Scheduled Tasks
```bash
celery -A celery.app beat --loglevel=info
```

## API Documentation

Once running, access API docs at:
- **Swagger UI**: `http://localhost:5000/api/docs`
- **ReDoc**: `http://localhost:5000/api/redoc`

## Testing

```bash
pytest tests/
pytest tests/ --cov=glidemate_backend
```

## Deployment

See `DEPLOYMENT.md` for production deployment guide.

## Architecture

- **Framework**: Flask/FastAPI
- **Task Queue**: Celery + RabbitMQ/Redis
- **Database**: PostgreSQL with SQLAlchemy ORM
- **Cache**: Redis
- **APIs**: REST with OpenAPI/Swagger documentation
- **Real-time**: WebSocket support for live data

## Contributing

1. Create feature branch: `git checkout -b feature/feature-name`
2. Commit changes: `git commit -am 'Add feature'`
3. Push to branch: `git push origin feature/feature-name`
4. Submit Pull Request

## License

Proprietary - GlideMate Project

## Support

For issues and questions:
- GitHub Issues: https://github.com/Strati0101/Glidemate_Backend/issues
- Email: l.stratmann@strtmn.de

---

**Last Updated**: March 4, 2026
**Version**: 2.0.0 (Phase 3)