"""
backend_celery_diagnostic_tasks.py

Celery tasks for system diagnostics.
Scheduled to run daily at 06:00 UTC and on demand.
"""

import logging
import json
from datetime import datetime
from celery import shared_task
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

@shared_task(
    name="tasks.run_system_diagnostics",
    bind=True,
    max_retries=3,
    default_retry_delay=300,  # 5 minutes
    time_limit=1800,  # 30 minutes hard limit
)
def run_system_diagnostics(self, trigger="scheduled"):
    """
    Run full system diagnostics and store results.
    
    Scheduled to run:
      - Daily at 06:00 UTC
      - On demand via API or manual trigger
      
    Args:
      trigger: 'startup', 'scheduled', 'manual', 'api'
      
    Returns:
      Dict with diagnostic results
    """
    try:
        logger.info(f"Starting system diagnostics (trigger: {trigger})")
        
        from diagnostics.system_check import run_full_diagnostics
        
        start_time = datetime.utcnow()
        
        # Run diagnostics
        results = run_full_diagnostics()
        
        duration = (datetime.utcnow() - start_time).total_seconds()
        results['duration_seconds'] = duration
        
        logger.info(f"Diagnostics completed: {results['status']} ({duration:.1f}s)")
        
        # Store results in database if available
        try:
            from backend_database_models import DiagnosticRun
            from sqlalchemy import create_engine
            from sqlalchemy.orm import sessionmaker
            import os
            
            db_url = os.getenv('DATABASE_URL')
            if db_url:
                engine = create_engine(db_url)
                Session = sessionmaker(bind=engine)
                session = Session()
                
                diagnostic_run = DiagnosticRun(
                    run_at=start_time,
                    duration_seconds=duration,
                    total_checks=results.get('total_checks', 0),
                    passed=results.get('passed', 0),
                    failed=results.get('failed', 0),
                    status=results.get('status', 'UNKNOWN'),
                    auto_fixes_applied=results.get('auto_fixes_applied', 0),
                    auto_fixes_failed=results.get('auto_fixes_failed', 0),
                    results_json=json.dumps(results),
                    trigger=trigger,
                )
                
                session.add(diagnostic_run)
                session.commit()
                session.close()
                
                logger.info(f"Diagnostic results stored in database (ID: {diagnostic_run.id})")
        except Exception as db_error:
            logger.warning(f"Could not store results in database: {db_error}")
        
        # Alert if failures
        if results['failed'] > 0:
            logger.warning(
                f"Diagnostics detected issues: {results['failed']} failures"
            )
            
            # Send Sentry alert if available
            try:
                import sentry_sdk
                with sentry_sdk.push_scope() as scope:
                    scope.set_tag("diagnostic", "failure")
                    scope.set_context("diagnostics", {
                        "passed": results['passed'],
                        "failed": results['failed'],
                        "status": results['status'],
                    })
                    sentry_sdk.capture_message(
                        f"System diagnostic check failed: {results['failed']} issues",
                        level="warning"
                    )
            except Exception as sentry_error:
                logger.debug(f"Sentry not available: {sentry_error}")
        
        return results
        
    except Exception as exc:
        logger.error(f"Diagnostics task failed: {exc}", exc_info=True)
        
        # Retry with exponential backoff
        retry_in = 300 * (2 ** self.request.retries)  # 5m, 10m, 20m
        logger.info(f"Retrying in {retry_in}s (attempt {self.request.retries + 1}/3)")
        
        raise self.retry(exc=exc, countdown=retry_in)


@shared_task(
    name="tasks.quick_health_check",
    max_retries=2,
    time_limit=300,  # 5 minute limit for quick check
)
def quick_health_check():
    """
    Quick health check (subset of full diagnostics).
    Runs more frequently to detect emergencies.
    
    Checks:
      - Redis connectivity
      - PostgreSQL connectivity
      - Celery workers
      - Disk space
    """
    try:
        logger.info("Running quick health check")
        
        from diagnostics.system_check import (
            DiagnosticRunner,
            check_redis,
            check_postgresql,
            check_celery,
            check_disk_space,
        )
        
        runner = DiagnosticRunner()
        
        # Run critical infrastructure checks only
        checks = [
            ("Redis", check_redis),
            ("PostgreSQL", check_postgresql),
            ("Celery", check_celery),
            ("Disk Space", check_disk_space),
        ]
        
        for name, check_fn in checks:
            try:
                check_fn(runner)
            except Exception as e:
                logger.warning(f"Check '{name}' failed: {e}")
        
        summary = runner.get_summary()
        status = "HEALTHY" if summary['failed'] == 0 else "DEGRADED"
        
        logger.info(f"Quick health check complete: {status}")
        
        if summary['failed'] > 0:
            logger.warning(
                f"Health check detected {summary['failed']} failures"
            )
        
        return {
            'status': status,
            'checks': summary['results'],
            'timestamp': summary['timestamp'],
        }
        
    except Exception as exc:
        logger.error(f"Quick health check failed: {exc}", exc_info=True)
        raise


@shared_task(
    name="tasks.validate_data_pipeline",
    max_retries=2,
    time_limit=600,  # 10 minute limit
)
def validate_data_pipeline():
    """
    Validate that all data sources are populated and recent.
    
    Checks:
      - ICON-EU, HARMONIE, AROME freshness
      - RADOLAN radar
      - EUMETSAT satellite
      - ERA5 climatology
      - Database integrity
    """
    try:
        logger.info("Validating data pipeline")
        
        from diagnostics.system_check import (
            DiagnosticRunner,
            check_icon_eu,
            check_knmi_harmonie,
            check_meteofrance_arome,
            check_radolan,
            check_eumetsat,
            check_era5,
        )
        
        runner = DiagnosticRunner()
        
        # Data source checks
        checks = [
            ("ICON-EU", check_icon_eu),
            ("KNMI HARMONIE", check_knmi_harmonie),
            ("MÉTÉO-FRANCE AROME", check_meteofrance_arome),
            ("RADOLAN", check_radolan),
            ("EUMETSAT", check_eumetsat),
            ("ERA5", check_era5),
        ]
        
        for name, check_fn in checks:
            try:
                check_fn(runner)
            except Exception as e:
                logger.warning(f"Pipeline check '{name}' failed: {e}")
        
        summary = runner.get_summary()
        
        logger.info(
            f"Data pipeline validation: {summary['passed']} passed, "
            f"{summary['failed']} failed"
        )
        
        return {
            'status': summary['status'],
            'passed': summary['passed'],
            'failed': summary['failed'],
            'checks': summary['results'],
        }
        
    except Exception as exc:
        logger.error(f"Pipeline validation failed: {exc}", exc_info=True)
        raise


# Schedule configuration (add to backend_celery_app.py beat_schedule)
DIAGNOSTICS_BEAT_SCHEDULE = {
    'run-system-diagnostics': {
        'task': 'tasks.run_system_diagnostics',
        'schedule': (60 * 60 * 24),  # Daily
        'args': ('scheduled',),
        'options': {
            'queue': 'diagnostics',
            'priority': 5,  # Normal priority
        }
    },
    'quick-health-check': {
        'task': 'tasks.quick_health_check',
        'schedule': (60 * 5),  # Every 5 minutes
        'options': {
            'queue': 'diagnostics',
            'priority': 8,  # Higher priority
        }
    },
    'validate-data-pipeline': {
        'task': 'tasks.validate_data_pipeline',
        'schedule': (60 * 30),  # Every 30 minutes
        'options': {
            'queue': 'diagnostics',
            'priority': 7,  # Higher than system checks but below health
        }
    },
}
