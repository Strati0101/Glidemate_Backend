#!/usr/bin/env python3
"""Celery worker entrypoint wrapper to avoid celeryconfig import issues"""

import os
import sys
from celery import Celery

# Set up environment
os.environ.setdefault('CELERY_BROKER_URL', 'redis://redis:6379/0')
os.environ.setdefault('CELERY_RESULT_BACKEND', 'redis://redis:6379/1')

# Import our celery app (this avoids celeryconfig lookup)
from celery_app import app

# Determine worker type from environ or default to worker
worker_type = os.getenv('WORKER_TYPE', 'worker').lower()

if worker_type == 'beat':
    # Run Celery Beat scheduler
    from celery.bin import beat
    beat_app = beat.beat(app=app, loglevel='info')
    beat_app.run()
elif worker_type == 'flower':
    # Run Flower monitoring
    from celery.bin import flower
    flower_app = flower.flower(
        app=app,
        port=int(os.getenv('FLOWER_PORT', '5555')),
        basic_auth=f"{os.getenv('FLOWER_USER', 'admin')}:{os.getenv('FLOWER_PASSWORD', 'admin')}"
    )
    flower_app.run()
else:
    # Default to worker mode
    from celery.bin import worker
    concurrency = int(os.getenv('CONCURRENCY', '4'))
    worker_app = worker.worker(
        app=app,
        loglevel='info',
        concurrency=concurrency,
        queues=['default', 'ingestion', 'tiles', 'ml'],
        max_tasks_per_child=100
    )
    worker_app.run()
