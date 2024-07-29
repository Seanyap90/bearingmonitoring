from celery import Celery
from celery.schedules import crontab
import pandas as pd
import json
import sys
import os

# Add the parent directory to the system path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bearing_feat_eng_pipeline.feat_eng_pipe import FeatureEngineeringPipeline # Ensure the correct import
from bearing_model_training_pipeline.training_pipe import ModelTrainer
from bearing_condition_predictor import project

# Initialize Celery
redis_broker_url = os.getenv('BROKER_URL', 'redis://localhost:6379/0')
celery = Celery('bearing_condition_predictor', broker=redis_broker_url)

celery.control.purge()

@celery.task(name='celery.run_feat_pipe', queue='feat_pipe_queue')
def run_feat_pipe(batch_size=50):
    pipeline = FeatureEngineeringPipeline('bearing_predictions.csv', 'last_position.txt')
    pipeline.run()
    
@celery.task(name='celery.run_training_pipe', queue='training_pipe_queue')
def run_training_pipe():
    trainer = ModelTrainer(
        project=project,
        test_size=0.1,
        model_description="classifier"
    )
    trainer.run()
    

# Periodic tasks can be scheduled here
celery.conf.beat_schedule = {
    'run-feat-pipe-period': {
        'task': 'celery.run_feat_pipe',
        'schedule': crontab(minute=5), # run every minute
    },
    'run-training-pipe-period': {
        'task': 'celery.run_training_pipe',
        'schedule': crontab(minute=20),  # run every 2 hours
    },
}
celery.conf.timezone = 'UTC'
#celery.autodiscover_tasks(['bearing_feat_eng_pipeline', 'bearing_model_training_pipeline'])
