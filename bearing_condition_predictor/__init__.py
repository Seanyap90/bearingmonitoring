import os
import sys
import json
import redis
from flask import Flask
from flask_cors import CORS
import hopsworks
from bearing_condition_predictor.config import Config
from bearing_condition_predictor.initialisation import FeatureGroupsLoader, ModelLoader

app = Flask(__name__)
CORS(app)

# Function to load config.json onto Redis
def load_config_to_redis(config_path, redis_host='localhost', redis_port=6379, redis_db=0, redis_key='config:settings'):
    with open(config_path, 'r') as file:
        config_data = json.load(file)

    r = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    config_data_str = json.dumps(config_data)
    r.set(redis_key, config_data_str)

# Load config.json onto Redis
load_config_to_redis("config.json")

project = hopsworks.login(api_key_value=Config.HOPSWORKS_API_KEY)
feature_groups_loader = FeatureGroupsLoader(project)
ModelLoader(project)

from bearing_condition_predictor.endpoints import endpoints_bp
app.register_blueprint(endpoints_bp)

from bearing_condition_predictor.celery import celery