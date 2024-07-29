import sys
import os
import json
import redis

# Add the parent directory to the system path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import hopsworks
from hsml.schema import Schema
from hsml.model_schema import ModelSchema
import numpy as np
import pandas as pd
import tensorflow as tf
from tensorflow import keras
from bearing_model_training_pipeline.NNclassifier import create_model
import joblib
from bearing_condition_predictor.initialisation import FeatureGroupsLoader

class ModelTrainer:
    def __init__(self, project, test_size, model_description, redis_host='localhost', redis_port=6379, redis_db=0, redis_key='config:settings'):
        self.project = project
        self.feature_store = self.project.get_feature_store()
        self.test_size = test_size
        self.model_description = model_description
        self.feature_groups_loader = FeatureGroupsLoader(self.project)
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.redis_key = redis_key
        self.redis_client = redis.Redis(host=self.redis_host, port=self.redis_port, db=self.redis_db)
        self.load_config()

        # Initialize model details dynamically from config
        self.model_name = next(iter(self.models.keys())) if self.models else None
        self.load_model_details()
        self.feature_view_name = self.config['FEATURE_VIEW']['feature_view_name']
        self.feature_view_version = self.config['FEATURE_VIEW']['version']

    def run(self):
        print(f"Model Subdirectory: {self.model_subdirectory}")
        print(f"Model Filename: {self.model_filename}")
        print(f"Model Version: {self.model_version}")
        print(f"Feature View Name: {self.feature_view_name}")
        print(f"Feature View Version: {self.feature_view_version}")

        self.load_feature_view()
        data, labels, X_train, y_train = self.prepare_data()
        model, history = self.train_model(data, labels)
        self.save_model(model, history, X_train, labels, data[0], data[1], data[2])

    def load_config(self):
        config_data_str = self.redis_client.get(self.redis_key)
        if config_data_str:
            self.config = json.loads(config_data_str)
            self.models = self.config.get('MODELS', {})
        else:
            self.config = {
                "MODELS": {},
                "FEATURE_VIEW": {
                    "feature_view_name": "bearing_monitoring",
                    "version": 1
                }
            }
            self.models = self.config['MODELS']

    def load_model_details(self):
        if self.model_name:
            models = self.models[self.model_name]
            if models:
                if isinstance(models, list):
                    latest_model = max(models, key=lambda x: x['version'])
                else:
                    latest_model = models
                
                self.model_subdirectory = latest_model['model_subdirectory']
                self.model_filename = latest_model['filename']
                self.model_version = latest_model['version']
                self.model_versions = models  # Store all model versions for appending
        else:
            raise ValueError("No models found in config.")  # You may want to handle this case

    def load_feature_view(self):
        fg_frequency_0, fg_frequency_1, fg_frequency_2, fg_frequency_3, fg_time_domain = self.feature_groups_loader.get_feature_groups()
        query = (fg_frequency_0.select_all()
                 .join(fg_frequency_1.select_all(), on="index")
                 .join(fg_frequency_2.select_all(), on="index")
                 .join(fg_frequency_3.select_all(), on="index")
                 .join(fg_time_domain.select_all(), on="index"))
        self.feature_view = self.feature_store.get_or_create_feature_view(
            name=self.feature_view_name,
            version=self.feature_view_version,
            query=query,
            labels=["labels"]
        )

    def prepare_data(self):
        X_train, X_test, y_train, y_test = self.feature_view.train_test_split(
            test_size=self.test_size,
            description='Bearing monitor',
            primary_keys=False,
            read_options={"arrow_flight_config": {"timeout": 3600}}
        )
        X_train.drop(columns=["index"], inplace=True)
        horizontal_data = X_train.iloc[:, :641].values
        vertical_data = X_train.iloc[:, 641:1282].values
        meta_data = X_train.iloc[:, -26:].values
        y_train_array = keras.utils.to_categorical(y_train.values, num_classes=4)
        return (horizontal_data, vertical_data, meta_data), y_train_array, X_train, y_train

    def train_model(self, data, labels):
        AEclassifier = create_model()
        history = AEclassifier.fit(
            data, labels, batch_size=512, epochs=15, shuffle=True
        )
        return AEclassifier, history

    def save_model(self, model, history, X_train, y_train_array, horizontal_data, vertical_data, meta_data):
        mr = self.project.get_model_registry()

        dtype = [('horizontal', np.float32, (horizontal_data.shape[1],)),
                 ('vertical', np.float32, (vertical_data.shape[1],)),
                 ('meta', np.float32, (meta_data.shape[1],))]
        combined_array = np.zeros(horizontal_data.shape[0], dtype=dtype)
        combined_array['horizontal'] = horizontal_data
        combined_array['vertical'] = vertical_data
        combined_array['meta'] = meta_data

        input_schema = Schema(combined_array)
        output_schema = Schema(y_train_array)
        model_schema = ModelSchema(
            input_schema=input_schema, 
            output_schema=output_schema,
        )

        model_dir = os.path.join(self.model_subdirectory)
        if not os.path.isdir(model_dir):
            os.makedirs(model_dir)

        model.save(os.path.join(model_dir, self.model_filename))

        # Collect metrics from the training history
        metrics = {key: value[-1] for key, value in history.history.items()}

        # Increment model version before creating the new model entry
        self.model_version += 1

        model_entry = mr.tensorflow.create_model(
            name=self.model_subdirectory,
            description=self.model_description,
            version=self.model_version,
            model_schema=model_schema,
            metrics=metrics
        )

        model_entry.save(model_dir)

        # Append new model version to the existing model_versions array
        new_model_version = {
            'model_subdirectory': self.model_subdirectory,
            'filename': self.model_filename,
            'version': self.model_version
        }
        if isinstance(self.model_versions, list):
            self.model_versions.append(new_model_version)
        elif isinstance(self.model_versions, dict):
            self.model_versions = [self.model_versions, new_model_version]

        # Update the config file with the new model version appended
        self.save_config()

    def save_config(self):
        # Update the MODELS key in the config dictionary
        self.config['MODELS'][self.model_name] = self.model_versions

        # Write back the updated config to Redis
        self.redis_client.set(self.redis_key, json.dumps(self.config))
      
# Example usage
#project = hopsworks.login()  # Assuming `project` is obtained from another script
#trainer = ModelTrainer(
#    project=project,
#    test_size=0.1,
#    model_description='classifier',
#)
#trainer.run()