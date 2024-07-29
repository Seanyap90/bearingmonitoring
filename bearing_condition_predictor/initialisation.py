import os
import hopsworks
import shutil
import tensorflow as tf
from tensorflow import keras
import json
import redis

class ModelLoader:
    _instance = None

    LOCAL_MODEL_BASE_DIR = "bearing_condition_predictor/local_model"

    def __new__(cls, project, redis_host='localhost', redis_port=6379, redis_db=0, redis_key='config:settings'):
        if cls._instance is None:
            cls._instance = super(ModelLoader, cls).__new__(cls)
            cls._instance.project = project
            cls._instance.redis_host = redis_host
            cls._instance.redis_port = redis_port
            cls._instance.redis_db = redis_db
            cls._instance.redis_key = redis_key
            cls._instance.config = cls._load_config_from_redis(redis_host, redis_port, redis_db, redis_key)
            cls._instance.models = cls._load_all_models(cls._instance.project, cls._instance.config)
        return cls._instance
    
    @staticmethod
    def _load_config_from_redis(redis_host, redis_port, redis_db, redis_key):
        r = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
        config_data_str = r.get(redis_key)
        if config_data_str is None:
            raise Exception(f"No configuration found in Redis for key: {redis_key}")
        config = json.loads(config_data_str)
        return config

    @staticmethod
    def _load_all_models(project, config):
        models = {}
        print(config)
        for model_name, model_infos in config["MODELS"].items():
            models[model_name] = {}
            for model_info in model_infos:
                version = model_info["version"]
                model_filename = model_info["filename"]
                model_subdirectory = model_info["model_subdirectory"]
                local_model_path = os.path.join(ModelLoader.LOCAL_MODEL_BASE_DIR, model_subdirectory, str(version), model_filename)

                if os.path.exists(local_model_path):
                    print(f"Found local model: {model_name} version: {version}")
                    model = tf.keras.models.load_model(local_model_path)
                else:
                    mr = project.get_model_registry()
                    retrieved_model = mr.get_model(name=model_name, version=version)
                    if retrieved_model is None:
                        print(f"Model {model_name} version {version} not found in model registry.")
                        continue

                    saved_model_dir = retrieved_model.download()
                    temp_model_path = os.path.join(saved_model_dir, model_filename)

                    if not os.path.exists(temp_model_path):
                        print(f"Model file {temp_model_path} does not exist.")
                        continue

                    if not os.path.exists(os.path.dirname(local_model_path)):
                        os.makedirs(os.path.dirname(local_model_path))

                    shutil.move(temp_model_path, local_model_path)
                    print(f"Loading model from {local_model_path}.")
                    model = tf.keras.models.load_model(local_model_path)

                models[model_subdirectory][version] = model
                print(models)

        return models

    @classmethod
    def get_model(cls, model_subdirectory, version):
        instance = cls._instance
        if not instance:
            raise Exception("ModelLoader instance not initialized.")

        model = instance.models.get(model_subdirectory, {}).get(int(version))
        if model is None:
            # Reload the configuration from Redis
            instance.config = cls._load_config_from_redis(instance.redis_host, instance.redis_port, instance.redis_db, instance.redis_key)
            # Reload all models if the requested version is not found
            instance.models = cls._load_all_models(instance.project, instance.config)
            print(instance.models)
            model = instance.models.get(model_subdirectory, {}).get(int(version))
            if model is None:
                raise Exception(f"Model {model_subdirectory} version {version} not found after reloading.")
        
        return model


class FeatureGroupsLoader:
    _instance = None

    def __new__(cls, project):
        if cls._instance is None:
            cls._instance = super(FeatureGroupsLoader, cls).__new__(cls)
            cls._instance.project = project
            cls._instance._load_feature_groups()
            cls._instance._update_max_index_value()
        return cls._instance

    def _load_feature_groups(self):
        self.fs = self.project.get_feature_store()
        self.fg_frequency_0 = self.fs.get_feature_group(name="frequency_domain_features_0", version=1)
        self.fg_frequency_1 = self.fs.get_feature_group(name="frequency_domain_features_1", version=1)
        self.fg_frequency_2 = self.fs.get_feature_group(name="frequency_domain_features_2", version=1)
        self.fg_frequency_3 = self.fs.get_feature_group(name="frequency_domain_features_3", version=1)
        self.fg_time_domain = self.fs.get_feature_group(name="time_domain_features", version=1)

    def _update_max_index_value(self):
        combined_df = self.fg_time_domain.select_all().read(read_options={"use_hive": True})
        max_value = combined_df['index'].max()
        self.current_index_value = max_value + 1

    def get_feature_groups(self):
        return self.fg_frequency_0, self.fg_frequency_1, self.fg_frequency_2, self.fg_frequency_3, self.fg_time_domain

    def get_current_index_value(self):
        return self.current_index_value

    def increment_index_value(self, increment_by):
        self.current_index_value += increment_by
        return self.current_index_value