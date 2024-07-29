import sys
import os
import pandas as pd
import numpy as np
from time import sleep
import redis

# Add the parent directory to the system path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bearing_condition_predictor import feature_groups_loader
from bearing_feat_eng_pipeline.AutoEncoder import AutoEncoder

# Helper functions to read and write the last position
def read_last_position(redis_client, redis_key):
    last_position = redis_client.get(redis_key)
    return int(last_position) if last_position else 0

def write_last_position(redis_client, redis_key, position):
    redis_client.set(redis_key, position)

class FeatureEngineeringPipeline:
    def __init__(self, csv_file_path, position_file):
        self.csv_file_path = csv_file_path
        self.redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
        self.REDIS_LAST_READ_KEY = 'db:last_read'
        self.last_position = read_last_position(self.redis_client, self.REDIS_LAST_READ_KEY)
        self.fg_frequency_0, self.fg_frequency_1, self.fg_frequency_2, self.fg_frequency_3, self.fg_time_domain = feature_groups_loader.get_feature_groups()
        self.column_names = ['bearing_number', 'timestamp', 'df_freq', 'df_time']
        self.feature_extractor = AutoEncoder()

    def read_in_chunks(self, chunk_size=50):
        df = pd.read_csv(self.csv_file_path, skiprows=range(1, self.last_position + 1), nrows=chunk_size, names=self.column_names)
        self.last_position += len(df)
        print(self.last_position)
        return df

    def generate_labels(self, df_freq):
        n_rows = int(0.8 * len(df_freq))
        Xt = df_freq.iloc[:n_rows].values
        Xf = df_freq.values
        
        return self.feature_extractor.get_anomaly_labels(Xt, Xf)
    
    def upload_features(self, df_freq, df_time, labels):
        current_index_value = feature_groups_loader.get_current_index_value()
        new_index_values = range(current_index_value, current_index_value + len(df_freq))
        
        frequency_df_0 = df_freq.iloc[:, :321].copy()
        frequency_df_0['index'] = list(new_index_values)
        frequency_df_1 = df_freq.iloc[:, 321:641].copy()
        frequency_df_1['index'] = list(new_index_values)
        frequency_df_2 = df_freq.iloc[:, 641:962].copy()
        frequency_df_2['index'] = list(new_index_values)
        frequency_df_3 = df_freq.iloc[:, 962:1282].copy()
        frequency_df_3['index'] = list(new_index_values)
        
        df_label = pd.DataFrame({'labels': labels})
        df_time_domain = pd.concat([df_time.reset_index(drop=True), df_label], axis=1)
        df_time_domain['Hzerocross'] = df_time_domain['Hzerocross'].astype(float)
        df_time_domain['Vzerocross'] = df_time_domain['Vzerocross'].astype(float)
        df_time_domain['index'] = list(new_index_values)
        
        print(df_time_domain)
        print(frequency_df_3)
        
        self.fg_frequency_0.insert(frequency_df_0, wait=True, overwrite=False)
        sleep(10)
        self.fg_frequency_1.insert(frequency_df_1, wait=True, overwrite=False)
        sleep(10)
        self.fg_frequency_2.insert(frequency_df_2, wait=True, overwrite=False)
        sleep(10)
        self.fg_frequency_3.insert(frequency_df_3, wait=True, overwrite=False)
        sleep(10)
        self.fg_time_domain.insert(df_time_domain, wait=True, overwrite=False)
        sleep(5)
        
        # Increment the current index value
        feature_groups_loader.increment_index_value(len(df_freq))
    
    def run(self):
        df_chunk = self.read_in_chunks()
        if df_chunk.empty or len(df_chunk) < 50:
            print("Not enough data to process")
            return
        
        # Initialize lists to store data from all rows in the chunk
        df_freq_list = []
        df_time_list = []
        
        for index, row in df_chunk.iterrows():
            json_df_freq = row['df_freq']
            json_df_time = row['df_time']
            bearing_number = row['bearing_number']
            time_str = row['timestamp']

            df_freq = pd.read_json(json_df_freq)
            df_time = pd.read_json(json_df_time)
            
             # Append to lists
            df_freq_list.append(df_freq)
            df_time_list.append(df_time)
        
        # Concatenate all rows into single DataFrames
        df_freq_all = pd.concat(df_freq_list, ignore_index=True)
        df_time_all = pd.concat(df_time_list, ignore_index=True)
        
        labels = self.generate_labels(df_freq_all)
        print(labels)
        self.upload_features(df_freq_all, df_time_all, labels)
        
        # Update the last read position
        write_last_position(self.redis_client, self.REDIS_LAST_READ_KEY, self.last_position)
        
        sleep(5)

#if __name__ == '__main__':
#    pipeline = FeatureEngineeringPipeline('bearing_predictions.csv', 'last_position.txt')
#    pipeline.run()