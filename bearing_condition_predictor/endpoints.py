from flask import Flask, jsonify, request, Response, Blueprint
import os
import sys
from time import sleep
import numpy as np
import pandas as pd
import tensorflow as tf
from tensorflow import keras
import hopsworks
import json

#script_dir = os.path.dirname(os.path.abspath(__file__))
#parent_dir = os.path.dirname(script_dir)
#sys.path.append(parent_dir)

from bearing_condition_predictor.initialisation import ModelLoader
from bearing_condition_predictor.single_feat_eng import ToFrequency, ToTime
#from bearing_condition_predictor.add_feat_pipe import feat_pipe

endpoints_bp = Blueprint('endpoints', __name__)

def feat_eng_single_row(df):
    df_freq = ToFrequency(df)
    df_time = ToTime(df)
    return df_freq, df_time

def update_csv(bearing_number, monitoring_time, df_freq, df_time, filename='bearing_predictions.csv'):
    # Prepare the data to be appended
    json_df_freq = df_freq.iloc[[0]].to_json(orient='records')
    json_df_time = df_time.iloc[[0]].to_json(orient='records')
    
    # Create a DataFrame with the new row
    new_row = pd.DataFrame({
        'bearing_number': [bearing_number],
        'time': [monitoring_time],
        'df_freq': [json_df_freq],
        'df_time': [json_df_time]
    })
    
    # Check if the file exists
    if os.path.isfile(filename):
        # Append the new row to the existing CSV
        new_row.to_csv(filename, mode='a', header=False, index=False)
    else:
        # Write the new row to a new CSV file with headers
        new_row.to_csv(filename, mode='w', header=True, index=False)
    
@endpoints_bp.route("/api/<model_name>/<int:version>/predict", methods=['POST'])
def predict(model_name, version):
    model = ModelLoader.get_model(model_name, version)
    if not model:
        return jsonify({"error": "Model not found"}), 404
        
    sleep(0.5)    
    data = request.json    
    if not data:
        return jsonify({"error": "No JSON data found"}), 400
    
    filtered_data = []
    directory = None
    if isinstance(data, list):
        # Handle the case where data is a list
        if data and isinstance(data[0], dict) and "Directory" in data[0]:
            directory = data[0]["Directory"]
            hours = data[0]["h"]
            minutes = data[0]["m"]
            seconds = data[0]["s"]
            monitoring_time = f"{hours:02}:{minutes:02}:{seconds:02}"
            #sleep(3)
        for item in data:
            if isinstance(item, dict):
                filtered_item = {key: value for key, value in item.items() if key != "Directory"}
                filtered_data.append(filtered_item)
                #sleep(1)
            else:
                return jsonify({"error": "List items must be dictionaries"}), 400
        data_df = pd.DataFrame(filtered_data)
    df_freq, df_time = feat_eng_single_row(data_df)
    X_frequency = df_freq.to_numpy()
    X_time = np.empty((0, 26), float)
    X_time = np.append(X_time, df_time.values, axis=0)
    
    if model is None:
        return jsonify({"error": "No model available"}), 500
    bearing_performance_label = model.predict([X_frequency[:, :641], X_frequency[:, 641:], X_time])
    predicted_label = np.argmax(bearing_performance_label, axis=1)
    predicted_string_label = str(predicted_label[0])
    response_data = {
        "messsage": "SUCCESS",
        "bearing": directory,
        "time": monitoring_time,
        "label": predicted_string_label 
    }
    print(directory)   
    update_csv(directory, monitoring_time, df_freq, df_time)
    
    return jsonify(response_data),200