<h1> Real time Bearing Condition Predictor System </h1>

<h2>Introduction</h2>
<p> Based on Predicting Bearings’ Degradation Stages for Predictive Maintenance in the Pharmaceutical Industry (2022) by Dovile Juodelyte, Veronika Cheplygina, Therese Graversen, Philippe Bonnet, we create a conceptual condition monitoring/predictive maintenance app to integrate their deep learing models for real time inference to predict state of degradation.  Also, the backend of this app will also schedule adding into feature pipelines and model training.  Hence this end-to-end system will be built based on the 3-pipeline methodology which includes the use of feature stores and ready made 3rd party model registry.  On the front end single page app with a monitoring dashboard that processes accelerometer data, displays the level of degradation as well as alerting a user of the seriousness of current condition of the monitored bearings.  The dataset used is from the FEMTO dataset.

</p>

<h3>Summary of Paper</h3>
<p>The research paper, Predicting Bearings’ Degradation Stages for Predictive Maintenance in the Pharmaceutical Industry (2022), propsed a two step solution for a robust deep learning model: </p>

- Data labelling is automated using a k-means bearing lifetime segmentation method, which is based on high-frequency bearing vibration signal embedded in a latent low-dimensional subspace 
- The a supervised classifier is built based on a multi-input neural network for bearing degradation stage detection


To illustrate:
![image](https://github.com/user-attachments/assets/3fced8d6-d814-45c2-a216-e1367f06292b)

as well as the model architectures for both steps:
![image](https://github.com/user-attachments/assets/af235c40-97b6-4edd-a7d5-2d0c76676e66)


<p> Each raw accelerometer data/signal from the FEMTO dataset are processed in both frequency and time domains.

For frequency domain, the signals are downsampled by half from 25600Hz to 12800Hz to reduce data dimensionality.  After conversion, frequencies up to 6,400 Hz are obtained, where bearing degradation signs are expected. The downsampled signal in the frequency domain has 641 features being created from each of both vertical and horizontal vibration signals.  For the time domain, both horizontal and vertical signals are split into the following features: mean, absolute median, standard deviation, skewness, kurtosis, crest factor, energy, rms, number of peaks, number of zero crossings, Shapiro test and KL divergence.

</p>

<h3> Summary of this project </h3>

<p>  The original study explored 2 methods of labelling the data - AutoEnconder (as mentioned above) and PCA; and then trained a neural network.  For this project the AutoEncoder is only used for labelling as it is interpreted to have better results.
</p>

<p> 
This project proposes a preliminary version of an integrated ML and IoT system to create a real time condition monitoring application with predictive capabilities.  A large part of the FEMTO dataset will be used for training the neural network as stated while the remaining part will be used as simulated real time accelerometer data.  An ubuntu virtual machine (Ubuntu 22.04) is used to simulate an edge gateway where trained neural networks will reside to predict bearing degradation from new sensor data.

In a typical straightforward IoT system, data exchange between edge gateways and sensors such as accelerometers are via MQTT protocol.  Within an edge gateway, servers such as nodejs servers and flask servers can be hosted and data will be sent to these servers via HTTP POST requests.  Lastly, server-side events (SSE) or websockets can be utilised to forward data packets to front end clients.

The trained neural network is used for real time inferences of incoming data (using simulated data) to determine state of degradation.  While inference is being done, the data is batched for scheduled asynchronous feature and model training pipleines; and streamed to a Streamlit dashboard for visualisation.  Predicted degradation labels are also streamed to both the dashboard and the backend of the alert system.  The latter determines the level of warning to be sent to the dashboard for further analysis or action by a user.

</p>

<h2> Demonstration </h2>

<h3> Overall </h3>

[![IMAGE ALT TEXT HERE](https://img.youtube.com/vi/4M7ylEkJ4IY/0.jpg)](https://www.youtube.com/watch?v=4M7ylEkJ4IY)

<h2> System Design </h2>

<h3> Assumptions </h3>
<p></p>

  -	Assume that this application is housed indoor, so good network connectivity is assumed
  - Assume that this end-to-end ML powered application will run on an edge gateway locally
  -	Assume that immediate inference is required on every processed accelerometer data
  -	Assume near real time visualisation of bearing data
  -	Assume scheduled feature and model training pipeline, with the assumption that will not be any major changes to the current model
  -	Assume that users would require consecutive occurences of degradation predictions for remedial action
  -	Assume 0 % packet loss for mqtt, post requests, and server side events
  -	Assume accelerometers and gateway are exchanging data in the same network
  -	Assume the there is no cybersecurity issues in downloading and uploading to Hopsworks

<h3> High Level Architecture </h3>

![end2endML_bearing_monitoring_highlevel](https://github.com/user-attachments/assets/53182ef6-f049-4c13-8f5c-42ee46957ec4)

<p> According to Hopsworks' approach to ML pipelines (https://www.hopsworks.ai/dictionary/ml-pipeline), it consists of feature, training and inference pipeline alongside a feature store and model registry, which is necessary for AI-based application.  However, in a real time IoT application where instantaneous inference of sensor data is required, the data will go through feature engineering before inference.  Corresponding feature and training pipeline for model (re)training will take place asynchronously.

A model is initially created and trained separately before being deployed in the ML server for generating real time predictions on the application backend alongside asynchronous processes for scheduled feature and training pipeline.  Also, since the dashboard is planned to showcase degradation labels and alerts with real time visualisation of accelerometer data, the system is designed to handle all these processes on the backend asynchronously.  Lastly, since the accelerometer data is simulated, the simulation will also assume concurrent transmission of sensor data belonging to multiple accelerometers to the backend.
</p>

<h3> Detailed Architecture </h3>

![Detail System Diagram](https://github.com/user-attachments/assets/d3e950fb-d768-46d0-b63d-d46f89c966ae)

<p>  We make use of the following: </p>

  - Hopsworks: For use of feature store, model registry and management
  - Celery: For asychronous processing such as scheduling workers for feature and training pipeline
  - Redis: For storing celery queues, utilisation of in memory storage for storing model configurations, last read positions of csv files and consecutive labels to trigger different types of alerts
  - paho-mqtt: for creating mqtt clients
  - aedes: mqtt broker in nodejs

<p> To develop the historical pipelines and the ML server, the various codes from the repository for this paper, (https://github.com/DovileDo/BearingDegradationStageDetection) are reconfigured. </p>

  - Historical_feature_and_training_pipeline.ipynb: Creating feature groups of at least 13k+ rows each, with 1282 features in the frequency domain after abovementioned fourier transformed, 28 features in the time domain and target labels generated from the AutoEncoder; creation of training pipeline via creation of a feature view in Hopsworks, training a multi-neural network and storage in Hopsworks' model registry
  - Index.js: Overall backend to facilitate mqtt broker, listening to mqtt topics and conduct server side events to emit data to a Streamlit interface
  - MQTT Clients: Client before ML server will listen for published simulated data before sending a post request to ML server for instanteous inference; client before alert system: listening for generated degradation predictions and publishing alert type based on consecutive occurences of specific degradation predictions
  - Flask server: _init_ constructor for loading initial configurations; initialisation.py for singleton instantiation of models and feature groups; endpoint.py for receiving post requests for inferences by specific models, feat_eng_pipe.py for updating feature groups and generating labels for new data; training_pipe.py for model (re)training with nnclassifier.py and updating model versions for real time inference
  - Shell/bash script: start_ml_server.sh for starting flask server, celery workers and scheduler, and redis
  - Frontend: bearing_front.py for receiving real time data and visualising accelerometer values for respective bearings with their degradation predictions and alert types.


<h2> Citations </h2>
Juodelyte, D., Cheplygina, V., Graversen, T., & Bonnet, P. (2022). Predicting Bearings’ Degradation Stages for Predictive Maintenance in the Pharmaceutical Industry. arXiv preprint arXiv:2203.03259.





