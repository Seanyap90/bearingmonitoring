#!/bin/bash

export HOPSWORKS_API_KEY="uJsXDDux4vsleHxd.A4nwCnW4qyfwgzPtVFnYsRwQBCEwmLrjmeb1elhzrO9SFFxJLtBXLpuv7W64jeWo"
export BROKER_URL="redis://localhost:6379/0"

# Start the Redis server if it's not already running
redis-server &
REDIS_PID=$!

# Function to handle Ctrl+C (SIGINT)
function cleanup {
    echo "Caught Ctrl+C, purging Celery queues..."
    celery -A bearing_condition_predictor.celery purge -f &
    PURGE_PID=$!

    echo "Stopping Celery worker..."
    kill $CELERY_PID &
    KILL_CELERY_PID=$!

    echo "Stopping Redis server..."
    /etc/init.d/redis-server stop &
    STOP_REDIS_PID=$!

    echo "Stopping Flask application..."
    rm celerybeat-schedule &
    kill -STOP $FLASK_PID

    # Wait for all background jobs to complete
    wait $PURGE_PID
    wait $KILL_CELERY_PID
    wait $STOP_REDIS_PID
    
    echo "Stopping Flask application..."
    kill $FLASK_PID

    echo "Cleanup completed."
    exit 0
}

# Set trap to catch SIGINT and call cleanup function
trap cleanup SIGINT

# Start the Celery worker
echo "Starting Celery worker..."
celery -A bearing_condition_predictor.celery beat --loglevel=info &
celery -A bearing_condition_predictor.celery worker -Q feat_pipe_queue --loglevel=info --pool threads &
celery -A bearing_condition_predictor.celery worker -Q training_pipe_queue --loglevel=info --pool threads &
CELERY_PID=$!

# Start the Flask application with Gunicorn
echo "Starting Flask application with Gunicorn..."
#gunicorn -w 1 -b 127.0.0.1:5000 bearing_condition_predictor:app &
#GUNICORN_PID=$!
python3 run.py
FLASK_PID=$!