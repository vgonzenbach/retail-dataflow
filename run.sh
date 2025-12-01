#!/bin/bash

export PROJECT_ID=gymshark-479509 # replace with your project id
export REGION=us-east1
export TOPIC_ID=projects/$PROJECT_ID/topics/backend-events-topic
export SUBSCRIPTION_ID=projects/$PROJECT_ID/subscriptions/backend-events-topic-sub
export GCS_PATH=gs://gymshark-retail-events

gcloud config set project $PROJECT_ID
gcloud auth application-default set-quota-project $PROJECT_ID
gcloud auth application-default login

# python3 publish.py --topic=$TOPIC_ID
python3 pipeline.py \
    --runner=DataflowRunner \
    --project=$PROJECT_ID \
    --region=$REGION \
    --job_name=gymshark-events-$(date +%Y%m%d-%H%M%S) \
    --input_subscription=$SUBSCRIPTION_ID \
    --temp_location=$GCS_PATH/tmp/ \
    --staging_location=$GCS_PATH/staging/ \
    --output_location=$GCS_PATH/output/ \
    --error_location=$GCS_PATH/errors/ \
    --streaming \
    --setup_file=./setup.py
