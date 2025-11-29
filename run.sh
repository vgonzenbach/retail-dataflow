#!/bin/bash

export PROJECT_ID=gymshark-479509
export REGION=us-east1
export TOPIC_ID=projects/$PROJECT_ID/topics/backend-events-topic
export SUBSCRIPTION_ID=projects/$PROJECT_ID/subscriptions/backend-events-topic-sub
export GCS_PATH=gs://gymshark-retail-events/output

export EVENT_PATH=data/order_event.json

#gcloud config set project $PROJECT_ID
#gcloud auth application-default set-quota-project $PROJECT_ID
#gcloud auth application-default login

python3 publish.py --topic=$TOPIC_ID
python3 pipeline.py \
    --project=$PROJECT_ID \
    --region=$REGION \
    --temp_location=temp/ \
    --input_subscription=$SUBSCRIPTION_ID \
    --output_gcs=$GCS_PATH
