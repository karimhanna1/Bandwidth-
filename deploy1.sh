#!/bin/sh

project_name=$(gcloud config get-value project)

#setup Aggregate Query Stream
AggregateQuery="SELECT 
    CUSTOMER_ID,
    MESSAGE_STATUS,	
    RECORD_TYPE,
    DLR_CODE,
    PRODUCT,
    MESSAGE_DIRECTION,
    COUNT(MDR_ID) as MESSAGE_COUNT,
    MESSAGE_DATE,	
    EXTRACT(HOUR FROM event_timestamp) as MESSAGE_HOUR,	
    EXTRACT(MINUTE FROM event_timestamp) as MESSAGE_MINUTE,	
    MAX(event_timestamp) as MESSAGE_DATE_HR_MIN,	
    CALLING_NUMBER,
    AMP_NAME,
    PROVIDER_NAME,	
    CALLED_NUMBER_COUNTRY,	
    MAX(event_timestamp) as SOURCE_INSERT_TIMESTAMP,	
    MAX(event_timestamp) as INSERT_TIMESTAMP,
    MAX(event_timestamp) as UPDATE_TIMESTAMP,	
    CUSTOMER_NAME,
    CALLED_NUMBER_STATE,	
    BILLABLE
FROM pubsub.topic.${project_name}.IncomingV2
Group By 
    TUMBLE(event_timestamp, 'INTERVAL 1 MINUTE'),
    CUSTOMER_ID,
    MESSAGE_STATUS,	
    RECORD_TYPE,
    DLR_CODE,
    PRODUCT,
    MESSAGE_DIRECTION,	
    EXTRACT(HOUR FROM event_timestamp),	
    EXTRACT(MINUTE FROM event_timestamp),	
    EXTRACT(HOUR FROM event_timestamp),	
    CALLING_NUMBER,
    AMP_NAME,
    PROVIDER_NAME,	
    CALLED_NUMBER_COUNTRY,
    CUSTOMER_NAME,	
    CALLED_NUMBER_STATE,	
    BILLABLE, 
    MESSAGE_DATE"

   gcloud dataflow sql query "${AggregateQuery}" --job-name='dfsql-aggregates' --region us-central1 --bigquery-write-disposition write-empty --bigquery-project ${project_name} --bigquery-dataset InsightsV2a --bigquery-table REALTIME_MDR_AGGREGATE


