# Hasura
requirements: 
  scalaVersion = 2.11.12
  sparkVersion = 2.4.7
  pyspark installed

The code consists of 2 files: 
    1. batchProcess.scala
    2. realTimeProcess.py

1. batchProcess.scala :
    This code accepts 3 parameters
        1. file_path
        2. bill_start_time (format yyyy-MM-dd HH:mm:ss)
        3. bill_end_time (format yyyy-MM-dd HH:mm:ss)
    
    and returns result in 2 formats:
      1. grouped on project_id and bill_date
				|-- project_id: string
				|-- bill_date: date
				|-- total_response_size: long
				|-- total_request_size: long
				|-- project_io: long
				|-- bill_start_time: string
				|-- bill_end_time: string
        
      2. grouped on project_id and bill_date and http_status
				|-- project_id: string
				|-- bill_date: date
				|-- http_status: integer
				|-- total_response_size: long
				|-- total_request_size: long
				|-- project_io: long
				|-- bill_start_time: string
				|-- bill_end_time: string
    
    CODE description :
    STEP 1 : read file
    STEP 2 : parse json create df
    STEP 3 : clean timestamp column and add bill_date column
    STEP 4 : Group by required columns to calculate bill 

2. realTimeProcess.py:
   In this code we subscribe to a kafka topic and create a dataframe for specified timeframe
   then we write spark sql query to transform data to get IO/billing info and write it to target file location
   the bill generation can run as a wrapper everyday or at specefied time duration
   
   STEP1 : subscribe to kafka topic
   STEP2 : create dataframe from log data
   STEP3 : clean data and apply transformation
   STEP4 : write df to target location 
