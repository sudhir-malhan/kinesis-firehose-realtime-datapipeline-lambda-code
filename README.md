# kinesis-firehose-realtime-datapipeline-lambda-code

**Problem statement:-**
Business wants a highly available, scalable and secure data processing solution that scales up or down based on the data pulled/ingested for processing JSON format data.

**Solution:-**
Design a Kinesis data pipeline. This entails spinning up a KDS - Kinesis Data Stream first where data is ingested from data sources such as sensors, API calls made from EC2 to be ingested in KDS and then from KDS it is ingested into Kinesis Firehose as data source for further processing to satiate data warehousing needs.

This repo provides lambda code for Kinesis Firehose Realtime data pipeline data consumption coming as Nested JSON format. This nested JSON data format is then flattened in rows, transformed as per business rules, converted to CSV data file format and then written to staging area in S3 bucket which is finally written to database tables in Redshift.

Code imports all necessary Main libraries, provides Logging feature, reads secrets from secrets manager, reading Nested JSON data from Kinesis Firehose, TRY-CATCH block, FINALLY block to cleanup staging tables and commit and close all database connections, Error Email Notifications and SNS Text notification via SNS Topic configured to a Mobile phone.
