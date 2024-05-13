# kinesis-firehose-realtime-datapipeline-lambda-code

**Problem statement:-**
Business wants a highly available, scalable and secure data stream processing solution that scales up or down based on the data pulled/ingested for processing JSON format data.

**Solution:-**
Design a Kinesis Firehose data pipeline. 

**Source:-**

Options - Select the source for your data stream, such as a topic in Amazon Managed Streaming for Kafka (MSK), a stream in Kinesis Data Streams, or write data using the Firehose Direct PUT API. Amazon Data Firehose is integrated into many other AWS services, so you can set up a stream from sources such as Amazon CloudWatch Logs, AWS WAF web ACL logs, AWS Network Firewall Logs, Amazon SNS, or AWS IoT, etc.


**KDS as data source:-** 
This entails spinning up a KDS - Kinesis Data Stream using AWS Console first. 
Then, Kinesis Agent is installed on an AWS EC2 running within AWS VPC. This EC2 runs python script scheduled via cronjob to run every few seconds in order to make API calls and get data in JSON format and ingest into the above created KDS (Kinesis Data Stream). from data sources such as API calls made by running python script on AWS EC2 where Kinesis Agent is installed which helps ingest data into in KDS (Kinesis Data Stream). Code included in this repo as example.

**Kinesis Firehose for processing data:-**
Kinesis Firehose pipeline is created using AWS Console.
KDS is selected as data source for the Kinesis Firehose pipeline.
Data from KDS is pulled and ingested into above created Kinesis Firehose for further processing to be staged in S3 bucket and finally loaded into Redshift for archival in data warehouse, running adhoc analytics on Redshift cluster and reporting needs by pulling Tables/Views in Tableau dashboard.

Kinesis Firehose - Lambda code:-
This repo includes lambda code for Kinesis Firehose data pipeline data consumption coming in Nested JSON format. This nested JSON data format is then flattened in rows, transformed as per business rules, converted to CSV data file format and then written to staging area in S3 bucket which is finally written to database tables in Redshift.

Code imports all necessary Main libraries, provides Logging feature, reads secrets from secrets manager, reading Nested JSON data from Kinesis Firehose, TRY-CATCH block, FINALLY block to cleanup staging tables and commit and close all database connections, Error Email Notifications and SNS Text notification via SNS Topic configured to a Mobile phone.

**Target:-**
Redshift cluster
