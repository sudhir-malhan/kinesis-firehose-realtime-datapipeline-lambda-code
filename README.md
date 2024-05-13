# kinesis-firehose-realtime-datapipeline-lambda-code

**Problem statement:-**
Business wants a highly available, scalable and secure data stream processing solution that scales up or down based on the data pulled/ingested for processing JSON format data.

**Solution:-**
Design a Kinesis Firehose data pipeline. 

**Source:-**

Options - Select the source for your data stream, such as a topic in Amazon Managed Streaming for Kafka (MSK), a stream in Kinesis Data Streams, or write data using the Firehose Direct PUT API. Amazon Data Firehose is integrated into many other AWS services, so you can set up a stream from sources such as Amazon CloudWatch Logs, AWS WAF web ACL logs, AWS Network Firewall Logs, Amazon SNS, or AWS IoT, etc.


**KDS as data source:-** 
This entails spinning up a KDS - Kinesis Data Stream using AWS Console first. 
Then, Kinesis Agent is installed on an AWS EC2 running within AWS VPC. This EC2 runs a python script scheduled via cronjob to run every few seconds in order to make API calls to a 3rd Party App hosted on an on-premise Server and get data in JSON format to be ingested into the above created KDS (Kinesis Data Stream). Code included in this repo as example.

**Kinesis Firehose for processing data:-**
A New Kinesis Firehose pipeline is created using AWS Console configured with an IAM role for all the permissions that the Firehose stream needs. 
KDS is selected as data source for this Kinesis Firehose pipeline.
KDS is integrated with above created Kinesis Firehose for receiving data for further processing. Any failed data goes to a pre-configured failed S3 bucket folder and Raw data goes to another source S3 bucket folder whereas staging data created after transformations and business logic using lambda code is staged in S3 bucket folder. This staged data is finally loaded into Redshift for archival in data warehouse, running adhoc analytics on Redshift cluster and reporting needs by pulling Tables/Views in Tableau dashboard.

Kinesis Firehose - Lambda code:-
This repo includes lambda code for Kinesis Firehose data pipeline data consumption coming in Nested JSON format. This nested JSON data format is then flattened in rows, transformed as per business rules, converted to CSV data file format and then written to staging area in S3 bucket which is finally written to database tables in Redshift.

Code imports all necessary Main libraries, provides Logging feature, reads secrets from secrets manager, reading Nested JSON data from Kinesis Firehose, TRY-CATCH block, FINALLY block to cleanup staging tables and commit and close all database connections, Error Email Notifications and SNS Text notification via SNS Topic configured to a Mobile phone.

**Target:-**
Redshift cluster
