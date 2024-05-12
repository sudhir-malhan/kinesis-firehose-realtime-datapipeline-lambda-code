import json
import boto3
import csv
import os
import sys
from botocore.vendored import requests
import base64
import uuid
import logging
import psycopg2
import urllib
from botocore.exceptions import ClientError
from base64 import b64decode
from datetime import datetime, timedelta
from pytz import timezone
import time
import boto3
import csv
import os
import sys
from botocore.vendored import requests
import base64
import uuid
import logging
import psycopg2
from botocore.exceptions import ClientError

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


sm_client = boto3.client("secretsmanager", region_name="us-west-2")
get_secret_value_response = sm_client.get_secret_value(SecretId="redshift_login_info")

secret = get_secret_value_response['SecretString']
secret = json.loads(secret)

user = secret.get("master_user")
password = secret.get("master_pass")
dbname = secret.get("dbname")
host_url = secret.get("host")
schema = secret.get("schema")
port = secret.get("port")

conn_string = "dbname='{}' port='{}' user='{}' password='{}' host='{}'"\
                .format(dbname,port,user,password,host_url)


def lambda_handler(event, context):

    output = []
    return_output = []
    #print(event)
    
    for record in event['records']:
        recordId =record['recordId']
        return_output.append({'recordId': record['recordId'], 'result': 'Ok', 'data': record['data']})
        
        # Do custom processing on the payload here
        payload = base64.b64decode(record['data']).decode("utf-8")
        output.append({'recordId': record['recordId'], 'result': 'Ok', 'data': payload})
        #print(payload)

    # Logic to get S3 bucket - Json file for requested dataset
    # Include columns - recordId, result, data from Kinesis Firehose.
    #<s3-bucket-name>/SmartSocialDistancing/data/kinesisfirehose_rawsourcedata
    
    bucket = '<your-s3-bucket-name>'
    s3_client = boto3.client('s3',region_name='us-west-2')
    
    ssdalert_counter=1
    ssdalert_dict = dict()
    
    people_counter=1
    pplcount_dict = dict()
    
    def jsontocsv_socialdistancing_alert(data):
    	for d in data:
    	   if (d=='images'):
    	       for key in data[d]:
    	          ssdalert_dict[key]= data[d][key][0]
    	   if (d=='extras'):
    	  	   for key in data[d]:
    	  	   	  ssdalert_dict[key]= data[d][key] 
    	  	   	  #print(key,'->',data[d][key])       
    	   if not(d=='event' or d=='images' or d=='extras'):
    	       ssdalert_dict[d] = data[d]	
    	   if (d=='event'):
    	       for key in data[d]:
    	          if (key == 'tags'):
    	            for r in data[d][key]:
    	               ssdalert_dict[r]= data[d][key][r]
    	          if not(key == 'tags' or key == 'details'):
    	          	ssdalert_dict[key]= data[d][key]               
    	          if (key == 'details'):
    	            for x in data[d][key]:
    	               #print(x)
    	               i=0
    	               for l in data[d][key][x]:
    	               	  i=i+1               	
    	               	  for k in l:
    	               	  	if (k=='confidence'):
    	               	  		ssdalert_dict[k+str(i)]= l[k]  
		
    	lst = []     
    	dict_ssd = dict()
    	#print(ssdalert_dict.values())
    	for ssdalert_counter in range(1,i+1):
    		#print(ssdalert_counter)
    		lst.append([ssdalert_dict['ec2_ip'], ssdalert_dict['camera_type'], ssdalert_dict['device'], ssdalert_dict['camera_id'], ssdalert_dict['camera_name'], ssdalert_dict['camera_view'], ssdalert_dict['camera_model'], ssdalert_dict['camera_make'], ssdalert_dict['id'], ssdalert_dict['group_id'], ssdalert_dict['cloud'], ssdalert_dict['priority'], ssdalert_dict['building_id'], ssdalert_dict['type'], ssdalert_dict['confidence'+str(ssdalert_counter)], ssdalert_dict['timestamp'], ssdalert_dict['max_distance'], ssdalert_dict['min_distance'], ssdalert_dict['count']])
    		dict_ssd[ssdalert_counter] = [ssdalert_dict['ec2_ip'], ssdalert_dict['camera_type'], ssdalert_dict['device'], ssdalert_dict['camera_id'], ssdalert_dict['camera_name'], ssdalert_dict['camera_view'], ssdalert_dict['camera_model'], ssdalert_dict['camera_make'], ssdalert_dict['id'], ssdalert_dict['group_id'], ssdalert_dict['cloud'], ssdalert_dict['priority'], ssdalert_dict['building_id'], ssdalert_dict['type'], ssdalert_dict['confidence'+str(ssdalert_counter)], ssdalert_dict['timestamp'], ssdalert_dict['max_distance'], ssdalert_dict['min_distance'], ssdalert_dict['count']]
    		ssdalert_counter=ssdalert_counter+1
    	
    	#print('dict_ssd->',dict_ssd.values())
    	#print('list_ssd',lst)
    		
    	with open('/tmp/dev_ssdalert_kinesisToRedshift.csv', mode='a') as ssd_kinesis_rawcsvfile:
    	    writer = csv.writer(ssd_kinesis_rawcsvfile)
    	    #print(lst)
    	    for v in dict_ssd.values():
    	        writer.writerow(v)
    	        
    
    def jsontocsv_peoplecount(data):
    	for d in data:
    	   if (d=='images'):
    	       for key in data[d]:
    	          pplcount_dict[key]= data[d][key]
    	   if (d=='extras'):
    	  	   for key in data[d]:
    	  	   	  pplcount_dict[key]= data[d][key] 
    	  	   	  #print(key,'->',data[d][key])       
    	   if not(d=='event' or d=='images' or d=='extras'):
    	       pplcount_dict[d] = data[d]	
    	   if (d=='event'):
    	       for key in data[d]:
    	          if (key == 'tags'):
    	            for r in data[d][key]:
    	               pplcount_dict[r]= data[d][key][r]
    	          if not(key == 'tags' or key == 'details'):
    	          	pplcount_dict[key]= data[d][key]               
    	          if (key == 'details'):
    	            for x in data[d][key]:
    	               #print(x)
    	               i=0
    	               for l in data[d][key][x]:
    	               	  i=i+1               	
    	               	  for k in l:
    	               	  	if (k=='confidence'):
    	               	  		pplcount_dict[k+str(i)]= l[k] 
		
    	lst = []               	  		      
    	dict_ppl = dict()

    	for people_counter in range(1,i+1):
    		#print(people_counter)
    		lst.append([pplcount_dict['ec2_ip'], pplcount_dict['camera_type'], pplcount_dict['device'], pplcount_dict['camera_id'], pplcount_dict['camera_name'], pplcount_dict['camera_view'], pplcount_dict['camera_model'], pplcount_dict['camera_make'], pplcount_dict['id'], pplcount_dict['group_id'], pplcount_dict['cloud'], pplcount_dict['priority'], pplcount_dict['building_id'], pplcount_dict['type'], pplcount_dict['confidence'+str(people_counter)], pplcount_dict['timestamp'], pplcount_dict['count']])
    		dict_ppl[people_counter] = [pplcount_dict['ec2_ip'], pplcount_dict['camera_type'], pplcount_dict['device'], pplcount_dict['camera_id'], pplcount_dict['camera_name'], pplcount_dict['camera_view'], pplcount_dict['camera_model'], pplcount_dict['camera_make'], pplcount_dict['id'], pplcount_dict['group_id'], pplcount_dict['cloud'], pplcount_dict['priority'], pplcount_dict['building_id'], pplcount_dict['type'], pplcount_dict['confidence'+str(people_counter)], pplcount_dict['timestamp'], pplcount_dict['count']]
    		people_counter=people_counter+1
    	
    	#print('dict_ppl->',dict_ppl.values())
    	#print('list_peoplecount',lst)
    		
    	with open('/tmp/dev_peoplecount_kinesisToRedshift.csv', mode='a') as pplcount_kinesis_rawcsvfile:
    	    writer = csv.writer(pplcount_kinesis_rawcsvfile)
    	    for v in dict_ppl.values():
    	        writer.writerow(v)
    		
    def jsontocsv(output):
        
        bucket = 'digital-experience-wex-dl-dev'
        s3_client = boto3.client('s3',region_name='us-west-2')
        
        curr_date = datetime.now().astimezone(timezone('US/Pacific')).strftime("%Y-%m-%d")
        curr_timestamp = datetime.now().astimezone(timezone('US/Pacific')).strftime("%Y%m%d%H%M")

        dict = {}
        records_data = output
        print(records_data)
        
        with open('/tmp/dev_ssdalert_kinesisToRedshift.csv', mode='w') as ssd_kinesis_rawcsvfile:
            fieldnames = ['ec2_ip','camera_type','device','camera_id','camera_name','camera_view','camera_model','camera_make','id','group_id','cloud','priority','building_id','type','confidence','timestamp','max_distance','min_distance','count']
            writer = csv.writer(ssd_kinesis_rawcsvfile)
            writer.writerow(fieldnames)
            
            #ssd_filewriter(data)
            for y in records_data:
                main_data = json.loads(y.get('data'))
                #print(type(main_data))
                #print('Main_data->',main_data)
                rdata = main_data
                #print(type(rdata))
                #print('rdata->',rdata)
                ssd_csv_file_exists = 0
                
                if rdata.get('type')=='Social Distancing Alert':
                    jsontocsv_socialdistancing_alert(rdata)
                    ssd_csv_file_exists = ssd_csv_file_exists + 1
                    print('ssd_csv_file_exists=',ssd_csv_file_exists)
                    
        
        with open('/tmp/dev_peoplecount_kinesisToRedshift.csv', mode='w') as pplcount_kinesis_rawcsvfile:
            fieldnames = ['ec2_ip','camera_type','device','camera_id','camera_name','camera_view','camera_model','camera_make','id','group_id','cloud','priority','building_id','type','confidence','timestamp','count']
            writer = csv.writer(pplcount_kinesis_rawcsvfile)  
            writer.writerow(fieldnames)
            
            #pplcount_filewriter(data)
            for y in records_data:
                main_data = json.loads(y.get('data'))
                #print(type(main_data))
                #print('Main_data->',main_data)                
                rdata = main_data
                #print(type(rdata))
                #print('rdata->',rdata)
                pplcount_csv_file_exists = 0
                
                if rdata.get('type')=='Person Count Analysis':
                    jsontocsv_peoplecount(rdata)
                    pplcount_csv_file_exists = pplcount_csv_file_exists + 1
                    print('pplcount_csv_file_exists=',pplcount_csv_file_exists)

        
        if pplcount_csv_file_exists >0:
            s3_client.upload_file('/tmp/dev_peoplecount_kinesisToRedshift.csv', bucket, 'SmartSocialDistancing/data/kinesisfirehose_transformeddata/dev_peoplecount_kinesisToRedshift.csv')
            
        if ssd_csv_file_exists >0:
            s3_client.upload_file('/tmp/dev_ssdalert_kinesisToRedshift.csv', bucket, 'SmartSocialDistancing/data/kinesisfirehose_transformeddata/dev_ssdalert_kinesisToRedshift.csv')
        
    jsontocsv(output)
    #print(output)
    #print(output)
    
    sm_client = boto3.client("secretsmanager", region_name="us-west-2")
    get_secret_value_response = sm_client.get_secret_value(SecretId="redshift_login_info")
    
    secret = get_secret_value_response['SecretString']
    secret = json.loads(secret)

    conn_string = "dbname='{}' port='{}' user='{}' password='{}' host='{}'"\
                    .format(dbname,port,user,password,host_url)

    s3_client = boto3.client('s3')
    bucket = 'your-s3-bucket-name'
    ssd_csv_file_name = str('dev_ssdalert_kinesisToRedshift.csv')
    ssd_csv_file_path = 'SmartSocialDistancing/data/kinesisfirehose_transformeddata/'+ssd_csv_file_name
    ssd_s3locationCsvFile = bucket+"/"+ssd_csv_file_path
    
    ppl_csv_file_name = str('dev_peoplecount_kinesisToRedshift.csv')
    ppl_csv_file_path = 'SmartSocialDistancing/data/kinesisfirehose_transformeddata/'+ppl_csv_file_name
    pplcount_s3locationCsvFile = bucket+"/"+ppl_csv_file_path
    

    cleanup_query = """
                     TRUNCATE TABLE digital_exp_wex.smartsocialdistancing_data_staging;
                     TRUNCATE TABLE digital_exp_wex.peopleoccupancy_data_staging;
                    """
    pplcount_rawcsvdata_staging_tbl_load_query = """
        
        BEGIN;
        
        TRUNCATE TABLE digital_exp_wex.peopleoccupancy_data_staging;
        
        copy digital_exp_wex.peopleoccupancy_data_staging from 's3://digital-experience-wex-dl-dev/SmartSocialDistancing/data/kinesisfirehose_transformeddata/dev_peoplecount_kinesisToRedshift.csv'
        credentials 'aws_iam_role=arn:aws:iam::190985923409:role/Redshift-Execution-Role'
        format as csv
        ignoreheader 1
        maxerror as 1
        compupdate off statupdate off;
        
        INSERT INTO digital_exp_wex.peopleoccupancy_data 
        (event_timestamp, camera_id, camera_name, group_id
        , priority, event_type, event_count, confidence, event_id, cloud_image_s3url
        , ec2_ip, camera_type, device, camera_view, camera_model, camera_make, building_id) 
        SELECT sds.event_timestamp, sds.camera_id, sds.camera_name, sds.group_id, sds.priority
        , sds.event_type, sds.event_count, sds.confidence, sds.event_id, sds.cloud_image_s3url, sds.ec2_ip, sds.camera_type, sds.device
        , sds.camera_view, sds.camera_model, sds.camera_make, sds.building_id
        FROM (Select timestamp as event_timestamp, camera_id, camera_name, group_id, priority
                , type as event_type, count as event_count, confidence, id as event_id
                , cloud as cloud_image_s3url, ec2_ip, camera_type, device
                , camera_view, camera_model, camera_make, building_id
              From digital_exp_wex.peopleoccupancy_data_staging )sds
        LEFT JOIN digital_exp_wex.peopleoccupancy_data sd
        ON sds.event_count = sd.event_id and sds.group_id = sd.group_id
           and sds.camera_id = sd.camera_id and sds.confidence = sd.confidence
           and sds.event_timestamp = sd.event_timestamp
        WHERE sd.event_id IS NULL;  
        
        END;
       """

    #pplcount_rawcsvdata_staging_tbl_load_query = pplcount_rawcsvdata_staging_tbl_load_query.format(pplcount_s3locationCsvFile=pplcount_s3locationCsvFile);


    ssd_rawcsvdata_staging_tbl_load_query = """
        
        BEGIN;
        
        TRUNCATE TABLE digital_exp_wex.smartsocialdistancing_data_staging;
        
        copy digital_exp_wex.smartsocialdistancing_data_staging from 's3://<your-s3-bucket-name>/SmartSocialDistancing/data/kinesisfirehose_transformeddata/dev_ssdalert_kinesisToRedshift.csv'
        credentials 'aws_iam_role=arn:aws:iam::190985923409:role/Redshift-Execution-Role'
        format as csv
        ignoreheader 1
        maxerror as 1
        compupdate off statupdate off;
        
        INSERT INTO digital_exp_wex.smartsocialdistancing_data 
        (event_timestamp, camera_id, camera_name, group_id
        , priority, event_type, event_count, confidence, event_id, cloud_image_s3url
        , ec2_ip, camera_type, device, camera_view, camera_model, camera_make, building_id,max_distance,min_distance) 
        SELECT sds.event_timestamp, sds.camera_id, sds.camera_name, sds.group_id, sds.priority
        , sds.event_type, sds.event_count, sds.confidence, sds.event_id, sds.cloud_image_s3url, sds.ec2_ip, sds.camera_type, sds.device
        , sds.camera_view, sds.camera_model, sds.camera_make, sds.building_id, sds.max_distance, sds.min_distance
        FROM (Select timestamp as event_timestamp, camera_id, camera_name, group_id, priority
                , type as event_type, count as event_count, confidence, id as event_id
                , cloud as cloud_image_s3url, ec2_ip, camera_type, device
                , camera_view, camera_model, camera_make, building_id,max_distance,min_distance
              From digital_exp_wex.smartsocialdistancing_data_staging )sds
        LEFT JOIN digital_exp_wex.smartsocialdistancing_data sd
        ON sds.event_id = sd.event_id and sds.group_id = sd.group_id
           and sds.camera_id = sd.camera_id and sds.confidence = sd.confidence
           and sds.event_timestamp = sd.event_timestamp
        WHERE sd.event_id IS NULL;  
        
        END;
       """

    #ssd_rawcsvdata_staging_tbl_load_query = ssd_rawcsvdata_staging_tbl_load_query.format(ssd_s3locationCsvFile=ssd_s3locationCsvFile);


    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()

    try:

        curr_date = datetime.now().astimezone(timezone('US/Pacific')).strftime("%Y-%m-%d")
        #print('pplcount_csv_file_exists_out=',pplcount_csv_file_exists_out)
        #print('ssd_csv_file_exists_out=',ssd_csv_file_exists_out)
        
        #if pplcount_csv_file_exists_out>0:
        cur.execute(pplcount_rawcsvdata_staging_tbl_load_query)
        conn.commit()
        #print("pplcount_rawcsvdata_staging_tbl_load_query executed successfully")
        
        #if ssd_csv_file_exists_out>0:
        cur.execute(ssd_rawcsvdata_staging_tbl_load_query)
        conn.commit()    
        #print("ssd_rawcsvdata_staging_tbl_load_query executed successfully")
            
        cur.execute(cleanup_query)
        conn.commit()    

        curr_timestamp = datetime.now().astimezone(timezone('US/Pacific')).strftime("%Y%m%d%H%M")
        
        print("Command executed successfully")
        
        cur.execute(cleanup_query)
        conn.commit()

        time_now = datetime.now()
        flag = 0

        
    except Exception as ex:
        LOGGER.info("Main Exception: %s", str(ex))
        print("Failed to execute copy command")

        ses = boto3.client('ses')
        sns = boto3.client('sns')
        
        response = ses.send_email(
                                    Source='abc@gmail.com',
                                    Destination={
                                        'ToAddresses': [
                                            'abc@gmail.com',
                                            'abc@gmail.com'
                                        ]
                                    },
                                    Message={
                                        'Subject': {
                                            'Data': 'SmartSocialDistancing Prod lambda has failed with EXCEPTION = {}'.format(str(ex))
                                        },
                                        'Body': {
                                            'Text': {
                                                'Data': 'SmartSocialDistancing Prod lambda has failed with EXCEPTION = {}'.format(str(ex))
                                            },
                                            'Html': {
                                                'Data': 'SmartSocialDistancing Prod lambda has failed with EXCEPTION = {}'.format(str(ex))
                                            }
                                        }
                                    }
                                )
        print(response)
        
        response = sns.publish(
                                    TopicArn='arn:aws:sns:us-west-2:<aws-account-id>:smart_social_distancing_lambda',
                                    Message='SmartSocialDistancing Prod lambda has failed with EXCEPTION = {}'.format(str(ex))
                                  )
            
        print(response)
        
        cur.execute(cleanup_query)
        conn.commit()
    finally:
        cur.execute(cleanup_query)
        conn.commit()
        cur.close()
        conn.close()
        print('Done')


    return {'records': return_output}
