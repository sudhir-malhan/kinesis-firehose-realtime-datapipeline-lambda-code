
import requests
import boto3
import csv
import json
import collections

url='http://10.37.16.61:8080/VccWebService/Json/PGS_GetPublicCarparksStallCount'
#payload = open("request.json")
r = requests.get(url)
#print(r.text)
#print(type(r.text))
data = json.loads(r.text)
#print(type(data))
#print(data)

def convert(data):
    if isinstance(data, basestring):
        return str(data)
    elif isinstance(data, collections.Mapping):
        return dict(map(convert, data.iteritems()))
    elif isinstance(data, collections.Iterable):
        return type(data)(map(convert, data))
    else:
        return data

new_data = convert(data)
#print(type(new_data), new_data)


AsOf = new_data['AsOf']

adict = {}
n = 1
for carpark in new_data['Carparks']:
    bdict = {'AsOf': AsOf}
    bdict['CarparkId'] = carpark['CarparkId']
    bdict['CarparkName'] = carpark['CarparkName']
    summary = carpark['CarparkSummary']
    for k0,v0 in summary.items():
        bdict['CarparkName_' + k0] = v0
        for level in carpark['Levels']:
            for k1,v1 in level.items(): 
                if k1 != 'LevelCount':
                    bdict['CarparkName_' + k1] = v1
                else:
                    for k2,v2 in level[k1].items():
                        bdict['CarparkName_LevelCount_' + k2] = v2
    adict[n] = bdict
    n += 1


string = str(new_data)
left = string.find('[')
string = string[:left+1] + string[left:].replace('[', '')
right = string.rfind(']')
string = string[:right].replace(']', '') + string[right:]
string = string.replace('\'', '"')
string1 = unicode(string, "utf-8")
print(string1)

client = boto3.client("kinesis", region_name = 'us-west-2')
partition_key = 'test_record'
response = client.put_record(StreamName = 'CS_SS_Kinesis_Stream_PSUtilization',
			     Data = string1, PartitionKey = partition_key)
print(response)