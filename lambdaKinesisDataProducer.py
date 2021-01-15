import boto3
import json
from datetime import datetime
import calendar
import random
import time
import math
import csv
my_stream_name = 'problems-solvers'
kinesis_client = boto3.client('kinesis', region_name='us-east-1')
def lambda_handler(event, context):

    records = getRecords('SogetiData.csv')
    for record in records:
        property_value = record
        put_to_stream(record[0],record[1],record[2],record[3],record[4],record[5],record[6])
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

def put_to_stream(dt_obj,casing_pressure,tubing_pressure,line_pressure,ct_differential,lt_differential,oil_volume):
    payload = {
        'dt_obj': dt_obj,
        'casing_pressure': casing_pressure,
        'tubing_pressure': tubing_pressure,
        'line_pressure' : line_pressure,
        'ct_differential': ct_differential,
        'lt_differential' : lt_differential,
        'oil_volume': oil_volume,
        'well_name' : 'applachia'
              }

    print(payload)
    try:
        put_response = kinesis_client.put_record(
                        StreamName=my_stream_name,
                        Data=json.dumps(payload),
            PartitionKey=payload.get('well_name')
                        )
    except Exception as e:
        print(e)


def getRecords(filepath):
    records = []
    try:
        with open(filepath, 'r') as csv_file:
            # creating a csv reader object
            csv_reader = csv.reader(csv_file)
            next(csv_reader)
            counter =0

            # extracting each data row one by one
            for row in csv_reader:
                dt_obj = str(datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S'))
                record = []
                casing_pressure = row[1]

                tubing_pressure = row[2]
                line_pressure = row[3]
                ct_differential = row[4]
                lt_differential = row[5]
                oil_volume = row[6]
                record.append(dt_obj)
                record.append(casing_pressure)
                record.append(tubing_pressure)
                record.append(line_pressure)
                record.append(ct_differential)
                record.append(lt_differential)
                record.append(oil_volume)
                counter = counter + 1
                records.append(record)

            print("Ingested %d records" % counter)
    except Exception as e:
        print (e)
    return records