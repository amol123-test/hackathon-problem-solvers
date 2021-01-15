from __future__ import print_function
import base64
import boto3
from botocore.config import Config
DATABASE_NAME = "problem-solvers"
TABLE_NAME = "input_data"
HT_TTL_HOURS = 8000
CT_TTL_DAYS = 360
import time
import ast


def lambda_handler(event, context):
    records = []
    session = boto3.Session(region_name='us-east-1')
    write_client = session.client('timestream-write', config=Config(read_timeout=20, max_pool_connections=5000,
                                                                    retries={'max_attempts': 10}))
    for record in event['Records']:
       #Kinesis data is base64 encoded so decode here
       payload=base64.b64decode(record["kinesis"]["data"])
       #print("Decoded payload: " + str(payload))
       records.append(payload.decode("utf-8"))

    bulk_write_records(records,write_client)

def bulk_write_records(records,write_client):
    try:
        counter = 0
        print("Number of records to be processed in this stream is :" + str(len(records)))
        records_to_write = []
        for row in records:

            try:
                row = ast.literal_eval(row)
            except Exception as e:
                print("Error reading the record :" + str(e.__class__))
                print (e)

            current_time = _current_milli_time()

            #print("Now calculating dimensions :")
            dimensions = [
                {'Name': 'Time_recorded', 'Value': str(row["dt_obj"])},
                {'Name': 'casing_pressure', 'Value': str(row["casing_pressure"])},
                {'Name': 'tubing_pressure', 'Value': str(row["tubing_pressure"])},
                {'Name': 'line_pressure', 'Value': str(row["line_pressure"])},
                {'Name': 'ct_differential', 'Value': str(row["ct_differential"])},
                {'Name': 'lt_differential', 'Value': str(row["lt_differential"])}
            ]
            #print("Now calculating oil volume :")
            oil_volume = {
                'Dimensions': dimensions,
                'MeasureName': 'oil_volume',
                'MeasureValue': str(row["oil_volume"]),
                'MeasureValueType': 'DOUBLE',
                'Time': current_time
            }
            records_to_write.append(oil_volume)
            counter = counter + 1

            if len(records_to_write) == 100:
                print("Now submitting records :")
                _submit_batch(records_to_write, counter, write_client)
                records = []

        if len(records) != 0:
            _submit_batch(records_to_write, counter,write_client)

        print("Total Written %d records to timestream in this streaming batch" % counter)
    except Exception as e:
        print("Erroring out in the bulk_write_records" + str(e.__class__))
        print (e)

def _submit_batch(records_to_write, counter,write_client):
    try:
        #print("Writing %d records to timestream" % counter)
        result = write_client.write_records(DatabaseName=DATABASE_NAME, TableName=TABLE_NAME,
                                           Records=records_to_write, CommonAttributes={})
        print("Processed [%d] records. WriteRecords Status: [%s]" % (counter,
                                                                     result['ResponseMetadata']['HTTPStatusCode']))
    except Exception as ex:
        print("Error:", str(ex.__class__))
        print(ex.response)

def _current_milli_time():
    return str(int(round(time.time() * 1000)))