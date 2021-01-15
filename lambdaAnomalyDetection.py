from __future__ import print_function
import base64
import boto3
from botocore.config import Config
DATABASE_NAME = "problem-solvers"
TABLE_NAME = "output_data"
HT_TTL_HOURS = 8000
CT_TTL_DAYS = 360
import time
import ast
import pandas as pd


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

    records = getRecords(records)
    records = getWriteDataFrame(records)
    bulk_write_records(records,write_client)

def getRecords(records):
    records_to_write = []
    for row in records:
        try:
            row = ast.literal_eval(row)
            record = [str(row["dt_obj"]) , str(row["casing_pressure"]),str(row["tubing_pressure"]),
                      str(row["line_pressure"]),str(row["ct_differential"]),str(row["lt_differential"])
                      ,str(row["oil_volume"])]
            records_to_write.append(record)
        except Exception as e:
            print("Error reading the record :" + str(e.__class__))
            print(e)

    return records_to_write
        # print("Now calculating dimensions :")



def getWriteDataFrame(records):

    df_new = pd.DataFrame(records, columns=['dt_obj', 'casing_pressure','tubing_pressure',
                                            'line_pressure','ct_differential',
                                            'lt_differential','oil_volume'])
    df_new = df_new.astype({"dt_obj": str, "casing_pressure": float,"tubing_pressure": float,"line_pressure": float,
                            "ct_differential": float,"lt_differential": float,"oil_volume": float})
    #dict_new = {'casing_pressure': casing_pressure, 'tubing_pressure': tubing_pressure, 'line_pressure': line_pressure,
     #           'oil_volume': oil_volume}
    #df_new = pd.DataFrame(dict_new)

    try:
        df_thresholds = pd.read_csv("Anomaly_Thresholds.csv")
        # Create a dictionary to contain the anomaly thresholds
        thresholds_dict = df_thresholds.to_dict(orient="records")[0]

        # Use the theresholds to determine if data points are anomalies (Put a 1 for anomaly and 0 for non-anomaly)
        df_new['casing_pressure_anomaly'] = (
                (df_new['casing_pressure'] > thresholds_dict['casing_pressure_upper_OL_threshold']) | (
                df_new['casing_pressure'] < thresholds_dict['casing_pressure_lower_OL_threshold'])).apply(int)
        df_new['tubing_pressure_anomaly'] = (
                (df_new['tubing_pressure'] > thresholds_dict['tubing_pressure_upper_OL_threshold']) | (
                df_new['tubing_pressure'] < thresholds_dict['tubing_pressure_lower_OL_threshold'])).apply(int)
        df_new['line_pressure_anomaly'] = (
                (df_new['line_pressure'] > thresholds_dict['line_pressure_upper_OL_threshold']) | (
                df_new['line_pressure'] < thresholds_dict['line_pressure_lower_OL_threshold'])).apply(int)
        df_new['oil_volume_anomaly'] = ((df_new['oil_volume'] > thresholds_dict['oil_volume_upper_OL_threshold']) | (
                df_new['oil_volume'] < thresholds_dict['oil_volume_lower_OL_threshold'])).apply(int)

        # In[6]:

        # Write the results to a new dataframe (New_Data_with_Anomalies)
        # df_new.to_csv("New_Data_with_Anomalies.csv", index=
    except Exception as e:
        print("Erroring out in the getWriteDataFrame" + str(e.__class__))
        print (e)


    return df_new.values.tolist()


def bulk_write_records(records,write_client):
    try:
        counter = 0
        print("Number of records to be processed in this stream is :" + str(len(records)))
        records_to_write = []
        for row in records:

            try:
                current_time = _current_milli_time()

                # print("Now calculating dimensions :")
                dimensions = [
                    {'Name': 'Time_recorded', 'Value': str(row[0])},
                    {'Name': 'casing_pressure', 'Value': str(row[1])},
                    {'Name': 'tubing_pressure', 'Value': str(row[2])},
                    {'Name': 'line_pressure', 'Value': str(row[3])},
                    {'Name': 'ct_differential', 'Value': str(row[4])},
                    {'Name': 'lt_differential', 'Value': str(row[5])},
                    {'Name': 'casing_pressure_anomaly', 'Value': str(row[7])},
                    {'Name': 'tubing_pressure_anomaly', 'Value': str(row[8])},
                    {'Name': 'line_pressure_anomaly', 'Value': str(row[9])},
                    {'Name': 'oil_volume_anomaly', 'Value': str(row[10])},
                ]
                # print("Now calculating oil volume :")
                oil_volume = {
                    'Dimensions': dimensions,
                    'MeasureName': 'oil_volume',
                    'MeasureValue': str(row[6]),
                    'MeasureValueType': 'DOUBLE',
                    'Time': current_time
                }

            except Exception as e:
                print("Error reading the record :" + str(e.__class__))
                print (e)

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
        print(ex)

def _current_milli_time():
    return str(int(round(time.time() * 1000)))