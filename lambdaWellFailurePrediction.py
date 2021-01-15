from __future__ import print_function
import base64
from botocore.config import Config
DATABASE_NAME = "problem-solvers"
TABLE_NAME = "input_data"
HT_TTL_HOURS = 8000
CT_TTL_DAYS = 360
import ast
import pandas as pd
from sklearn import metrics
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
import boto3
sns = boto3.client('sns')

def lambda_handler(event, context):
    records = []
    session = boto3.Session(region_name='us-east-1')
    print("Staring well failure prediction ")
    write_client = session.client('timestream-write', config=Config(read_timeout=20, max_pool_connections=5000,
                                                                    retries={'max_attempts': 10}))
    for record in event['Records']:
       #Kinesis data is base64 encoded so decode here
       payload=base64.b64decode(record["kinesis"]["data"])
       #print("Decoded payload: " + str(payload))
       records.append(payload.decode("utf-8"))

    recordDF = getDFNew(records)
    scoreFromDF = getWriteDataFrame(recordDF)
    if scoreFromDF < 0.95:
        send_email_records(scoreFromDF)



def getWriteDataFrame(recordDF):
    print("Entering getWriteDataFrame.")

    # make model
    try:
        try:
            print("Reading Data.csv")
            df = pd.read_csv('Data.csv')
            dataTypeSeries_old = df.dtypes
            print("Column Types :")
            print(dataTypeSeries_old)
            df['casing_pressure_squared'] = df['casing_pressure'] ** 2
            df['tubing_pressure_squared'] = df['tubing_pressure'] ** 2
            df['line_pressure_squared'] = df['line_pressure'] ** 2
            X = df[['casing_pressure', 'tubing_pressure', 'line_pressure', 'casing_pressure_squared',
                    'tubing_pressure_squared', 'line_pressure_squared']]
            y = df['oil_volume']
            #X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=4)
            lm = LinearRegression()
            lm.fit(X, y)
            print("Done Training")
        except Exception as ex:
            print("Erroring out while running the model" + str(ex.__class__))
            print(ex)
# make dataframe with new data
        try :
            df_new = pd.DataFrame(recordDF, columns=['casing_pressure', 'tubing_pressure', 'line_pressure', 'oil_volume'])
            print(df_new)
            dataTypeSeries = df_new.dtypes
            print("Column Types :")
            print(dataTypeSeries)

        # df_new = recordDF[['casing_pressure','tubing_pressure','line_pressure','oil_volume']]
            df_new['casing_pressure_squared'] = df_new['casing_pressure'] ** 2
            df_new['tubing_pressure_squared'] = df_new['tubing_pressure'] ** 2
            df_new['line_pressure_squared'] = df_new['line_pressure'] ** 2

            X_new = df_new[['casing_pressure', 'tubing_pressure', 'line_pressure', 'casing_pressure_squared',
                        'tubing_pressure_squared', 'line_pressure_squared']]
            y_new = df_new['oil_volume']
            print("Done calculating X_new and y_new")
            score = 0.95
            X_new = X_new.round(2)
            predictions = lm.predict(X_new)
            predictions = predictions.round(2)
            y_new = y_new.round(2)
        except Exception as exp:
            print("Erroring out in the getWriteDataFrame" + str(exp.__class__))
            print(exp)
        print("Now calculating score ")

        score = metrics.r2_score(y_new, predictions)
        print("Score is " + str(score))
    except Exception as e:
        print("Score is :" + str(score))
        print("Erroring out in the getWriteDataFrame" + str(e.__class__))
        print (e)
    return score


def send_email_records(scoreFromDF):
    try:

        response = sns.publish(
            TopicArn="arn:aws:sns:us-east-1:526453079014:problemsolvers",
            Message="There is : " + str(scoreFromDF*100) + "% chance of well failure "
        )
        print(response)
    except Exception as e:
        print("Error in send_email_records")
        print(e)


def getDFNew(records):
    records_to_write = []
    print("Entering getDFNew")
    for row in records:
        try:
            row = ast.literal_eval(row)
            record = [str(row["dt_obj"]) , str(row["casing_pressure"]),str(row["tubing_pressure"]),
                      str(row["line_pressure"]),str(row["ct_differential"]),str(row["lt_differential"])
                      ,str(row["oil_volume"])]
            records_to_write.append(record)
        except Exception as e:
            print("Error in getDFNew :" + str(e.__class__))
            print(e)

    df_new = pd.DataFrame(records_to_write, columns=['dt_obj', 'casing_pressure', 'tubing_pressure',
                                            'line_pressure', 'ct_differential',
                                            'lt_differential', 'oil_volume'])
    df_new = df_new.astype(
        {"dt_obj": str, "casing_pressure": float, "tubing_pressure": float, "line_pressure": float,
         "ct_differential": float, "lt_differential": float, "oil_volume": float})
    return df_new
