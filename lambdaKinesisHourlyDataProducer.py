import boto3
import json
from datetime import datetime
import calendar
import random
import math
import csv
from scipy.linalg import eigh, cholesky
from scipy.stats import norm
import pandas as pd
from datetime import datetime
import numpy as np
from dateutil.relativedelta import relativedelta
import pytz
local_tz = pytz.timezone('America/Chicago')
my_stream_name = 'problems-solvers'
kinesis_client = boto3.client('kinesis', region_name='us-east-1')


def lambda_handler(event, context):
    df_new = generate_hour()
    df_new = df_new.rename(columns={"date": "dt_obj"})

    # df_new = df_new[['Time_recorded', 'casing_pressure','tubing_pressure','line_pressure',
    #                 'ct_differential','lt_differential','oil_volume']]
    # pd.set_option('display.max_columns', None)

    records = df_new.values.tolist()
    for record in records:
        property_value = record
        put_to_stream(record[0], record[1], record[2], record[3], record[4], record[5], record[6])
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


def put_to_stream(dt_obj, casing_pressure, tubing_pressure, line_pressure, ct_differential, lt_differential,
                  oil_volume):
    payload = {
        'dt_obj': dt_obj,
        'casing_pressure': casing_pressure,
        'tubing_pressure': tubing_pressure,
        'line_pressure': line_pressure,
        'ct_differential': ct_differential,
        'lt_differential': lt_differential,
        'oil_volume': oil_volume,
        'well_name': 'applachia'
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


def generate(median, err, outlier_err, size=60, outlier_size=0, outlier_end="low"):
    errs = err * np.random.rand(size) * np.random.choice((-1, 1), size)  # random noise
    data = median + errs  # non-outlier data

    prob = np.random.uniform(0, 1)  # 50-50 probability of being a high outlier or a low outlier

    # if prob is less than 0.5, then choose outliers below the median.
    # if prob is above 0.5, then choose outliers above the median.

    if prob < 0.5:
        outlier_end = "low"
    else:
        outlier_end = "high"

    finalOutliers = []  # final list of non-negative outliers
    if outlier_end == "low":
        lower_errs = outlier_err * np.random.rand(outlier_size)

        outliers = median - err - lower_errs
        # if any outliers in the list above is negative, otherwise choose a random number between 10 and median-err-outlier_err
        for outlier in outliers:
            if outlier < 0:
                finalOutliers.append(np.random.uniform(10, median - err - outlier_err))
            else:
                finalOutliers.append(outlier)  # otherwise leave the outlier as is

    else:
        upper_errs = outlier_err * np.random.rand(
            outlier_size)  # if the outlier to be generated is to be above the median
        # then add the median, err and upper_errs
        outliers = median + err + upper_errs
        finalOutliers = outliers  # use this as the final list of outliers

    finalOutliers = [round(num, 2) for num in finalOutliers]

    data = np.concatenate((data, finalOutliers))  # concatenate the data
    # np.random.shuffle(data)

    return data


count = 0


def generate_hour():
    end = datetime.now()
    end = end.astimezone(local_tz)
    print("Time now is :")
    print(end)
    start = end + relativedelta(minutes=-59)
    start = start.astimezone(local_tz)
    global count
    # casing pressure constants
    CP_MEDIAN = 2100
    CP_ERR = 500
    CP_OUTLIER_ERR = 1200
    # tubing pressure constants
    TP_MEDIAN = 2100
    TP_ERR = 400
    TP_OUTLIER_ERR = 730
    # line pressure constants
    LP_MEDIAN = 2600
    LP_ERR = 900
    LP_OUTLIER_ERR = 1890

    casing_pressure = []
    tubing_pressure = []
    line_pressure = []

    date_rng = pd.date_range(start=start, end=end, freq='min')
    date_rng = date_rng.strftime("%Y-%m-%d %H:%M:%S")
    print(len(date_rng))
    df = pd.DataFrame(date_rng, columns=['date'])

    if count == 13:
        casing_pressure.extend(
            generate(median=CP_MEDIAN, err=CP_ERR, outlier_err=CP_OUTLIER_ERR, size=0, outlier_size=60,
                     outlier_end="low"))
        tubing_pressure.extend(
            generate(median=TP_MEDIAN, err=TP_ERR, outlier_err=TP_OUTLIER_ERR, size=60, outlier_size=60,
                     outlier_end="low"))
        line_pressure.extend(
            generate(median=LP_MEDIAN, err=LP_ERR, outlier_err=LP_OUTLIER_ERR, size=60, outlier_size=60,
                     outlier_end="low"))
    else:
        casing_pressure.extend(
            generate(median=CP_MEDIAN, err=CP_ERR, outlier_err=CP_OUTLIER_ERR, size=60, outlier_size=0,
                     outlier_end="low"))
        tubing_pressure.extend(
            generate(median=TP_MEDIAN, err=TP_ERR, outlier_err=TP_OUTLIER_ERR, size=60, outlier_size=0,
                     outlier_end="low"))
        line_pressure.extend(generate(median=LP_MEDIAN, err=LP_ERR, outlier_err=LP_OUTLIER_ERR, size=60, outlier_size=0,
                                      outlier_end="low"))

    df['casing_pressure'] = casing_pressure
    df['tubing_pressure'] = tubing_pressure
    df['line_pressure'] = line_pressure

    method = 'cholesky'
    # method = 'eigenvectors'

    # The desired covariance matrix.
    r = np.array([
        [3.40, 1.25, 1.1],
        [1.25, 4.20, 1.50],
        [1.1, 1.50, 1.25]
    ])

    # Generate samples from three independent normally distributed random

    # We need a matrix `c` for which `c*c^T = r`.  We can use, for example,
    # the Cholesky decomposition, or the we can construct `c` from the
    # eigenvectors and eigenvalues.
    x = []
    x.append(df['casing_pressure'])
    x.append(df['tubing_pressure'])
    x.append(df['line_pressure'])
    if method == 'cholesky':
        # Compute the Cholesky decomposition.
        c = cholesky(r, lower=True)
    else:
        # Compute the eigenvalues and eigenvectors.
        evals, evecs = eigh(r)
        # Construct c, so c*c^T = r.
        c = np.dot(evecs, np.diag(np.sqrt(evals)))

        # Convert the data to correlated random variables.
    y = np.dot(c, x)

    df['casing_pressure'] = y[0]
    df['tubing_pressure'] = y[1]
    df['line_pressure'] = y[2]

    df['ct_differential'] = df['casing_pressure'] - df[
        'tubing_pressure']  # difference between casing pressure and tubing pressure
    df['lt_differential'] = df['line_pressure'] - df[
        'tubing_pressure']  # difference between line pressure and tubing pressure

    NUM_MINUTES = df.shape[0]

    # creates oil volume column out of the tube pressure and adds noise to it
    df['oil_volume'] = 0.000001 * (df['tubing_pressure'] - 7000) ** 2 + 3000 + CP_ERR * np.random.normal(0, 1,
                                                                                                         NUM_MINUTES)

    df = df.round(2)
    count = count + 1
    if count == 24:
        count = 0
    return df
