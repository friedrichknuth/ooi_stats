import requests
from requests.packages.urllib3.util.retry import Retry
import datetime
import pandas as pd
import concurrent.futures
import logging
from math import isnan
import gc
import os
import glob
import json
import pickle as pk
from copy import deepcopy
import smtplib
from email.mime.text import MIMEText
from jinja2 import Environment
import netCDF4 as nc
import numpy as np
from sys import getsizeof
import click as click
import netrc

# netrc = netrc.netrc()
# remoteHostName = "ooinet.oceanobservatories.org"
# info = netrc.authenticators(remoteHostName)
# username = info[0]
# token = info[2]


username = ''
token = ''

with open('urls.pk', 'rb') as f:
    request_urls = pk.load(f)

with open('ranges.pk', 'rb') as f:
    global_ranges = pk.load(f)





QC_PARAMETER_URL = 'https://ooinet.oceanobservatories.org/api/m2m/12578/qcparameters/'
ANNOTATIONS_URL = 'https://ooinet.oceanobservatories.org/api/m2m/12580/anno/find?'
DEPLOYEMENT_URL = 'https://ooinet.oceanobservatories.org/api/m2m/12587/events/deployment/inv/'
DATA_URL = 'https://ooinet.oceanobservatories.org/api/m2m/12576/sensor/inv/'
DATA_TEAM_PORTAL_URL = 'http://ooi.visualocean.net/data-streams/science/'

# out_dir = '/home/knuth/ooi_stats/stats/output/'
out_dir = '/Users/knuth/Documents/ooi/repos/github/ooi_stats/stats/output/'

virtual_times = ['time','met_timeflx','botsflu_time15s','botsflu_time24h']
CE_cabled = ["CE02SHBP", "CE04OSBP", "CE04OSPS"]

started_date = datetime.datetime.utcnow().strftime('%Y-%m-%d')

ntp_epoch = datetime.datetime(1900, 1, 1)
unix_epoch = datetime.datetime(1970, 1, 1)
ntp_delta = (unix_epoch - ntp_epoch).total_seconds()

pool = concurrent.futures.ThreadPoolExecutor(max_workers=20)
session = requests.session()
retry = Retry(total=10, backoff_factor=0.3,)
adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=retry, pool_block=True)
session.mount('https://', adapter)



def request_data(url,username,token):
    auth = (username, token)
    return session.get(url,auth=auth)

def to_integer(dt_time):
    return 10000*dt_time.year + 100*dt_time.month + dt_time.day

def diff_days(d1,d2):
    return (d2 - d1).days

def create_dir(new_dir):
    # Check if dir exists.. if it doesn't... create it.
    if not os.path.isdir(new_dir):
        try:
            os.makedirs(new_dir)
        except OSError:
            if os.path.exists(new_dir):
                pass
            else:
                raise

def send_gr_data_requests(array,request_urls,global_ranges,username,token):
    print('    sending data requests...')

    log_filename = array
    logging.basicConfig(filename=log_filename+'_requests.log',level=logging.DEBUG)

    # ooi_parameter_data_gr = pd.DataFrame()

    # with open('ooi_parameter_data_gr.csv', 'a') as f:

    future_to_url = {pool.submit(request_data, url, username, token): url for url in request_urls}
    for future in concurrent.futures.as_completed(future_to_url):
        try:
            data = future.result() 
            data = data.json()

            refdes_list = []
            parameter_list = []
            method_list = []
            stream_list = []
            timestamp_list = []
            value_list = []
            

            refdes = data[-1]['pk']['subsite'] + '-' + data[-1]['pk']['node'] + '-' + data[-1]['pk']['sensor']
            method = data[-1]['pk']['method']
            stream = data[-1]['pk']['stream']

            y = global_ranges[global_ranges['refdes'] == refdes]

            for i in range(len(data)):
                for ts in virtual_times:
                    try:
                        timestamp = data[i][ts]
                        timestamp = datetime.datetime.utcfromtimestamp(timestamp - ntp_delta).replace(microsecond=0)
                        timestamp = timestamp.date()
                        refdes_list.append(refdes)
                        method_list.append(method)
                        stream_list.append(stream)
                        parameter_list.append(ts)
                        value_list.append(data[i][ts])
                        timestamp_list.append(timestamp)
                    except:
                        continue
                

                else:
                    for var in y.parameter.values:
                        for j in data[i].keys():
                            if var == j:
                                z = data[i][j]
                                # conditional to handle 2d datasets, in which case the first non nan value is checked
                                if type(z) != list:
                                    refdes_list.append(refdes)
                                    method_list.append(method)
                                    stream_list.append(stream)
                                    parameter_list.append(var)
                                    value_list.append(z)
                                    timestamp_list.append(timestamp)
                                else:
                                    u = next(u for u in z if not isnan(u))
                                    refdes_list.append(refdes)
                                    method_list.append(method)
                                    stream_list.append(stream)
                                    parameter_list.append(var)
                                    value_list.append(u)
                                    timestamp_list.append(timestamp)

            # create data frame from lists collected above
            data_dict = {
                'refdes':refdes_list,
                'method':method_list,
                'stream':stream_list,
                'parameter':parameter_list,
                'value':value_list,
                'date':timestamp_list}
            response_data = pd.DataFrame(data_dict, columns = ['refdes','method','stream','parameter','value','date'])
            
            # subset to mode time stamp of response to omit data returned outside time range (day) requested
            response_data = response_data.loc[response_data['date'] == response_data['date'].mode()[0]]

            for ts in virtual_times:
                if ts in response_data['parameter'].values:
                    data_length = len(response_data[response_data['parameter'] == ts])
                else:
                    continue
            
            # merge into data frame with global range values and check if value between global ranges
            df = y.merge(response_data,indicator=True,how='outer')
            df['pass'] = (df['value'] < pd.to_numeric(df['global_range_max'])) & \
                            (df['value'] > pd.to_numeric(df['global_range_min'])) 

            # assign true to all time parameter instances
            for ts in virtual_times:
                try:
                    df.loc[df['parameter'] == ts, 'pass'] = True
                except:
                    continue
                    

            # collapse the data frame to calculate percent of data points that pass the test for that day
            df2 = df['pass'].groupby([df['refdes'], \
                                      df['method'], \
                                      df['stream'], \
                                      df['parameter'],\
                                      df['date'] \
                                     ]).sum().reset_index()


            df2['percent'] = (df2['pass'] / data_length) * 100
            df2['data_points'] = data_length
            df2 = df2[['refdes','method','stream','parameter','date','percent']]

            # append result for this ref des and day to final data frame
            df2.to_csv('ooi_parameter_data_gr.csv', mode='a', index=False, header=False) 
                
        except:
            pass

# out = data_out_dir + array + '_ooi_parameter_data_gr_'+ started_date + '.csv'
# ooi_parameter_data_gr.to_csv(out, index=False)



@click.command()
@click.option('--array', type=click.Choice(['RS','CP','GA','GI','GP','GS','CE']))
def main(array):
    send_gr_data_requests(array,request_urls,global_ranges,username,token)


if __name__ == '__main__':
    main()
