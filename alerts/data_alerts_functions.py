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

QC_PARAMETER_URL = 'https://ooinet.oceanobservatories.org/api/m2m/12578/qcparameters/'
ANNOTATIONS_URL = 'https://ooinet.oceanobservatories.org/api/m2m/12580/anno/find?'
DEPLOYEMENT_URL = 'https://ooinet.oceanobservatories.org/api/m2m/12587/events/deployment/inv/'
DATA_URL = 'https://ooinet.oceanobservatories.org/api/m2m/12576/sensor/inv/'
DATA_TEAM_PORTAL_URL = 'http://ooi.visualocean.net/data-streams/science/'

# out_dir = '/home/knuth/ooi_stats/alerts/output/'
out_dir = '/Users/knuth/Documents/ooi/repos/github/ooi_stats/alerts/output/'

virtual_times = ['time','met_timeflx','botsflu_time15s','botsflu_time24h']
CE_cabled = ["CE02SHBP", "CE04OSBP", "CE04OSPS"]

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

def get_most_recent(array):
    try:
        param_dir = out_dir + array+'/'+'param'+'/*'
        stream_dir = out_dir + array+'/'+'stream'+'/*'
        method_dir = out_dir + array+'/'+'method'+'/*'
        refdes_dir = out_dir + array+'/'+'refdes'+'/*'

        param_list_of_files = glob.glob(param_dir)
        stream_list_of_files = glob.glob(stream_dir)
        method_list_of_files = glob.glob(method_dir)
        refdes_list_of_files = glob.glob(refdes_dir)


        param_latest_file = max(param_list_of_files, key=os.path.getctime)
        stream_latest_file = max(stream_list_of_files, key=os.path.getctime)
        method_latest_file = max(method_list_of_files, key=os.path.getctime)
        refdes_latest_file = max(refdes_list_of_files, key=os.path.getctime)

        with open(param_latest_file, 'rb') as f:
            param_most_recent = pk.load(f)
        with open(stream_latest_file, 'rb') as f:
            stream_most_recent = pk.load(f)
        with open(method_latest_file, 'rb') as f:
            method_most_recent = pk.load(f)
        with open(refdes_latest_file, 'rb') as f:
            refdes_most_recent = pk.load(f)

        return param_most_recent, stream_most_recent, method_most_recent, refdes_most_recent

    except:
        param_most_recent = pd.DataFrame(columns =['refdes','method','stream','parameter'])
        stream_most_recent = pd.DataFrame(columns =['refdes','method','stream'])
        method_most_recent = pd.DataFrame(columns =['refdes','method_type'])
        refdes_most_recent = pd.DataFrame(columns =['refdes'])

        return param_most_recent, stream_most_recent, method_most_recent, refdes_most_recent







def request_gr(username, token):
    print("requesting qc data...")
    r = requests.get(QC_PARAMETER_URL, auth=(username, token))
    data = r.json()

    refdes_qc_list = []
    parameter_qc_list = []
    globalrange_min_qc_list = []

    for i in range(len(data)):
        if data[i]['qcParameterPK']['qcId'] == 'dataqc_globalrangetest_minmax' \
        and data[i]['qcParameterPK']['parameter'] == 'dat_min':
            
            refdes = data[i]['qcParameterPK']['refDes']['subsite']+'-'+\
                data[i]['qcParameterPK']['refDes']['node']+'-'+\
                data[i]['qcParameterPK']['refDes']['sensor']
            refdes_qc_list.append(refdes)
            
            parameter = data[i]['qcParameterPK']['streamParameter']
            parameter_qc_list.append(parameter)
            
            globalrange_min = data[i]['value']
            globalrange_min_qc_list.append(globalrange_min)

    qc_dict = {
        'refdes':refdes_qc_list,
        'parameter':parameter_qc_list,
        'global_range_min':globalrange_min_qc_list,
    }     
            
    globalrange_min_qc_data = pd.DataFrame(qc_dict,columns=['refdes','parameter','global_range_min'])

    refdes_qc_list = []
    parameter_qc_list = []
    globalrange_max_qc_list = []

    for i in range(len(data)):
        if data[i]['qcParameterPK']['qcId'] == 'dataqc_globalrangetest_minmax' \
        and data[i]['qcParameterPK']['parameter'] == 'dat_max':
            
            refdes = data[i]['qcParameterPK']['refDes']['subsite']+'-'+\
                data[i]['qcParameterPK']['refDes']['node']+'-'+\
                data[i]['qcParameterPK']['refDes']['sensor']
            refdes_qc_list.append(refdes)
            
            parameter = data[i]['qcParameterPK']['streamParameter']
            parameter_qc_list.append(parameter)
            
            globalrange_max = data[i]['value']
            globalrange_max_qc_list.append(globalrange_max)

    qc_dict = {
        'refdes':refdes_qc_list,
        'parameter':parameter_qc_list,
        'global_range_max':globalrange_max_qc_list,
    }     
            
    globalrange_max_qc_data = pd.DataFrame(qc_dict,columns=['refdes','parameter','global_range_max'])

    global_ranges = pd.merge(globalrange_min_qc_data,globalrange_max_qc_data, on=['refdes','parameter'], how='outer')

    return global_ranges






def check_sci_v_gr(array, global_ranges,request_inputs):
    print("    checking science classifications vs what has global range qc values...")
    ranges = global_ranges[['refdes','parameter']].drop_duplicates()
    
    if array == 'CE':
        ranges = ranges[ranges.refdes.str.contains('|'.join(CE_cabled))==False]
        ranges = ranges[ranges.refdes.str.startswith(array)]

    if array == 'RS':
        ranges_temp = ranges[ranges.refdes.str.contains('|'.join(CE_cabled))]
        ranges_temp2 = ranges[ranges.refdes.str.startswith(array)]
        ranges = pd.concat([ranges_temp,ranges_temp2])

    expected = request_inputs[['refdes','parameter']].drop_duplicates()
    not_found = ranges.merge(expected,indicator=True, how='outer')
    missing_gr_qc_values = not_found[not_found['_merge'] == 'right_only']
    del missing_gr_qc_values['_merge']
    missing_science_classification = not_found[not_found['_merge'] == 'left_only']
    del missing_science_classification['_merge']
    
    return missing_gr_qc_values , missing_science_classification





def request_annotations(array, username, token):
    beginDT  = int(nc.date2num(datetime.datetime.strptime("2012-01-01T01:00:01Z",'%Y-%m-%dT%H:%M:%SZ'),'seconds since 1970-01-01')*1000)
    endDT = int(nc.date2num(datetime.datetime.utcnow(),'seconds since 1970-01-01')*1000) 

    refdes_in = DATA_TEAM_PORTAL_URL + array
    refdes_list = pd.read_csv(refdes_in)
    refdes_list = refdes_list[['reference_designator','method', 'stream_name','parameter_name']]
    refdes_list.columns = ['refdes','method', 'stream','parameter']
    refdes_list = refdes_list['refdes']

    # added regex search to exclude or grab cabled cabled assets to produce complete Endurance and Cabled Array outputs
    if array == 'CE':
        refdes_list = refdes_list[refdes_list.str.contains('|'.join(CE_cabled))==False]

    if array == 'RS':
        refdes_in_CE = DATA_TEAM_PORTAL_URL + 'CE'
        refdes_list_CE = pd.read_csv(refdes_in_CE)
        refdes_list_CE = refdes_list_CE[['reference_designator','method', 'stream_name','parameter_name']]
        refdes_list_CE.columns = ['refdes','method', 'stream','parameter']
        refdes_list_CE = refdes_list_CE['refdes']
        refdes_list_CE = refdes_list_CE[refdes_list_CE.str.contains('|'.join(CE_cabled))]
        refdes_list = pd.concat([refdes_list_CE,refdes_list])

    refdes_list = refdes_list.drop_duplicates()

    print("    building annotation info requests...")
    anno_requests = []
    for i in refdes_list:
        request_url = ANNOTATIONS_URL+'beginDT='+str(beginDT)+'&endDT='+str(endDT)+'&refdes='+i
        anno_requests.append(request_url)
        
    print("    sending annotation info requests...")
    ref_des_list = []
    future_to_url = {pool.submit(request_data, url, username, token): url for url in anno_requests}
    for future in concurrent.futures.as_completed(future_to_url):
        url_rf = future_to_url[future]
        try:
            anno_info = future.result()
            anno_info = anno_info.json()
            
            for i in range(len(anno_info)):
                if anno_info[i]['endDT'] is None and anno_info[i]['qcFlag'] == 'not_operational':
                    refdes =  url_rf[111:]
                    ref_des_list.append(refdes)
        except:
            pass

                    
    data_dict={
        'refdes':ref_des_list}

    not_operational = pd.DataFrame(data_dict, columns = ['refdes'])

    return not_operational






def alert_request_deployments(array, username, token):
    refdes_in = DATA_TEAM_PORTAL_URL + array
    refdes_list = pd.read_csv(refdes_in)
    refdes_list = refdes_list[['reference_designator','method', 'stream_name','parameter_name']]
    refdes_list.columns = ['refdes','method', 'stream','parameter']
    refdes_list = refdes_list['refdes']

    # added regex search to exclude or grab cabled cabled assets to produce complete Endurance and Cabled Array outputs
    if array == 'CE':
        refdes_list = refdes_list[refdes_list.str.contains('|'.join(CE_cabled))==False]

    if array == 'RS':
        refdes_in_CE = DATA_TEAM_PORTAL_URL + 'CE'
        refdes_list_CE = pd.read_csv(refdes_in_CE)
        refdes_list_CE = refdes_list_CE[['reference_designator','method', 'stream_name','parameter_name']]
        refdes_list_CE.columns = ['refdes','method', 'stream','parameter']
        refdes_list_CE = refdes_list_CE['refdes']
        refdes_list_CE = refdes_list_CE[refdes_list_CE.str.contains('|'.join(CE_cabled))]
        refdes_list = pd.concat([refdes_list_CE,refdes_list])

    refdes_list = refdes_list.drop_duplicates()

    print("working on", array)
    print("    building deployment info requests...")
    asset_requests = []
    for i in refdes_list:
        sub_site = i[:8]
        platform = i[9:14]
        instrument = i[15:27]
        asset_url_inputs = '/'.join((sub_site, platform, instrument))
        request_url = DEPLOYEMENT_URL+asset_url_inputs+'/-1'
        asset_requests.append(request_url)

    print("    sending deployment info requests...")
    ref_des_list = []
    start_time_list = []
    deployment_list = []

    start_time = datetime.datetime.utcnow() - datetime.timedelta(seconds=86400)

    future_to_url = {pool.submit(request_data, url, username, token): url for url in asset_requests}
    for future in concurrent.futures.as_completed(future_to_url):
        try:
            asset_info = future.result()
            asset_info = asset_info.json()

            for i in range(len(asset_info)):
                if asset_info[i]['eventStopTime'] is None:
                    refdes = asset_info[i]['referenceDesignator']
                    ref_des_list.append(refdes)
                    
                    deployment = asset_info[i]['deploymentNumber']
                    deployment_list.append(deployment)
                    start_time_list.append(start_time)
        except:
            pass
     
    data_dict={
        'refdes':ref_des_list,
        'deployment':deployment_list,
        'start_time':start_time_list}

    alert_deployment_data = pd.DataFrame(data_dict, columns = ['refdes', 'deployment','start_time'])

    return alert_deployment_data







def alert_build_requests(array, alert_deployment_data):
    print("    building data request urls...")

    refdes_streams = DATA_TEAM_PORTAL_URL + array
    refdes_streams_df = pd.read_csv(refdes_streams)
    refdes_streams_df = refdes_streams_df[['reference_designator','method', 'stream_name','parameter_name']]
    refdes_streams_df.columns = ['refdes','method', 'stream','parameter']
    refdes_streams_df = refdes_streams_df[refdes_streams_df['method'].str.contains("recovered")==False]

    # regex search to exclude or grab cabled cabled assets to produce complete Endurance and Cabled Array outputs
    if array == 'CE':
        refdes_streams_df = refdes_streams_df[refdes_streams_df['refdes'].str.contains('|'.join(CE_cabled))==False]

    if array == 'RS':
        refdes_streams_CE = DATA_TEAM_PORTAL_URL + 'CE'
        refdes_streams_df_CE = pd.read_csv(refdes_streams_CE)
        refdes_streams_df_CE = refdes_streams_df_CE[['reference_designator','method', 'stream_name','parameter_name']]
        refdes_streams_df_CE.columns = ['refdes','method', 'stream','parameter']
        refdes_streams_df_CE = refdes_streams_df_CE[refdes_streams_df_CE['method'].str.contains("recovered")==False]
        refdes_streams_df_CE = refdes_streams_df_CE[refdes_streams_df_CE['refdes'].str.contains('|'.join(CE_cabled))]
        refdes_streams_df = pd.concat([refdes_streams_df_CE,refdes_streams_df])

    refdes_streams_df = refdes_streams_df.drop_duplicates()

    request_inputs = pd.merge(refdes_streams_df,alert_deployment_data, on='refdes')

    request_inputs['subsite'] = request_inputs.refdes.str[:8]
    request_inputs['platform'] = request_inputs.refdes.str[9:14]
    request_inputs['instrument'] = request_inputs.refdes.str[15:27]
    request_inputs['date'] = pd.to_datetime(request_inputs['start_time'])
    request_inputs['date'] = request_inputs.date.dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')


    request_inputs['urls'] = DATA_URL+\
                            request_inputs.subsite+\
                            '/'+request_inputs.platform+\
                            '/'+request_inputs.instrument+\
                            '/'+request_inputs.method+\
                            '/'+request_inputs.stream+\
                            '?beginDT='+request_inputs.date+\
                            '&limit=1000'

    request_urls = request_inputs['urls'].drop_duplicates()
    request_urls = request_urls.values.tolist()

    return request_urls , request_inputs









def send_gr_data_requests(array,request_urls,global_ranges,username,token):

    print('    sending data requests...')
    

    ooi_parameter_data_gr = pd.DataFrame()
    missing = []

    future_to_url = {pool.submit(request_data, url, username, token): url for url in request_urls}
    for future in concurrent.futures.as_completed(future_to_url):
        # url = future_to_url[future]
        try:
            data = future.result() 
            data = data.json()

            refdes_list = []
            parameter_list = []
            method_list = []
            stream_list = []
            timestamp_list = []
            value_list = []
            
            # use this to speed up the loop
    #         df = pd.DataFrame.from_records(map(json.loads, map(json.dumps,data)))

            refdes = data[-1]['pk']['subsite'] + '-' + data[-1]['pk']['node'] + '-' + data[-1]['pk']['sensor']
            method = data[-1]['pk']['method']
            stream = data[-1]['pk']['stream']

            y = global_ranges[global_ranges['refdes'] == refdes]

            for i in range(len(data)):
                for ts in virtual_times:
                    # print('yes')
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
                
                if y.empty:
                    missing.append(refdes)
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
    #         df2 = df2[['refdes','method','stream','parameter','date','data_points','percent']]

            # append result for this ref des and day to final data frame
            ooi_parameter_data_gr = ooi_parameter_data_gr.append(df2)
            # ooi_parameter_data_gr = ooi_parameter_data_gr.drop_duplicates()
            # print(getsizeof(ooi_parameter_data_gr))
            gc.collect()
                
        except:
            # print('no data for ', url)
            pass



    # subset resonse to only return instances where more than 50% of the data points returned pass the global range test. 
    # time always passes the test, so stream availability is not lost.
    try:
        ooi_parameter_data_gr['value'] = np.where(ooi_parameter_data_gr['percent'] > 50, 1, 0)
        ooi_parameter_data_gr = ooi_parameter_data_gr[ooi_parameter_data_gr['value'] == 1]
        ooi_parameter_data_gr = ooi_parameter_data_gr[['refdes','method','stream','parameter','date','value']]
        ooi_parameter_data_gr = ooi_parameter_data_gr.drop_duplicates()
    except:
        ooi_parameter_data_gr = pd.DataFrame(columns=['refdes','method','stream','parameter','date','value'])
        pass
    gc.collect()

    # print(getsizeof(ooi_parameter_data_gr))

    return ooi_parameter_data_gr








def alert_create_all_outputs(array,ooi_parameter_data_gr,request_inputs):
    # parameter level output
    param_inputs = request_inputs[['refdes','method','stream', 'parameter']].copy()
    param_inputs = param_inputs.drop_duplicates()
    param_result = ooi_parameter_data_gr[['refdes','method','stream','parameter']].copy()
    param_result = param_result.drop_duplicates()
    failed_gr_qc = param_result.merge(param_inputs,indicator=True,how='outer')
    failed_gr_qc = failed_gr_qc[failed_gr_qc['_merge'] == 'right_only']
    del failed_gr_qc['_merge']
    failed_gr_qc['value'] = 0
    param_result['value'] = 1
    param_final = pd.concat([param_result, failed_gr_qc])

    # stream level rollup
    stream_inputs = request_inputs[['refdes','method','stream']].copy()
    stream_inputs = stream_inputs.drop_duplicates()
    stream_result = ooi_parameter_data_gr[['refdes','method','stream']].copy()
    stream_result = stream_result.drop_duplicates()
    missing_streams = stream_result.merge(stream_inputs,indicator=True,how='outer')
    missing_streams = missing_streams[missing_streams['_merge'] == 'right_only']
    del missing_streams['_merge']
    missing_streams['value'] = 0
    stream_result['value'] = 1
    stream_final = pd.concat([stream_result,missing_streams])


    #method level output
    method_inputs = request_inputs[['refdes','method']].copy()
    method_inputs = method_inputs.drop_duplicates()
    x = list(method_inputs.method.values)
    y = []
    for i in x:
        if 'recovered' in i:
            y.append('recovered')
        elif 'telemetered' in i:
            y.append('telemetered')
        elif 'streamed' in i:
            y.append('streamed')
    method_inputs['method_type'] = y
    del method_inputs['method']
    method_inputs = method_inputs.drop_duplicates()
    method_result = ooi_parameter_data_gr[['refdes','method']].copy()
    method_result = method_result.drop_duplicates()
    x = list(method_result.method.values)
    y = []
    for i in x:
        if 'recovered' in i:
            y.append('recovered')
        elif 'telemetered' in i:
            y.append('telemetered')
        elif 'streamed' in i:
            y.append('streamed')
    method_result['method_type'] = y
    del method_result['method']
    method_result = method_result.drop_duplicates()
    missing_methods = method_result.merge(method_inputs,indicator=True,how='outer')
    missing_methods = missing_methods[missing_methods['_merge'] == 'right_only']
    del missing_methods['_merge']
    missing_methods['value'] = 0
    method_result['value'] = 1
    method_final = pd.concat([method_result, missing_methods])

    # refdes level rollup
    refdes_inputs = request_inputs[['refdes']].copy()
    refdes_inputs = refdes_inputs.drop_duplicates()
    refdes_result = ooi_parameter_data_gr[['refdes']].copy()
    refdes_result = refdes_result.drop_duplicates()
    missing_refdes = refdes_result.merge(refdes_inputs,indicator=True,how='outer')
    missing_refdes = missing_refdes[missing_refdes['_merge'] == 'right_only']
    del missing_refdes['_merge']
    missing_refdes['value'] = 0
    refdes_result['value'] = 1
    refdes_final = pd.concat([refdes_result, missing_refdes])

    return param_final, stream_final, method_final, refdes_final









def alert_create_missing_output(array, param_final, stream_final, method_final, refdes_final, missing_gr_qc_values):
    print('    writing output...')

    param_final_out_temp = param_final[param_final['value'] == 0]
    del param_final_out_temp['value']
    stream_final_out = stream_final[stream_final['value'] == 0]
    del stream_final_out['value']
    method_final_out = method_final[method_final['value'] == 0]
    del method_final_out['value']
    refdes_final_out = refdes_final[refdes_final['value'] == 0]
    del refdes_final_out['value']

    # only capture parameters not contained in streams that are missing entirely
    param_final_out_temp2 = param_final_out_temp.merge(stream_final_out,indicator=True, how='outer')
    param_final_out_temp2 = param_final_out_temp2[param_final_out_temp2['_merge'] == 'left_only']
    del param_final_out_temp2['_merge']

    # only capture parameters that are actually failing the global range test, not missing because no values have been entered.
    param_final_out = missing_gr_qc_values.merge(param_final_out_temp2,indicator=True, how='outer')
    param_final_out = param_final_out[param_final_out['_merge'] == 'right_only']
    del param_final_out['_merge']
    param_final_out = param_final_out[['refdes','method','stream','parameter']]

    param_dir = out_dir + array+'/'+'param'+'/'
    create_dir(param_dir)
    stream_dir = out_dir + array+'/'+'stream'+'/'
    create_dir(stream_dir)
    method_dir = out_dir + array+'/'+'method'+'/'
    create_dir(method_dir)
    refdes_dir = out_dir + array+'/'+'refdes'+'/'
    create_dir(refdes_dir)

    out = param_dir + array + '_param_data_'+ datetime.datetime.utcnow().strftime('%Y-%m-%d') + '.pd'
    with open(out, 'wb') as fh:
            pk.dump(param_final_out,fh)

    out = stream_dir + array + '_stream_data_'+ datetime.datetime.utcnow().strftime('%Y-%m-%d') + '.pd'
    with open(out, 'wb') as fh:
            pk.dump(stream_final_out,fh)

    out = method_dir + array + '_method_data_'+ datetime.datetime.utcnow().strftime('%Y-%m-%d') + '.pd'
    with open(out, 'wb') as fh:
            pk.dump(method_final_out,fh)

    out = refdes_dir + array + '_refdes_data_'+ datetime.datetime.utcnow().strftime('%Y-%m-%d') + '.pd'
    with open(out, 'wb') as fh:
            pk.dump(refdes_final_out,fh)

    return param_final_out, stream_final_out, method_final_out, refdes_final_out








def compare_operational(not_operational, stream_final_out, request_inputs):
    difference = stream_final_out.merge(not_operational,indicator=True, how='outer')
    annotated_and_not_operational = difference[difference['_merge'] == 'both']
    no_data_not_annotated = difference[difference['_merge'] == 'left_only']
    data_but_annotated = difference[difference['_merge'] == 'right_only']
    del annotated_and_not_operational['_merge']
    del no_data_not_annotated['_merge']
    data_but_annotated = data_but_annotated[['refdes']]

    request_inputs = request_inputs[['refdes']]
    request_inputs = request_inputs.drop_duplicates()
    data_but_annotated = request_inputs.merge(data_but_annotated,indicator=True, how='outer')
    data_but_annotated = data_but_annotated[data_but_annotated['_merge'] == 'both']
    del data_but_annotated['_merge']
    
    return no_data_not_annotated, annotated_and_not_operational, data_but_annotated







def stream_compare_output(array, stream_final_out, stream_most_recent, request_inputs):  
    try:
        print('    comparing stream output to most recent...')
        difference = stream_most_recent.merge(stream_final_out,indicator=True, how='outer')
        stream_difference_new = difference[difference['_merge'] == 'right_only']
        stream_difference_resumed = difference[difference['_merge'] == 'left_only']
        del stream_difference_new['_merge']
        del stream_difference_resumed['_merge']

        # check that the resumed stream is still expected
        request_inputs = request_inputs[['refdes','method','stream']]
        request_inputs = request_inputs.drop_duplicates()
        stream_difference_resumed = request_inputs.merge(stream_difference_resumed,indicator=True, how='outer')
        stream_difference_resumed = stream_difference_resumed[stream_difference_resumed['_merge'] == 'both']
        del stream_difference_resumed['_merge']
        return stream_difference_new , stream_difference_resumed

    except:
        print('    nothing to compare to...')
        stream_difference_resumed = pd.DataFrame()
        stream_difference_new = pd.DataFrame()
        return stream_difference_new , stream_difference_resumed




def parameter_compare_output(array, param_final_out, param_most_recent, request_inputs):  
    try:
        print('    comparing param output to most recent...')
        difference = param_most_recent.merge(param_final_out,indicator=True, how='outer')
        param_difference_new = difference[difference['_merge'] == 'right_only']
        param_difference_resumed = difference[difference['_merge'] == 'left_only']
        del param_difference_new['_merge']
        del param_difference_resumed['_merge']


        # check that the resumed parameter is still expected
        request_inputs = request_inputs[['refdes','method','stream','parameter']]
        request_inputs = request_inputs.drop_duplicates()
        param_difference_resumed = request_inputs.merge(param_difference_resumed,indicator=True, how='outer')
        param_difference_resumed = param_difference_resumed[param_difference_resumed['_merge'] == 'both']
        del param_difference_resumed['_merge']

        return param_difference_new , param_difference_resumed


    except:
        print('    nothing to compare to...')
        param_difference_resumed = pd.DataFrame()
        param_difference_new = pd.DataFrame()
        return param_difference_new , param_difference_resumed






def sendEmail(msg,recipients):
    print('    sending alert...')
    recipients = recipients
    server = smtplib.SMTP('smtp.gmail.com:587')
    server.ehlo()
    server.starttls()
    server.login("ooidatateam@gmail.com", "")
    server.sendmail("ooidatateam@gmail.com",recipients,msg.as_string())

def print_html_doc(html_template,text):
    return Environment().from_string(html_template).render(body=text)

def alert_send(array,no_data_not_annotated,annotated_and_not_operational,data_but_annotated,stream_difference_new,stream_difference_resumed,param_final_out, param_difference_new,param_difference_resumed,missing_gr_qc_values,missing_science_classification,RS_recipients,CE_recipients,GA_recipients,CP_recipients):
    html_template = """
    <html>
    <body>{{ body }}</body>
    </html>
    """

    if stream_difference_new.empty and data_but_annotated.empty and stream_difference_resumed.empty and param_difference_new.empty and param_difference_resumed.empty:
        text = '<h2>Ongoing Issues</h2>'
        pass
    else:
        text = '<h2><font color="red">New Alert</font></h2>'
        # text = text + '<br><small>*Alerts are only sent once when there is a new alert. If and instrument resumes producing data a new alert will be sent.</small><br><br>'

    if stream_difference_new.empty: 
        pass
    else:
        text = text + '<b>Instruments and streams that <font color="red">have not produced data</font> in the past 24 hours:</b><br><br>'
        f = stream_difference_new.to_html()
        text = text + str(f) + '<br><br>'

    if data_but_annotated.empty:
        pass
    else:
        text = text + '<b>Instruments that are <font color="red">annotated as not_operational, but have resumed producing data:</font></b><br><br>'
        f = data_but_annotated.to_html()
        text = text + str(f) + '<br><br>'

    if param_difference_new.empty:
        pass
    else:
        text = text + '<b>Parameters <font color="red">not producing data within global ranges</font> in the past 24 hours:</b><br><br>'
        f = param_difference_new.to_html()
        text = text + str(f) + '<br><br>'

    if stream_difference_resumed.empty:
        pass
    else:
        text = text + '<b>Instruments and streams that <font color="green">resumed producing data</font> in the past 24 hours:</b><br><br>'
        f = stream_difference_resumed.to_html()
        text = text + str(f) + '<br><br>'

    if param_difference_resumed.empty:
        pass
    else:
        text = text + '<b>Parameters that <font color="green">resumed producing data within global ranges</font> in the past 24 hours:</b><br><br>'
        f = param_difference_resumed.to_html()
        text = text + str(f) + '<br><br>'





    if stream_difference_new.empty and data_but_annotated.empty and stream_difference_resumed.empty and param_difference_new.empty and param_difference_resumed.empty:
        pass
    else: 
        text = text + '<h2>Summary of New and Ongoing Issues</h2>'
    # text = text + '<HR COLOR="lightblue" WIDTH="100%" SIZE=10>' + '<br><br>'

    if no_data_not_annotated.empty:
        pass
    else:
        text = text + 'Instruments and streams <b>not producing data</b> for over 24 hours:<br><br>'
        f = no_data_not_annotated.to_html()
        text = text + str(f) + '<br><br>'

    if annotated_and_not_operational.empty:
        pass
    else:
        text = text + 'Instruments and streams <b>not producing data and annotated as not_operational</b>:<br><br>'
        f = annotated_and_not_operational.to_html()
        text = text + str(f) + '<br><br>'

    if param_final_out.empty:
        pass
    else:
        text = text + 'Parameters <b>not producing data within global ranges</b> for over 24 hours:<br><br>'
        f = param_final_out.to_html()
        text = text + str(f) + '<br><br>'     


    # if missing_gr_qc_values.empty:
    #     pass
    # else:
    #     text = text + 'Parameter instances <b>missing global range qc values:</b><br><br>'
    #     text = text + '<small>Values need to be entered here: https://github.com/ooi-integration/qc-lookup/blob/master/data_qc_global_range_values.csv</small><br><br>'
    #     f = missing_gr_qc_values.to_html()
    #     text = text + str(f) + '<br><br>'

    # if missing_science_classification.empty:
    #     pass
    # else:
    #     text = text + 'Parameter instances with QC values but <b>not classified as science parameters:</b> but that have global range qc values<br><br>'
    #     text = text + '<small>Classification needs to be made in preload as "Science Data" under column dataproducttype: https://github.com/oceanobservatories/preload-database/blob/master/csv/ParameterDefs.csv</small><br><br>'
    #     text = text + '<small>If the parameter does not exist in preload, then the parameter needs to be removed from the QC tables https://github.com/ooi-integration/qc-lookup/blob/master/data_qc_global_range_values.csv</small><br><br>'
    #     f = missing_science_classification.to_html()
    #     text = text + str(f) + '<br><br>'



    subject = 'Data Quality and Availability Alert for ' + array + ' on ' + datetime.datetime.utcnow().strftime('%Y-%m-%d')
    text = print_html_doc(html_template,text)

    msg = MIMEText(text,'html')
    msg['Subject'] = subject

    if array == 'RS':
        recipients = RS_recipients
        sendEmail(msg,recipients)
    elif array == 'CE':
        recipients = CE_recipients
        sendEmail(msg,recipients)
    elif array == 'GA':
        recipients = GA_recipients
        sendEmail(msg,recipients)
    elif array == 'GI':
        recipients = GA_recipients
        sendEmail(msg,recipients)
    elif array == 'GP':
        recipients = GA_recipients
        sendEmail(msg,recipients)
    elif array == 'GS':
        recipients = GA_recipients
        sendEmail(msg,recipients)
    elif array == 'CP':
        recipients = CP_recipients
        sendEmail(msg,recipients)
