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







def request_stats_deployments(array, username, token):
    refdes_in = DATA_TEAM_PORTAL_URL + array
    refdes_list = pd.read_csv(refdes_in)
    refdes_list = refdes_list[['reference_designator','method', 'stream_name','parameter_name']]
    refdes_list.columns = ['refdes','method', 'stream','parameter']
    refdes_list = refdes_list['refdes']

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
    end_time_list = []
    deployment_list = []

    future_to_url = {pool.submit(request_data, url, username, token): url for url in asset_requests}
    for future in concurrent.futures.as_completed(future_to_url):
        try:
            asset_info = future.result()
            asset_info = asset_info.json()
            
            for i in range(len(asset_info)):
                refdes = asset_info[i]['referenceDesignator']
                ref_des_list.append(refdes)
                
                deployment = asset_info[i]['deploymentNumber']
                deployment_list.append(deployment)
                
                start = asset_info[i]['eventStartTime']
                end = asset_info[i]['eventStopTime']
                
                try:
                    start_time = datetime.datetime.utcfromtimestamp(start/1000.0)
                    start_time_list.append(start_time)
                    
                    end_time = datetime.datetime.utcfromtimestamp(end/1000.0)
                    end_time_list.append(end_time)
                    
                except:
                    end_time = datetime.datetime.utcnow()
                    end_time_list.append(end_time)
                    
        except:
            pass
     
    data_dict = {
        'refdes':ref_des_list,
        'deployment':deployment_list,
        'start_time':start_time_list,
        'end_time':end_time_list}
    stats_deployment_data = pd.DataFrame(data_dict, columns = ['refdes', 'deployment','start_time', 'end_time'])

    return stats_deployment_data







def build_stats_requests(array, stats_deployment_data):
    print("    calculating days between deployment dates...")
    deployment_data_days = pd.DataFrame(columns = ['refdes', 'deployment','date'])

    # calculate days between deployment dates
    for index, row in stats_deployment_data.iterrows():
        start_time = row['start_time']
        end_time = row['end_time']
        periods = diff_days(start_time, end_time)
        start_time = to_integer(start_time)
        total_days = pd.DataFrame({'date' : pd.date_range(str(start_time),periods=periods,freq='D')})
        
        total_days['refdes'] = row['refdes']
        total_days['deployment'] = row['deployment']
        deployment_data_days = deployment_data_days.append(total_days)

    # re-order data frame columns
    deployment_data_days = deployment_data_days[['refdes', 'deployment','date']]

    print("    building data request urls...")
    deployment_data_days['start_date'] = deployment_data_days['date'] + datetime.timedelta(seconds=5)
    deployment_data_days['end_date'] = deployment_data_days['date'] + datetime.timedelta(seconds=86395)

    # refdes_streams = DATA_TEAM_PORTAL_URL + array
    refdes_streams = DATA_TEAM_PORTAL_URL + array
    refdes_streams_df = pd.read_csv(refdes_streams)
    refdes_streams_df = refdes_streams_df[['reference_designator','method', 'stream_name','parameter_name']]
    refdes_streams_df.columns = ['refdes','method', 'stream','parameter']
    refdes_streams_df = refdes_streams_df.drop_duplicates()

    request_inputs = pd.merge(refdes_streams_df,deployment_data_days, on='refdes')

    request_inputs['subsite'] = request_inputs.refdes.str[:8]
    request_inputs['platform'] = request_inputs.refdes.str[9:14]
    request_inputs['instrument'] = request_inputs.refdes.str[15:27]
    request_inputs['start_date'] = pd.to_datetime(request_inputs['start_date'])
    request_inputs['start_date'] = request_inputs.start_date.dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    request_inputs['end_date'] = pd.to_datetime(request_inputs['end_date'])
    request_inputs['end_date'] = request_inputs.end_date.dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')

    request_inputs['urls'] = DATA_URL+\
                            request_inputs.subsite+\
                            '/'+request_inputs.platform+\
                            '/'+request_inputs.instrument+\
                            '/'+request_inputs.method+\
                            '/'+request_inputs.stream+\
                            '?beginDT='+request_inputs.start_date+\
                            '&endDT='+request_inputs.end_date+\
                            '&limit=50'

    request_urls = request_inputs['urls'].drop_duplicates()
    request_urls = request_urls.values.tolist()

    # request_urls = request_urls[-100:]

    return request_urls , request_inputs








def check_sci_v_gr(array, global_ranges,request_inputs):
    print("    checking science classifications vs what has global range qc values...")
    ranges = global_ranges[['refdes','parameter']].drop_duplicates()
    
    if array == 'CE':
        ranges = ranges[ranges.refdes.str.contains('|'.join(CE_cabled))==False]
        ranges = ranges[ranges.refdes.str.startswith(array)]

    elif array == 'RS':
        ranges_temp = ranges[ranges.refdes.str.contains('|'.join(CE_cabled))]
        ranges_temp2 = ranges[ranges.refdes.str.startswith(array)]
        ranges = pd.concat([ranges_temp,ranges_temp2])

    else:
        ranges = ranges[ranges.refdes.str.startswith(array)]

    expected = request_inputs[['refdes','parameter']].drop_duplicates()
    not_found = ranges.merge(expected,indicator=True, how='outer')
    missing_gr_qc_values = not_found[not_found['_merge'] == 'right_only']
    del missing_gr_qc_values['_merge']
    missing_science_classification = not_found[not_found['_merge'] == 'left_only']
    del missing_science_classification['_merge']
    
    return missing_gr_qc_values , missing_science_classification








# def send_gr_data_requests(array,request_urls,global_ranges,username,token):

#     print('    sending data requests...')

#     data_out_dir = out_dir + array+'/'+'data'+'/'
#     create_dir(data_out_dir)
    
#     log_filename = array
#     logging.basicConfig(filename=log_filename+'_requests.log',level=logging.DEBUG)

#     ooi_parameter_data_gr = pd.DataFrame()
#     missing = []

#     future_to_url = {pool.submit(request_data, url, username, token): url for url in request_urls}
#     for future in concurrent.futures.as_completed(future_to_url):
#         # url = future_to_url[future]
#         try:
#             data = future.result() 
#             data = data.json()

#             refdes_list = []
#             parameter_list = []
#             method_list = []
#             stream_list = []
#             timestamp_list = []
#             value_list = []
            
#             # use this to speed up the loop
#     #         df = pd.DataFrame.from_records(map(json.loads, map(json.dumps,data)))

#             refdes = data[-1]['pk']['subsite'] + '-' + data[-1]['pk']['node'] + '-' + data[-1]['pk']['sensor']
#             method = data[-1]['pk']['method']
#             stream = data[-1]['pk']['stream']

#             y = global_ranges[global_ranges['refdes'] == refdes]

#             for i in range(len(data)):
#                 for ts in virtual_times:
#                     # print('yes')
#                     try:
#                         timestamp = data[i][ts]
#                         timestamp = datetime.datetime.utcfromtimestamp(timestamp - ntp_delta).replace(microsecond=0)
#                         timestamp = timestamp.date()
#                         refdes_list.append(refdes)
#                         method_list.append(method)
#                         stream_list.append(stream)
#                         parameter_list.append(ts)
#                         value_list.append(data[i][ts])
#                         timestamp_list.append(timestamp)
#                     except:
#                         continue
                
#                 if y.empty:
#                     missing.append(refdes)
#                     continue

#                 else:
#                     for var in y.parameter.values:
#                         for j in data[i].keys():
#                             if var == j:
#                                 z = data[i][j]
#                                 # conditional to handle 2d datasets, in which case the first non nan value is checked
#                                 if type(z) != list:
#                                     refdes_list.append(refdes)
#                                     method_list.append(method)
#                                     stream_list.append(stream)
#                                     parameter_list.append(var)
#                                     value_list.append(z)
#                                     timestamp_list.append(timestamp)
#                                 else:
#                                     u = next(u for u in z if not isnan(u))
#                                     refdes_list.append(refdes)
#                                     method_list.append(method)
#                                     stream_list.append(stream)
#                                     parameter_list.append(var)
#                                     value_list.append(u)
#                                     timestamp_list.append(timestamp)

#             # create data frame from lists collected above
#             data_dict = {
#                 'refdes':refdes_list,
#                 'method':method_list,
#                 'stream':stream_list,
#                 'parameter':parameter_list,
#                 'value':value_list,
#                 'date':timestamp_list}
#             response_data = pd.DataFrame(data_dict, columns = ['refdes','method','stream','parameter','value','date'])
            
#             # subset to mode time stamp of response to omit data returned outside time range (day) requested
#             response_data = response_data.loc[response_data['date'] == response_data['date'].mode()[0]]

#             for ts in virtual_times:
#                 if ts in response_data['parameter'].values:
#                     data_length = len(response_data[response_data['parameter'] == ts])
#                 else:
#                     continue
            
#             # merge into data frame with global range values and check if value between global ranges
#             df = y.merge(response_data,indicator=True,how='outer')
#             df['pass'] = (df['value'] < pd.to_numeric(df['global_range_max'])) & \
#                             (df['value'] > pd.to_numeric(df['global_range_min'])) 

#             # assign true to all time parameter instances
#             for ts in virtual_times:
#                 try:
#                     df.loc[df['parameter'] == ts, 'pass'] = True
#                 except:
#                     continue
                    

#             # collapse the data frame to calculate percent of data points that pass the test for that day
#             df2 = df['pass'].groupby([df['refdes'], \
#                                       df['method'], \
#                                       df['stream'], \
#                                       df['parameter'],\
#                                       df['date'] \
#                                      ]).sum().reset_index()


#             df2['percent'] = (df2['pass'] / data_length) * 100
#             df2['data_points'] = data_length
#     #         df2 = df2[['refdes','method','stream','parameter','date','data_points','percent']]

#             # append result for this ref des and day to final data frame
#             ooi_parameter_data_gr = ooi_parameter_data_gr.append(df2)
#             # ooi_parameter_data_gr = ooi_parameter_data_gr.drop_duplicates()
#             # print(getsizeof(ooi_parameter_data_gr))
            
                
#         except:
#             # print('no data for ', url)
#             pass

#         # gc.collect()

        
#     # print('yo')
#     # print(getsizeof(ooi_parameter_data_gr))

#     out = data_out_dir + array + '_ooi_parameter_data_gr_'+ started_date + '.csv'
#     ooi_parameter_data_gr.to_csv(out, index=False)


#     # with open(out, 'wb') as fh:
#     #         pk.dump(ooi_parameter_data_gr,fh)

#     # gc.collect()

#     return out










def stats_create_all_outputs(array,request_inputs, out_dir):

    param_dir  = out_dir + 'output/' + array + '/param/'
    stream_dir = out_dir + 'output/' + array + '/stream/'
    method_dir = out_dir + 'output/' + array + '/method/'
    refdes_dir = out_dir + 'output/' + array + '/refdes/'

    create_dir(param_dir)
    create_dir(stream_dir)
    create_dir(method_dir)
    create_dir(refdes_dir)

    colnames=['refdes','method','stream','parameter','date','percent']


    ooi_parameter_data_gr = pd.read_csv('ooi_parameter_data_gr.csv', names=colnames, header=None)

    ooi_parameter_data_gr['value'] = np.where(ooi_parameter_data_gr['percent'] > 50, 1, 0)
    ooi_parameter_data_gr = ooi_parameter_data_gr[ooi_parameter_data_gr['value'] == 1]
    ooi_parameter_data_gr = ooi_parameter_data_gr[['refdes','method','stream','parameter','date','value']]
    ooi_parameter_data_gr = ooi_parameter_data_gr.drop_duplicates()

    print('    creating outputs param...')
    # parameter level output
    # param_inputs = request_inputs[['refdes','method','stream', 'parameter','date']].copy()
    param_inputs = request_inputs[['refdes','method','stream', 'parameter','date']]
    param_inputs = param_inputs.drop_duplicates()
    param_inputs['date'] = pd.to_datetime(param_inputs['date'])
    param_inputs['date'] = param_inputs.date.dt.strftime('%Y-%m-%d')
    
    # param_result = ooi_parameter_data_gr[['refdes','method','stream','parameter','date']].copy()
    param_result = ooi_parameter_data_gr[['refdes','method','stream','parameter','date']]
    param_result = param_result.drop_duplicates()
    param_result['date'] = pd.to_datetime(param_result['date'])
    param_result['date'] = param_result.date.dt.strftime('%Y-%m-%d')

    failed_gr_qc = param_result.merge(param_inputs,indicator=True,how='outer')
    failed_gr_qc = failed_gr_qc[failed_gr_qc['_merge'] == 'right_only']
    del failed_gr_qc['_merge']
    failed_gr_qc['value'] = 0
    param_result['value'] = 1
    param_final = pd.concat([param_result, failed_gr_qc])

    # stream level rollup
    print('    creating outputs stream...')
    # stream_inputs = request_inputs[['refdes','method','stream','date']].copy()
    stream_inputs = param_inputs[['refdes','method','stream','date']]
    stream_inputs = stream_inputs.drop_duplicates()
    stream_inputs['date'] = pd.to_datetime(stream_inputs['date'])
    stream_inputs['date'] = stream_inputs.date.dt.strftime('%Y-%m-%d')
    
    # stream_result = ooi_parameter_data_gr[['refdes','method','stream','date']].copy()
    stream_result = param_result[['refdes','method','stream','date']]
    stream_result = stream_result.drop_duplicates()
    stream_result['date'] = pd.to_datetime(stream_result['date'])
    stream_result['date'] = stream_result.date.dt.strftime('%Y-%m-%d')
    
    missing_streams = stream_result.merge(stream_inputs,indicator=True,how='outer')
    missing_streams = missing_streams[missing_streams['_merge'] == 'right_only']
    del missing_streams['_merge']
    missing_streams['value'] = 0
    stream_result['value'] = 1
    stream_final = pd.concat([stream_result,missing_streams])


    #method level output
    print('    creating outputs method...')
    # method_inputs = request_inputs[['refdes','method','date']].copy()
    method_inputs = stream_inputs[['refdes','method','date']]
    method_inputs = method_inputs.drop_duplicates()
    method_inputs['date'] = pd.to_datetime(method_inputs['date'])
    method_inputs['date'] = method_inputs.date.dt.strftime('%Y-%m-%d')

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

    # method_result = ooi_parameter_data_gr[['refdes','method','date']].copy()
    method_result = stream_result[['refdes','method','date']]
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
    print('    creating outputs refdes...')
    # refdes_inputs = request_inputs[['refdes','date']].copy()
    refdes_inputs = method_inputs[['refdes','date']]
    refdes_inputs = refdes_inputs.drop_duplicates()
    refdes_inputs['date'] = pd.to_datetime(refdes_inputs['date'])
    refdes_inputs['date'] = refdes_inputs.date.dt.strftime('%Y-%m-%d')
    
    # refdes_result = ooi_parameter_data_gr[['refdes','date']].copy()
    refdes_result = method_result[['refdes','date']]
    refdes_result = refdes_result.drop_duplicates()
    refdes_result['date'] = pd.to_datetime(refdes_result['date'])
    refdes_result['date'] = refdes_result.date.dt.strftime('%Y-%m-%d')
    
    missing_refdes = refdes_result.merge(refdes_inputs,indicator=True,how='outer')
    missing_refdes = missing_refdes[missing_refdes['_merge'] == 'right_only']
    del missing_refdes['_merge']
    missing_refdes['value'] = 0
    refdes_result['value'] = 1
    refdes_final = pd.concat([refdes_result, missing_refdes])


    param_final.to_csv(param_dir + array + '_param_final.csv', index=False)
    stream_final.to_csv(stream_dir + array + '_stream_final.csv', index=False)
    method_final.to_csv(method_dir + array + '_method_final.csv', index=False)
    refdes_final.to_csv(refdes_dir + array + '_refdes_final.csv', index=False)

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

def alert_send(array,missing_gr_qc_values,missing_science_classification,recipients):
    html_template = """
    <html>
    <body>{{ body }}</body>
    </html>
    """

    text = '<h2>' + array + ' Stats Run Completed</h2>'

    text = text + 'The data are available for import into the QC Database from boardwalk under /home/knuth/ooi_stats/stats/output'

    text = text + '<h3><font color="red">Descrepancies:</font></h3>'
    if missing_gr_qc_values.empty:
        pass
    else:
        text = text + 'Parameter instances <b><font color="red">missing global range qc values:</font></b><br><br>'
        text = text + '<small>Values need to be entered here: https://github.com/ooi-integration/qc-lookup/blob/master/data_qc_global_range_values.csv</small><br><br>'
        f = missing_gr_qc_values.to_html()
        text = text + str(f) + '<br><br>'

    if missing_science_classification.empty:
        pass
    else:
        text = text + 'Parameter instances with QC values but <b><font color="red">not classified as science parameters:</font></b> but that have global range qc values<br><br>'
        text = text + '<small>Classification needs to be made in preload as "Science Data" under column dataproducttype: https://github.com/oceanobservatories/preload-database/blob/master/csv/ParameterDefs.csv</small><br><br>'
        text = text + '<small>If the parameter does not exist in preload, then the parameter needs to be removed from the QC tables https://github.com/ooi-integration/qc-lookup/blob/master/data_qc_global_range_values.csv</small><br><br>'
        f = missing_science_classification.to_html()
        text = text + str(f) + '<br><br>'



    subject = 'Stats completed for ' + array + ' on ' + datetime.datetime.utcnow().strftime('%Y-%m-%d at %H:%M:%S')
    text = print_html_doc(html_template,text)

    msg = MIMEText(text,'html')
    msg['Subject'] = subject

    sendEmail(msg,recipients)









