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
DATA_TEAM_PORTAL_URL = 'http://ooi.visualocean.net/data-streams/export/'

# out_dir = '/home/knuth/ooi_stats/alerts/platform_alerts/'
out_dir = '/Users/knuth/Documents/ooi/repos/github/ooi_stats/alerts/platform_alerts/'

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

def get_most_recent_eng(array):
    try:
        stream_dir = out_dir + 'output/' + array+'/'+'engineering_stream'+'/*'
        stream_list_of_files = glob.glob(stream_dir)

        stream_latest_file = max(stream_list_of_files, key=os.path.getctime)

        with open(stream_latest_file, 'rb') as f:
            stream_most_recent = pk.load(f)

        return stream_most_recent

    except:
        stream_most_recent = pd.DataFrame(columns =['refdes','method','stream'])

        return stream_most_recent









def request_annotations(array, username, token):
    beginDT  = int(nc.date2num(datetime.datetime.strptime("2012-01-01T01:00:01Z",'%Y-%m-%dT%H:%M:%SZ'),'seconds since 1970-01-01')*1000)
    endDT = int(nc.date2num(datetime.datetime.utcnow(),'seconds since 1970-01-01')*1000) 

    refdes_in = DATA_TEAM_PORTAL_URL + array
    refdes_list = pd.read_csv(refdes_in)
    refdes_list = refdes_list[refdes_list['method'].str.contains("recovered")==False]
    refdes_list = refdes_list[refdes_list['stream_type'].str.contains("Science")==False]
    refdes_list = refdes_list[['reference_designator','method', 'stream_name']]
    refdes_list.columns = ['refdes','method', 'stream']
    refdes_list = refdes_list['refdes']

    # added regex search to exclude or grab cabled cabled assets to produce complete Endurance and Cabled Array outputs
    if array == 'CE':
        refdes_list = refdes_list[refdes_list.str.contains('|'.join(CE_cabled))==False]

    if array == 'RS':
        refdes_in_CE = DATA_TEAM_PORTAL_URL + 'CE'
        refdes_list_CE = pd.read_csv(refdes_in_CE)
        refdes_list_CE = refdes_list_CE[refdes_list_CE['method'].str.contains("recovered")==False]
        refdes_list_CE = refdes_list_CE[refdes_list_CE['stream_type'].str.contains("Science")==False]
        refdes_list_CE = refdes_list_CE[['reference_designator','method', 'stream_name']]
        refdes_list_CE.columns = ['refdes','method', 'stream']
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
    platform_list = []
    node_list = []

    future_to_url = {pool.submit(request_data, url, username, token): url for url in anno_requests}
    for future in concurrent.futures.as_completed(future_to_url):
        url_rf = future_to_url[future]
        try:
            anno_info = future.result()
            anno_info = anno_info.json()

            for i in range(len(anno_info)):
                if anno_info[i]['endDT'] is None and anno_info[i]['sensor'] is None and anno_info[i]['qcFlag'] == 'not_operational':
                    platform =  anno_info[i]['subsite'] 
                    platform_list.append(platform)
                    node =  anno_info[i]['node'] 
                    node_list.append(node)
        except:
            pass


    data_dict={
        'subsite':platform_list,
        'platform':node_list}

    not_operational = pd.DataFrame(data_dict, columns = ['subsite','platform'])
    not_operational.fillna(value=np.nan, inplace=True)

    return not_operational






# def alert_request_eng_deployments(array, username, token):
    # refdes_in = DATA_TEAM_PORTAL_URL + array
    # refdes_list = pd.read_csv(refdes_in)
    # refdes_list = refdes_list[refdes_list['method'].str.contains("recovered")==False]
    # refdes_list = refdes_list[refdes_list['stream_type'].str.contains("Science")==False]
    # refdes_list = refdes_list[['reference_designator','method', 'stream_name']]
    # refdes_list.columns = ['refdes','method', 'stream']
    # refdes_list = refdes_list['refdes']

    # # added regex search to exclude or grab cabled cabled assets to produce complete Endurance and Cabled Array outputs
    # if array == 'CE':
    #     refdes_list = refdes_list[refdes_list.str.contains('|'.join(CE_cabled))==False]

    # if array == 'RS':
    #     refdes_in_CE = DATA_TEAM_PORTAL_URL + 'CE'
    #     refdes_list_CE = pd.read_csv(refdes_in_CE)
    #     refdes_list_CE = refdes_list_CE[refdes_list_CE['method'].str.contains("recovered")==False]
    #     refdes_list_CE = refdes_list_CE[refdes_list_CE['stream_type'].str.contains("Science")==False]
    #     refdes_list_CE = refdes_list_CE[['reference_designator','method', 'stream_name']]
    #     refdes_list_CE.columns = ['refdes','method', 'stream']
    #     refdes_list_CE = refdes_list_CE['refdes']
    #     refdes_list_CE = refdes_list_CE[refdes_list_CE.str.contains('|'.join(CE_cabled))]
    #     refdes_list = pd.concat([refdes_list_CE,refdes_list])

    # refdes_list = refdes_list.drop_duplicates()

    # print("working on", array)
    # print("    building deployment info requests...")
    # asset_requests = []
    # for i in refdes_list:
    #     sub_site = i[:8]
    #     platform = i[9:14]
    #     instrument = i[15:27]
    #     asset_url_inputs = '/'.join((sub_site, platform, instrument))
    #     request_url = DEPLOYEMENT_URL+asset_url_inputs+'/-1'
    #     asset_requests.append(request_url)

    # print("    sending deployment info requests...")
    # ref_des_list = []
    # start_time_list = []
    # deployment_list = []

    # start_time = datetime.datetime.utcnow() - datetime.timedelta(seconds=86400)

    # future_to_url = {pool.submit(request_data, url, username, token): url for url in asset_requests}
    # for future in concurrent.futures.as_completed(future_to_url):
    #     try:
    #         asset_info = future.result()
    #         asset_info = asset_info.json()

    #         for i in range(len(asset_info)):
    #             if asset_info[i]['eventStopTime'] is None:
    #                 refdes = asset_info[i]['referenceDesignator']
    #                 ref_des_list.append(refdes)
                    
    #                 deployment = asset_info[i]['deploymentNumber']
    #                 deployment_list.append(deployment)
    #                 start_time_list.append(start_time)
    #     except:
    #         pass
     
    # data_dict={
    #     'refdes':ref_des_list,
    #     'deployment':deployment_list,
    #     'start_time':start_time_list}

    # alert_deployment_data = pd.DataFrame(data_dict, columns = ['refdes', 'deployment','start_time'])

    # alert_deployment_data['subsite'] = alert_deployment_data.refdes.str[:8]
    # alert_deployment_data['platform'] = alert_deployment_data.refdes.str[9:14]
    # alert_deployment_data['instrument'] = alert_deployment_data.refdes.str[15:27]
    # alert_deployment_data = alert_deployment_data[['subsite','platform','start_time']]

    # alert_deployment_data = alert_deployment_data.drop_duplicates()

    # return alert_deployment_data







def alert_build_eng_requests(array):

    print("    building data request urls...")
    start_time = datetime.datetime.utcnow() - datetime.timedelta(seconds=86400)

    refdes_streams = out_dir + 'RS_input.csv'
    refdes_streams_df = pd.read_csv(refdes_streams)
    refdes_streams_df['method'] = 'streamed'
    refdes_streams_df = refdes_streams_df[['refdes','method', 'stream']]
    refdes_streams_df['subsite'] = refdes_streams_df.refdes.str[:8]
    refdes_streams_df['platform'] = refdes_streams_df.refdes.str[9:14]
    refdes_streams_df['instrument'] = refdes_streams_df.refdes.str[15:27]

    request_inputs = refdes_streams_df

    request_inputs['date'] = pd.to_datetime(start_time)
    request_inputs['date'] = request_inputs.date.dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')

    request_inputs['urls'] = DATA_URL+\
                            request_inputs.subsite+\
                            '/'+request_inputs.platform+\
                            '/'+request_inputs.instrument+\
                            '/'+request_inputs.method+\
                            '/'+request_inputs.stream+\
                            '?beginDT='+request_inputs.date+\
                            '&limit=100'

    request_urls = request_inputs['urls'].drop_duplicates()
    request_urls = request_urls.values.tolist()



    # print("    building data request urls...")
    # refdes_streams = out_dir + 'RS_input.csv'
    # refdes_streams_df = pd.read_csv(refdes_streams)
    # refdes_streams_df['method'] = 'streamed'
    # refdes_streams_df = refdes_streams_df[['refdes','method', 'stream']]
    # refdes_streams_df['subsite'] = refdes_streams_df.refdes.str[:8]
    # refdes_streams_df['platform'] = refdes_streams_df.refdes.str[9:14]
    # refdes_streams_df['instrument'] = refdes_streams_df.refdes.str[15:27]


    # request_inputs = pd.merge(refdes_streams_df,alert_deployment_data, on=['subsite','platform'])


    # request_inputs['date'] = pd.to_datetime(request_inputs['start_time'])
    # request_inputs['date'] = request_inputs.date.dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')


    # request_inputs['urls'] = DATA_URL+\
    #                         request_inputs.subsite+\
    #                         '/'+request_inputs.platform+\
    #                         '/'+request_inputs.instrument+\
    #                         '/'+request_inputs.method+\
    #                         '/'+request_inputs.stream+\
    #                         '?beginDT='+request_inputs.date+\
    #                         '&limit=100'

    # request_urls = request_inputs['urls'].drop_duplicates()
    # request_urls = request_urls.values.tolist()

    return request_urls , request_inputs









def send_eng_data_requests(array,request_urls,username,token,RS_recipients,CE_recipients,GA_recipients,CP_recipients):

    print('    sending data requests...')
    eng_streams_data = pd.DataFrame()

    future_to_url = {pool.submit(request_data, url, username, token): url for url in request_urls}
    for future in concurrent.futures.as_completed(future_to_url):
        # url = future_to_url[future]
        try:
            data = future.result() 
            data = data.json()

            refdes_list = []
            method_list = []
            stream_list = []
            value_list = []
            
            # use this to speed up the loop
    #         df = pd.DataFrame.from_records(map(json.loads, map(json.dumps,data)))

            refdes = data[-1]['pk']['subsite'] + '-' + data[-1]['pk']['node'] + '-' + data[-1]['pk']['sensor']
            method = data[-1]['pk']['method']
            stream = data[-1]['pk']['stream']

            for ts in virtual_times:
                try:
                    value_list.append(data[-1][ts])
                    refdes_list.append(refdes)
                    method_list.append(method)
                    stream_list.append(stream)
                except:
                    continue
                

            # create data frame from lists collected above
            data_dict = {
                'refdes':refdes_list,
                'method':method_list,
                'stream':stream_list,
                'value':value_list
                }
            response_data = pd.DataFrame(data_dict, columns = ['refdes','method','stream','value'])


            eng_streams_data = eng_streams_data.append(response_data)
                
        except:
            # print('no data for ', url)
            pass
    gc.collect()

    if eng_streams_data.empty:
        html_template = """
        <html>
        <body>{{ body }}</body>
        </html>
        """
        text = '<h3><font color="red">No platform engineering data return. OMS Extractor possibly down. </font></h3>'

        subject = 'Engineering Streams Alert for ' + array + ' on ' + datetime.datetime.utcnow().strftime('%Y-%m-%d')
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

    return eng_streams_data








def alert_create_eng_outputs(array,eng_streams_data,request_inputs):

    # stream level rollup
    stream_inputs = request_inputs[['refdes','method','stream']].copy()
    stream_inputs = stream_inputs.drop_duplicates()
    stream_result = eng_streams_data[['refdes','method','stream']].copy()
    stream_result = stream_result.drop_duplicates()
    missing_streams = stream_result.merge(stream_inputs,indicator=True,how='outer')
    missing_streams = missing_streams[missing_streams['_merge'] == 'right_only']
    del missing_streams['_merge']
    missing_streams['value'] = 0
    stream_result['value'] = 1
    stream_final = pd.concat([stream_result,missing_streams])

    return stream_final









def alert_create_missing_output(array,stream_final):
    print('    writing output...')

    stream_final_out = stream_final[stream_final['value'] == 0]
    del stream_final_out['value']

    stream_dir = out_dir + 'output/' + array+'/'+'engineering_stream'+'/'
    create_dir(stream_dir)


    out = stream_dir + array + '_eng_stream_data_'+ datetime.datetime.utcnow().strftime('%Y-%m-%d') + '.pd'
    with open(out, 'wb') as fh:
            pk.dump(stream_final_out,fh)

    return stream_final_out







def compare_operational(not_operational, stream_final_out):

    stream_final_out['subsite'] = stream_final_out.refdes.str[:8]
    stream_final_out['platform'] = stream_final_out.refdes.str[9:14]
    annotated_subsites = stream_final_out.merge(not_operational,indicator=True, on=['subsite'], how='inner')
    annotated_subsites = annotated_subsites[['refdes','method','stream']]
    annotated_platforms = stream_final_out.merge(not_operational,indicator=True, on=['subsite','platform'], how='inner')
    annotated_platforms = annotated_platforms[['refdes','method','stream']]
    annotated_and_not_operational = pd.concat([annotated_subsites,annotated_platforms])
    annotated_and_not_operational = annotated_and_not_operational.drop_duplicates()

    stream_final_out = stream_final_out[['refdes','method','stream']]
    no_data_not_annotated = annotated_and_not_operational.merge(stream_final_out,indicator=True,how='outer')
    no_data_not_annotated = no_data_not_annotated[no_data_not_annotated['_merge'] == 'right_only']
    del no_data_not_annotated['_merge']
    
    return no_data_not_annotated, annotated_and_not_operational







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

def alert_send(array,no_data_not_annotated,annotated_and_not_operational,stream_difference_new,stream_difference_resumed,RS_recipients,CE_recipients,GA_recipients,CP_recipients):
    html_template = """
    <html>
    <body>{{ body }}</body>
    </html>
    """

    if stream_difference_new.empty and stream_difference_resumed.empty:
        text = '<h2>Ongoing Issues</h2>'
        pass
    else:
        text = '<h2><font color="red">New Alert</font></h2>'
        # text = text + '<br><small>*Alerts are only sent once when there is a new alert. If and instrument resumes producing data a new alert will be sent.</small><br><br>'

    if stream_difference_new.empty: 
        pass
    else:
        text = text + '<b>Engineering streams that <font color="red">have not produced data</font> in the past 24 hours:</b><br><br>'
        f = stream_difference_new.to_html()
        text = text + str(f) + '<br><br>'

    # if data_but_annotated.empty:
    #     pass
    # else:
    #     text = text + '<b>Instruments that are <font color="red">annotated as not_operational, but have resumed producing data:</font></b><br><br>'
    #     f = data_but_annotated.to_html()
    #     text = text + str(f) + '<br><br>'

    if stream_difference_resumed.empty:
        pass
    else:
        text = text + '<b>Engineering streams that <font color="green">resumed producing data</font> in the past 24 hours:</b><br><br>'
        f = stream_difference_resumed.to_html()
        text = text + str(f) + '<br><br>'





    if stream_difference_new.empty and stream_difference_resumed.empty:
        pass
    else: 
        text = text + '<h2>Summary of New and Ongoing Issues</h2>'
    # text = text + '<HR COLOR="lightblue" WIDTH="100%" SIZE=10>' + '<br><br>'

    if no_data_not_annotated.empty:
        pass
    else:
        text = text + 'Engineering streams <b>not producing data</b> for over 24 hours:<br><br>'
        f = no_data_not_annotated.to_html()
        text = text + str(f) + '<br><br>'

    if annotated_and_not_operational.empty:
        pass
    else:
        text = text + 'Engineering streams <b>not producing data and annotated as not_operational</b>:<br><br>'
        f = annotated_and_not_operational.to_html()
        text = text + str(f) + '<br><br>'
    



    subject = 'Engineering Streams Alert for ' + array + ' on ' + datetime.datetime.utcnow().strftime('%Y-%m-%d')
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
