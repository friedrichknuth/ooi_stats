import platform_alerts_functions as af
import warnings
warnings.filterwarnings("ignore")

username = ''
token = ''

array = 'RS'

RS_recipients = ['']
CE_recipients = ['']
GA_recipients = ['']
CP_recipients = ['']


stream_most_recent = af.get_most_recent_eng(array)
alert_deployment_data = af.alert_request_eng_deployments(array,username,token)
not_operational = af.request_annotations(array, username, token)
request_urls,request_inputs = af.alert_build_eng_requests(array,alert_deployment_data)
eng_streams_data = af.send_eng_data_requests(array,request_urls,username,token)
stream_final = af.alert_create_eng_outputs(array,eng_streams_data,request_inputs)
stream_final_out = af.alert_create_missing_output(array,stream_final)
no_data_not_annotated,annotated_and_not_operational = af.compare_operational(not_operational, stream_final_out)
stream_difference_new,stream_difference_resumed = af.stream_compare_output(array, stream_final_out, stream_most_recent, request_inputs)
af.alert_send(array,
          no_data_not_annotated,
          annotated_and_not_operational,
          stream_difference_new,
          stream_difference_resumed,
          RS_recipients,
          CE_recipients,
          GA_recipients,
          CP_recipients)

