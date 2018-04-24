import stats_functions as sf
import pickle as pk
import os
import subprocess
import shutil
import datetime
import click as click

# arrays = ['RS','CP','GA','GI','GP','GS','CE']

username = ''
token = ''

@click.command()
@click.option('--array', type=click.Choice(['RS','CP','GA','GI','GP','GS','CE']))
def main(array):
    
    

    print(array)


    out_dir = '/home/knuth/ooi_stats/stats/'
    interp = '/home/coolgroup/bin/miniconda2/envs/stats/bin/python'

    # out_dir = '/Users/knuth/Documents/ooi/repos/github/ooi_stats/stats/'
    # interp = '/Users/knuth/miniconda2/envs/stats/bin/python'

    # recipients = ['vardaro@marine.rutgers.edu','crowley@marine.rutgers.edu','knuth@marine.rutgers.edu','michaesm@marine.rutgers.edu','lgarzio@marine.rutgers.edu','leila@marine.rutgers.edu','sage@marine.rutgers.edu']
    recipients = ['knuth@marine.rutgers.edu','sage@marine.rutgers.edu','vardaro@marine.rutgers.edu']


    global_ranges = sf.request_gr(username, token)

    stats_deployment_data = []
    request_urls,request_inputs = [],[]
    missing_gr_qc_values , missing_science_classification = [],[]

    stats_deployment_data = sf.request_stats_deployments(array, username, token)
    request_urls,request_inputs = sf.build_stats_requests(array, stats_deployment_data)
    missing_gr_qc_values , missing_science_classification = sf.check_sci_v_gr(array, global_ranges , request_inputs)
    
    with open('urls.pk', 'wb') as fh:
        pk.dump(request_urls,fh)
    
    with open('ranges.pk', 'wb') as fh:
        pk.dump(global_ranges,fh)

    subprocess.call([interp, out_dir+'request_data.py', '--array', array])

    sf.stats_create_all_outputs(array,request_inputs,out_dir)
    sf.alert_send(array,missing_gr_qc_values,missing_science_classification,recipients)
    
    os.remove('urls.pk')
    os.remove('ooi_parameter_data_gr.csv')
    os.remove(array+'_requests.log')

    # x = datetime.datetime.utcnow().strftime('%Y%m%d')
    # shutil.make_archive(out_dir + 'previous_output/' + x, 'zip', out_dir+'output')  

    os.remove('ranges.pk')


if __name__ == '__main__':
    main()
