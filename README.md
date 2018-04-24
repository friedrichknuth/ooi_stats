# Description

This repository contains a set of scripts that produce the statistics seen under `monthly stats` and `daily stats` at http://ooi.visualocean.net/, as well as automated daily alerts about data avilability and science data products being produced outside global ranges. For questions, please contact knuth@marine.rutgers.edu.

## Setup

`conda create -n python=3.6 stats requests jinja2 netcdf4 numpy jupyter ipykernel`

### Crontab
The scripts are not designed to be run manually on a regular basis. Credentials, directory paths and python interpreters are hard coded into the routines, as outlined below, and not designed to be interactively entered. Instead, the scripts are to be run server side on a cron job. For example:

##### 0 8 * * * /your/server/path/to/miniconda2/envs/stats/bin/python ~/ooi_stats/alerts/data_alerts_run.py  
##### 0 8 * * 5 ~/ooi_stats/stats/stats_run_all.sh > ~/ooi_stats/stats/stats_run.log  
##### 0 8 * * * /your/server/path/to/bin/miniconda2/envs/stats/bin/python ~/ooi_stats/alerts/platform_alerts/platform_alerts_run.py  

### Data Availability and Global Range Stats

`stats/stats_run_all.sh`

1. Specify your python interpreter
2. Specify the output directory
3. Add ooinet username and password
4. Specify the output directory and python interpreter
5. Add your sender email credentials

### Daily Data Availability Alerts

`alerts/data_alerts_run.py`

1. Add ooinet username and password
2. Add alert recipients
3. Specify the output directory
4. Add your sender email credentials


### Daily Platform Engineering Alerts

`alerts/engineering_alerts_run.py`

1. Add ooinet username and password  
2. Add alert recipients 
3. Specify the output directory 
4. Add your sender email credentials 
