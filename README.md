# Description

This repository contains a set of scripts that produce the statistics seen under `monthly stats` and `daily stats` at http://ooi.visualocean.net/, as well as automated daily alerts about data avilability and science data products being produced outside global ranges. For questions, please contact knuth@marine.rutgers.edu.

## Setup

`conda create -n python=3.6 stats requests jinja2 netcdf4 numpy jupyter ipykernel`

### Crontab
The scripts are not designed to be run manually on a regular basis. Credentials, directory paths and python interpreters are hard coded into the routines, as outlined below, and not designed to be interactively entered. Instead, the scripts are to be run server side on a cron job. For example:

0 8 * * * /your/server/path/to/miniconda2/envs/stats/bin/python ~/ooi_stats/alerts/data_alerts_run.py
0 8 * * 5 ~/ooi_stats/stats/stats_run_all.sh > ~/ooi_stats/stats/stats_run.log
0 8 * * * /your/server/path/to/bin/miniconda2/envs/stats/bin/python ~/ooi_stats/alerts/platform_alerts/platform_alerts_run.py

### Data Availability and Global Range Stats

`stats/stats_run_all.sh`

1. Specify your python interpreter https://github.com/friedrichknuth/ooi_stats/blob/master/stats/stats_run_all.sh#L9
2. Specify the output directory https://github.com/friedrichknuth/ooi_stats/blob/master/stats/stats_run_all.sh#L11
3. Add ooinet username and password https://github.com/friedrichknuth/ooi_stats/blob/master/stats/stats_run.py#L11
4. Specify the output directory and python interpreter https://github.com/friedrichknuth/ooi_stats/blob/master/stats/stats_run.py#L23
5. Add your sender email credentials https://github.com/friedrichknuth/ooi_stats/blob/master/stats/stats_functions.py

### Daily Data Availability Alerts

`alerts/data_alerts_run.py`

1. Add ooinet username and password https://github.com/friedrichknuth/ooi_stats/blob/master/alerts/data_alerts_run.py#L3
2. Add alert recipients https://github.com/friedrichknuth/ooi_stats/blob/master/alerts/data_alerts_run.py#L8
3. Specify the output directory https://github.com/friedrichknuth/ooi_stats/blob/master/alerts/data_alerts_functions.py#L27
4. Add your sender email credentials https://github.com/friedrichknuth/ooi_stats/blob/master/alerts/data_alerts_functions.py#L770


### Daily Platform Engineering Alerts

`alerts/engineering_alerts_run.py`

1. Add ooinet username and password https://github.com/friedrichknuth/ooi_stats/blob/master/alerts/platform_alerts/platform_alerts_run.py#L5
2. Add alert recipients https://github.com/friedrichknuth/ooi_stats/blob/master/alerts/platform_alerts/platform_alerts_run.py#L10
2. Specify the output directory https://github.com/friedrichknuth/ooi_stats/blob/master/alerts/platform_alerts/platform_alerts_functions.py#L27
2. Add your sender email credentials https://github.com/friedrichknuth/ooi_stats/blob/master/alerts/platform_alerts/platform_alerts_functions.py#L481
