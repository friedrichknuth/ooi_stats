#!/bin/bash
# PATH=/bin:/usr/bin;
# PATH="/home/coolgroup/bin/miniconda2/bin:$PATH"

# source activate stats
for i in RS CP GA GI GP GS CE 
# for i in RS
do
	/home/coolgroup/bin/miniconda2/envs/stats/bin/python /home/knuth/ooi_stats/stats/stats_run.py --array $i
done
zip -r /home/knuth/ooi_stats/stats/previous_output/$(date +"%Y%m%d") /home/knuth/ooi_stats/stats/output
# source deactivate
