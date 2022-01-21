#!/bin/bash -l


echo $(date)

# This is the new AWS-oriented VDOT code. For multiple regions.
# Maps closely to GDOT.
# Run in parallel with VDOT-Flexdashboard-Report local code, until stable.

echo ---------------------------------------
echo --- RUN R SCRIPTS - NEWER VDOT CODE --- 
echo

echo --- Pull ATSPM DATA
# This is done on the local VDOT server and pushed to S3
# ~/miniconda3/bin/python pull_atspm_data.py


echo --- R Scripts
cd /home/atoppen/Code/VDOT/scheduled_tasks
Rscript Monthly_Report_Calcs_ec2.R
Rscript Monthly_Report_Package.R


echo --- Watchdog
# Ideally this would run at 7am or so to capture today's ATSPM watchdogs.
# This is run on local VDOT server and pushed to S3
#~/miniconda3/bin/python get_watchdog_alerts.py
Rscript get_alerts.R

