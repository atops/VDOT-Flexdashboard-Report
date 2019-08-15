#!/bin/bash -l


echo $(date)
cd /home/atoppen/Code/VDOT/VDOT-Flexdashboard-Report

Rscript Monthly_Report_Calcs.R
Rscript Monthly_Report_Package.R

aws s3 sync . s3://vdot-spm/dashboard --exclude "*.*" --include "ATSPM_Det*.csv"
aws s3 sync . s3://vdot-spm/dashboard --exclude "*.*" --include "*.fst"

#../flush_dns.sh 
