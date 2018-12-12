#!/bin/bash -l


echo $(date)
cd /home/atoppen/Code/VDOT/VDOT-Flexdashboard-Report

Rscript Monthly_Report_Calcs.R
Rscript Monthly_Report_Package.R

../flush_dns.sh 
