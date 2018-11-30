#!/bin/bash -l


#/home/atoppen/Code/VDOT/restart_vpn.sh 

cd /home/atoppen/Code/VDOT/VDOT-Flexdashboard-Report

Rscript Monthly_Report_Calcs.R
Rscript Monthly_Report_Package.R

/home/atoppen/Code/VDOT/flush_dns.sh 
