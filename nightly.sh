#!/bin/bash

#/home/atoppen/Code/VDOT/restart_vpn.sh 

Rscript /home/atoppen/Code/VDOT/VDOT-Flexdashboard-Report/Monthly_Report_Calcs.R
Rscript /home/atoppen/Code/VDOT/VDOT-Flexdashboard-Report/Monthly_Report_Package.R
/home/atoppen/Code/VDOT/flush_dns.sh 
