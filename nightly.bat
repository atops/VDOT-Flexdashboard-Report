echo %date%%time%
for /f "tokens=1-4 delims=/ " %%i in ("%date%") do (
     set dow=%%i
     set month=%%j
     set day=%%k
     set year=%%l
)
set datestr=%year%%month%%day%
echo datestr is %datestr% > nightly_last_run.txt

set rando=%random%
"C:\Program Files\R\R-4.2.2\bin\Rscript.exe" Monthly_Report_Calcs.R > logs\nightly_%datestr%_%rando%.log
"C:\Program Files\R\R-4.2.2\bin\Rscript.exe" Monthly_Report_Package.R >> logs\nightly_%datestr%_%rando%.log
"C:\Program Files\R\R-4.2.2\bin\Rscript.exe" Monthly_Report_Package_1hr.R >> logs\nightly_%datestr%_%rando%.log
"C:\Program Files\R\R-4.2.2\bin\Rscript.exe" Monthly_Report_Package_15min.R >> logs\nightly_%datestr%_%rando%.log
