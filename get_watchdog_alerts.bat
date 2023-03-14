echo %date%%time%
for /f "tokens=1-4 delims=/ " %%i in ("%date%") do (
     set dow=%%i
     set month=%%j
     set day=%%k
     set year=%%l
)
set datestr=%year%%month%%day%
echo datestr is %datestr%

call activate.bat
cd c:\TractionMetrics\production
if not exist logs mkdir logs
conda run -n vdot-flexdashboard-report python get_watchdog_alerts.py >> logs\get_watchdog_alerts_%datestr%.log
Rscript get_alerts.R
