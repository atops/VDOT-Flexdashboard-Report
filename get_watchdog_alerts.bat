echo %date%%time%
for /f "tokens=1-4 delims=/ " %%i in ("%date%") do (
     set dow=%%i
     set month=%%j
     set day=%%k
     set year=%%l
)
set datestr=%year%%month%%day%
echo datestr is %datestr%

set rando=%random%
call activate.bat
cd c:\TractionMetrics\production
if not exist logs mkdir logs
conda run -n tractionmetrics python get_watchdog_alerts.py >> logs\get_watchdog_alerts_%datestr%_%rando%.log
"C:\Program Files\R\R-4.2.3\bin\Rscript.exe" get_alerts.R >> logs\get_watchdog_alerts_%datestr%_%rando%.log

