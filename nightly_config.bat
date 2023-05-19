echo %date%%time%
for /f "tokens=1-4 delims=/ " %%i in ("%date%") do (
     set dow=%%i
     set month=%%j
     set day=%%k
     set year=%%l
)
set datestr=%year%%month%%day%
del nightly_config_last_run.txt
echo datestr is %datestr% > nightly_config_last_run.txt

call activate.bat
cd c:\TractionMetrics\production\
del logs\nightly_config_%datestr%.log
conda run -n tractionmetrics python nightly_config.py > logs\nightly_config_%datestr%.log
