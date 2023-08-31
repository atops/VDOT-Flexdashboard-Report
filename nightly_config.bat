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

call c:\ProgramData\miniconda3\Scripts\activate.bat
cd c:\TractionMetrics\production\
call c:\ProgramData\miniconda3\Scripts\conda activate tractionmetrics
call C:\Users\alan_toppen\.conda\envs\tractionmetrics\python nightly_config.py && (
  echo nightly.py was successful >> nightly_config_last_run.txt
) || (
  echo nightly.py failed >> nightly_config_last_run.txt
)
