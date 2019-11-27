# Q-Replication-Monitoring

This repository contains SQL scripts to 

a) monitor IBM Q Replication runtime processes 
    qrep_monitor_capture_EN.sql
    qrep_monitor_apply_EN.sql
    
b) validate a Q Replication setup
    qrep_check_qsubs_capture_EN.sql
    qrep_check_qsubs_apply_EN.sql
        
How to run the scripts:

#> db2 connect to <capture_server>
#> db2 set current schema = '<capture_schema>'
#> db2 -tvf <capture-script>

#> db2 connect to <apply_server>
#> db2 set current schema = '<apply_schema>'
#> db2 -tvf <apply-script>

All four scripts create an easily readable report. 
