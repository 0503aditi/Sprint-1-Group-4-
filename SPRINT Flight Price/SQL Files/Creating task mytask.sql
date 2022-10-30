------CREATING TASK MYTASK--- 
CREATE or replace TASK mytask
WAREHOUSE = my_wh
SCHEDULE = 'Using CRON 0 0 * * THU   Asia/Kolkata'
TIMESTAMP_INPUT_FORMAT = 'YYYY-MM-DD HH24'
 
AS
copy into mytable2
from (select t.$1,t.$2,t.$3,t.$4,t.$5,t.$6,t.$7,t.$8,t.$9,t.$10,t.$11,t.$12 from @my_s3_stage/ t)
file_format = (type = csv field_optionally_enclosed_by='"')
pattern = '.*.csv'
on_error = 'skip_file'; 

SHOW TASKS;   
    
alter task mytask resume;
alter task mytask suspend;