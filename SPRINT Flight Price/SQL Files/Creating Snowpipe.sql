------CREATING SNOWPIPE--
create or replace pipe snowpipe auto_ingest=true as
copy into mytable2
from @my_s3_stage   
file_format = (type = csv field_optionally_enclosed_by='"')
pattern = '.*.csv'
on_error = 'skip_file';
    
show pipes;
alter pipe snowpipe refresh;
select * from mytable2;


select SYSTEM$PIPE_STATUS('snowpipe')

select * from table(validate_pipe_load(
pipe_name=>'sprint4.PUBLIC.snowpipe',
start_time=>dateadd(minute, -4, current_timestamp())));
  
select *
from table(information_schema.copy_history(table_name=>'mytable2', start_time=> dateadd(minute, -4, current_timestamp())));