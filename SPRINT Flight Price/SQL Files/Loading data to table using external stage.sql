-----CREATING TABLE------
create or replace table mytable2 
(row_id  number , airline  string , flight varchar , source_city  string  ,
departure_time string , stops string , arrival_time string , destination_city string , class string ,
duration varchar ,  days_left number , price number);

copy into mytable2
from @my_s3_stage
file_format = (type = csv field_optionally_enclosed_by='"',skip_header=1)
pattern = '.*.csv'
on_error = 'skip_file';
    
    select * from mytable2;