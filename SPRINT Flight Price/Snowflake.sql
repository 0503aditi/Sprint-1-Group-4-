-------CREATING DATABASE--------
create database sprint4;

---------AWS INTEGRATION EXTERNAL STAGE----------
create or replace storage integration s3_int
type = external_stage
storage_provider = s3
enabled = true
storage_aws_role_arn = 'arn:aws:iam::242650538488:role/sprint-4-role'
storage_allowed_locations = ('s3://sprint-4-bucket/');
  
  DESC INTEGRATION s3_int;
  
  -----CREATE FILE FORMAT------
create or replace file format my_csv_format
type = csv field_delimiter = ',' skip_header = 1 null_if = ('NULL', 'null') empty_field_as_null = true; 


  desc file format my_csv_format; 
  
  ----STAGE CREATION--------
create or replace stage my_s3_stage
storage_integration = s3_int
url = 's3://sprint-4-bucket/'
file_format = my_csv_format;
  
  List @my_s3_stage;    
  
select t.$1 as row_id , t.$2 as airline , t.$3 as flight , t.$4 as source_city ,    t.$5 as departure_time ,  t.$6 as stops , 
t.$7 as arrival_time ,  t.$8 as destination_city ,  t.$9 as class ,  t.$10 as duration ,  t.$11 as days_left ,  t.$12 as price   
from @my_s3_stage/ t; 

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

SHOW TASKS   
    
alter task mytask resume;
alter task mytask suspend;
 
--execute task mytask; 

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

  
--------CREATING SCD2-------
create or replace  stream sprint4 on table mytable2;



select * from sprint4;


insert into mytable2 values (1, 'Indigo','SG-8709','Nagpur','Early_Morning' , 'zero' , 'Night' , 'Mumbai' , 'Economy',
                            2.17 , 1 ,5953 );
insert into mytable2 values (21, 'AirAsia','SG-8710','Gadchiroli','Late_Night' , 'one' , 'Early_Morning' , 'Mumbai' , 'Business',
                            2.18 , 1 ,5900 ); 
                            
                             
                             
update  mytable2  set departure_time = 'Night' where row_id=22581;

-----CREATING TABLE TARGER_T-----
create or replace  
table target_t(row_id  number , airline  string , flight varchar , source_city  string  ,
departure_time string , stops string , arrival_time string , destination_city string , class string ,
duration varchar ,  days_left number , price number,stream_type string default null, rec_version number default 0, REC_DATE TIMESTAMP_LTZ);

--select * from tgt_merge;  

-----CREATING TASK TGT_MERGE-----
CREATE or replace TASK  tgt_merge
WAREHOUSE = my_wh
SCHEDULE = '1 minute'
WHEN
SYSTEM$STREAM_HAS_DATA('sprint4')
AS
merge into target_t t
using sprint4 s 
on t.row_id=s.row_id and (metadata$action='DELETE')
when matched and metadata$isupdate='FALSE' then update set rec_version=9999, stream_type='DELETE'
when matched and metadata$isupdate='TRUE' then update set rec_version=rec_version-1, stream_type='UPDATE'
when not matched then insert  (row_id , airline   , flight  , source_city    ,
departure_time  , stops  , arrival_time , destination_city  , class  ,
duration  ,  days_left , price,stream_type, rec_version ,REC_DATE) 
values(s.row_id  , s.airline   , s.flight  , s.source_city    ,
s.departure_time  , s.stops  , s.arrival_time , s.destination_city  , s.class  ,
s.duration  ,  s.days_left , s.price,metadata$action,0,CURRENT_TIMESTAMP());



--stream_type, rec_version ,REC_DATE  metadata$action,0,CURRENT_TIMESTAMP()

ALTER TASK tgt_merge RESUME;
ALTER TASK mytask RESUME;
alter task tgt_merge suspend;
show tasks
select * from sprint4;
select * from target_t;
select * from mytable2;

  
insert into mytable2 values (1, 'Indigo','SG-8709','Nagpur','Early_Morning' , 'zero' , 'Night' , 'Mumbai' , 'Economy',
                             2.17 , 1 ,5953 );
                             
 -- update  mytable2  set departure_time = 'Night' where row_id=1;   
delete from mytable2 where row_id=1;  
  
  
  
  ----COLUMN LEVEL SECURITY---
  
  --create or replace table Passengers as select * from 


CREATE  MASKING POLICY flight_policy AS (VAL STRING) RETURNS STRING ->
CASE
WHEN CURRENT_ROLE() IN ('FLIGHT') THEN VAL
ELSE '******'
END;

create role flight;
	  
	  
CREATE OR REPLACE TABLE flight_number( flight STRING MASKING POLICY flight_policy,flight1 STRING)

INSERT INTO flight_number(flight,flight1)
SELECT flight,flight FROM mytable2;
  
select current_role();
select current_user();
    
GRANT SELECT ON flight_number TO ROLE flight;
grant usage on warehouse my_wh to role flight;

grant usage on database sprint4 to role flight;
	
grant usage on schema public to role flight;
	
grant  role flight to user  ADITI0503;
    
select * from flight_number;  
  

-------ROW LEVEL SECURITY----
-- Apply Row-level security using Secure Views
-- create a secure view

------STEP 1 : CREATION OF ROLE----
create or replace role Spicejet;
create or replace role Vistara;
create or replace role Go_First;
create or replace role Air_India;
create or replace role AirAsia;
create or replace role Indigo;

create or replace user sayali password = 'temp123' default_Role = 'Spicejet';
grant role Spicejet to user sayali;

create or replace user kanchan password = 'temp123' default_Role = 'Vistara';
grant role Vistara to user kanchan;


create or replace user bhumika password = 'temp123' default_Role = 'Go_First';
grant role Go_First to user bhumika;

create or replace user priyanshi password = 'temp123' default_Role = 'Air_India';
grant role Air_India to user priyanshi;

create or replace user sandhya password = 'temp123' default_Role = ' AirAsia';
grant role  AirAsia to user sandhya;

create or replace user aditi password = 'temp123' default_Role = 'Indigo';
grant role Indigo to user aditi;

------STEP 2 : GRANT PRIVILEGES TO ROLES-----
grant role SPICEJET to user ADITI0503;
grant role VISTARA to user ADITI0503;
grant role GO_FIRST to user ADITI0503;
grant role AIR_INDIA to user ADITI0503;
grant role AIRASIA to user ADITI0503;
grant role INDIGO to user ADITI0503;


grant usage on warehouse my_wh to role AIRASIA;
grant usage on warehouse my_wh to role AIR_INDIA;
grant usage on warehouse my_wh to role INDIGO;
grant usage on warehouse my_wh to role VISTARA;
grant usage on warehouse my_wh to role GO_FIRST;
grant usage on warehouse my_wh to role SPICEJET;

grant usage on database sprint4 to role  AIRASIA;
grant usage on database sprint4 to role AIR_INDIA;
grant usage on database sprint4 to role INDIGO;
grant usage on database sprint4 to role  VISTARA;
grant usage on database sprint4 to role GO_FIRST;
grant usage on database sprint4 to role SPICEJET;

grant usage on schema public to role AIRASIA;
grant usage on schema public to role AIR_INDIA;
grant usage on schema public to role INDIGO; 
grant usage on schema public to role VISTARA;
grant usage on schema public to role GO_FIRST;
grant usage on schema public to role SPICEJET; 

----STEP 3 : CREATING SECURE VIEW----
create or replace secure view vw_airline as
select a.*
from mytable2 a 
where upper(a.airline) in (select upper(airline)
from airline_rls r 
where upper(airline) = upper(current_role()));
               
select current_role();
select * from vw_airline;
               
grant select on view vw_airline to role AIRASIA;
grant select on view vw_airline to role AIR_INDIA;
grant select on view vw_airline to role INDIGO;
grant select on view vw_airline to role VISTARA;
grant select on view vw_airline to role SPICEJET;
grant select on view vw_airline to role GO_FIRST;

    
  
CREATE OR REPLACE TABLE airline_RLS( row_id number, airline string);

INSERT INTO airline_RLS(row_id,airline)
SELECT row_id,airline FROM mytable2;
    
select * from airline_rls;
    
    
-----STEP 4 : VERIFY ROLES-----

use role AIRASIA;
use database SPRINT4;
use schema PUBLIC;
select * from vw_airline;



use role AIR_INDIA;
use database SPRINT4;
use schema PUBLIC;
select * from vw_airline;



use role INDIGO;
use database SPRINT4;
use schema PUBLIC;
select * from vw_airline;



use role VISTARA;
use database SPRINT4;
use schema PUBLIC;
select * from vw_airline;


use role GO_FIRST;
use database SPRINT4;
use schema PUBLIC;
select * from vw_airline;


use role SPICEJET;
use database SPRINT4;
use schema PUBLIC;
select * from vw_airline;

ALTER TASK MYTASK RESUME;
ALTER TASK MYTASK SUSPEND;

ALTER TASK TGT_MERGE RESUME;
ALTER TASK TGT_MERGE SUSPEND;

SHOW TASKS;