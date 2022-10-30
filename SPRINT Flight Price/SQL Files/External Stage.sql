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