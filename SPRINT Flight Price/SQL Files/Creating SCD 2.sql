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