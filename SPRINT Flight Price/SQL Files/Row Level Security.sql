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