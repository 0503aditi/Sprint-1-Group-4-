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