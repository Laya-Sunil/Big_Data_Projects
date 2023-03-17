## partioning in HIVE

-- partition the treatments table on year
CREATE EXTERNAL TABLE IF NOT EXISTS address_part (addressid int,address1 string,city string,zip int)
COMMENT 'Address_partition'
PARTITIONED BY (state string) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n';

insert overwrite table address_part partition(state) select addressid ,address1 ,city,zip,state from address;

-- total number of registered patients in each city
select ap.state, count(pt.patientid)
from address_part ap join person p on ap.addressid=p.addressid 
join patient pt on pt.patientid=p.personid
group by ap.state;

CREATE EXTERNAL TABLE state_wise_patients(
    state string,
    count int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

INSERT OVERWRITE TABLE state_wise_patients
select ap.state, count(pt.patientid)
from address_part ap join person p on ap.addressid=p.addressid 
join patient pt on pt.patientid=p.personid
group by ap.state;

-- create mysql table
CREATE  TABLE state_wise_patients(
    state string,
    count int
);

sqoop export \
--connect jdbc:mysql://localhost:3306/healthcare_out \
--username sqoop \
--password sqoop \
--table state_wise_patients \
--export-dir /user/hive/warehouse/state_wise_patients \
--input-fields-terminated-by ',';
