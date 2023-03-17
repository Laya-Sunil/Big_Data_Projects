/*
Problem Statement 5:  An Insurance company wants a state wise report of the treatments to 
claim ratio between 1st April 2021 and 31st March 2022 (days both included). Assist 
them to create such a report.
*/
select a.state, count(t.`treatmentID`)/count(t.`claimID`) as ratio
from address a inner join person p on a.`addressID` = p.`addressID`
inner join patient pt on p.`personID` = pt.`patientID`
inner join treatment t on pt.`patientID` = t.`patientID`
where t.date between '2021-04-01' and '2022-03-31'
group by a.state;

-- equivalent HIVE QL
select a.state, count(t.`treatmentID`)/count(t.`claimID`) as ratio
from address_part a inner join person p on a.`addressID` = p.`addressID`
inner join patient pt on p.`personID` = pt.`patientID`
inner join treatment t on pt.`patientID` = t.`patientID`
where cast(t.date as date) between '2021-04-01' and '2022-03-31'
group by a.state;

-- create partitioned table to store treatment data
CREATE TABLE treatment_part(
    treatmentID BIGINT,
    patientID BIGINT,
    date STRING,
    claimID BIGINT
)
COMMENT 'Treatment to claim ratio'
PARTITIONED BY (diseaseID STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

-- insert the data
insert overwrite table treatment_part partition(diseaseID)
select treatmentID, patientID, date, claimID,  diseaseID from treatment;

-- create external table to store above query output
CREATE EXTERNAL TABLE state_wise_treatment_claim(
    state STRING,
    treatment_claim_ratio FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/training/hive_8';

-- insert the query data
INSERT OVERWRITE TABLE state_wise_treatment_claim
select a.state, count(t.`treatmentID`)/count(t.`claimID`) as ratio
from address_part a inner join person p on a.`addressID` = p.`addressID`
inner join patient pt on p.`personID` = pt.`patientID`
inner join treatment_part t on pt.`patientID` = t.`patientID`
where cast(t.date as date) between '2021-04-01' and '2022-03-31'
group by a.state;

-- create a table to recieve data in mysql
CREATE  TABLE state_wise_treatment_claim(
    state varchar(20),
    treatment_claim_ratio FLOAT
);

-- export the output data to client DB
sqoop export \
--connect jdbc:mysql://localhost:3306/healthcare_out \
--username sqoop \
--password sqoop \
--table state_wise_treatment_claim \
--export-dir /user/training/hive_8/000000_0 \
--input-fields-terminated-by ',';

