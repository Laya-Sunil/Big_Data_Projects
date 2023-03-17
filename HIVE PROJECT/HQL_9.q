## partioned and clustered table in HIVE

/*
Problem Statement 5:  Jhonny, from the finance department of Arizona(AZ), has 
requested a report that lists the total quantity of medicine each pharmacy in his 
state has prescribed that falls under Tax criteria I for treatments that took 
place in 2021. Assist Jhonny in generating the report. 
*/

select p.`pharmacyID`, p.pharmacyName, sum(k.quantity) 'Quantity'
from pharmacy p 
inner join (select `addressID` from address 
            where state='AZ') b
            on b.`addressID` = p.`addressID`
inner join keep k on k.`pharmacyID` = p.`pharmacyID`
inner join medicine m on m.`medicineID`=k.`medicineID`
inner join prescription pr on pr.`pharmacyID` = p.`pharmacyID`
inner join (select `treatmentID` from treatment 
            where year(date)='2021') a
            on a.`treatmentID`=pr.`treatmentID`
where m.`taxCriteria` = 'I'
group by p.`pharmacyName`, p.`pharmacyID`;

-- create a partioned and clustered table from treatment table
CREATE TABLE treatment_part_clus(
    treatmentID BIGINT,
    patientID BIGINT,
    date STRING,
    claimID BIGINT
)
COMMENT 'Treatment to claim ratio'
PARTITIONED BY (diseaseID STRING) CLUSTERED BY(patientID) INTO 2 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

-- insert the data from treatment table
insert overwrite table treatment_part_clus partition(diseaseID)
select treatmentID, patientID, date, claimID, diseaseID from treatment;

-- 
create external table pharmacy_quantity_2021(
    pharmacyID BIGINT,
    pharmacyName STRING,
    quantity BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/training/hive_9';


insert overwrite table pharmacy_quantity_2021
select p.`pharmacyID`, p.pharmacyName, sum(k.quantity) `Quantity`
from pharmacy p 
inner join (select `addressID` from address_part 
            where state='AZ') b
            on b.`addressID` = p.`addressID`
inner join keep k on k.`pharmacyID` = p.`pharmacyID`
inner join medicine m on m.`medicineID`=k.`medicineID`
inner join prescription pr on pr.`pharmacyID` = p.`pharmacyID`
inner join (select `treatmentID` from treatment_part 
            where year(date)='2021') a
            on a.`treatmentID`=pr.`treatmentID`
where m.`taxCriteria` = 'I'
group by p.`pharmacyName`, p.`pharmacyID`;

create table pharmacy_quantity_2021(
    pharmacyID BIGINT,
    pharmacyName varchar(40),
    quantity BIGINT
);

sqoop export \
--connect jdbc:mysql://localhost:3306/healthcare_out \
--username sqoop \
--password sqoop \
--table pharmacy_quantity_2021 \
--export-dir /user/training/hive_9/000000_0 \
--input-fields-terminated-by ',';