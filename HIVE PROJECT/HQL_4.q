## case in HIVE
/*
Problem Statement 1:  Jimmy, from the healthcare department, has requested a report that shows how the number of treatments each age category of patients has gone through in the year 2022. 
The age category is as follows, Children (00-14 years), Youth (15-24 years), Adults (25-64 years), and Seniors (65 years and over).
Assist Jimmy in generating the report. 

*/

CREATE EXTERNAL TABLE age_group_analysis(
    age_group STRING,
    total_treatments BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/training/hive_4';


select case 
			when timestampdiff(year, p.dob, curdate())>=0 and timestampdiff(year, dob, curdate())<15 then `Children(0-14)`
            when timestampdiff(year, p.dob, curdate())>=15 and timestampdiff(year, dob, curdate())<25 then `Youth(15-24)`
            when timestampdiff(year, p.dob, curdate())>=25 and timestampdiff(year, dob, curdate())<65 then `Adults(25-64)`
            else `Seniors(>65 years)`
		end as `Age_group`, count(t.treatmentid) `Total Treatments`
 from patient p right join treatment t on p.patientid = t.patientid
 group by `Age_group`;


INSERT OVERWRITE TABLE age_group_analysis
SELECT
  CASE
    WHEN datediff(current_date(), p.dob) >= 0 AND datediff(current_date(), p.dob) < 15*365 THEN 'Children(0-14)'
    WHEN datediff(current_date(), p.dob) >= 15*365 AND datediff(current_date(), p.dob) < 25*365 THEN 'Youth(15-24)'
    WHEN datediff(current_date(), p.dob) >= 25*365 AND datediff(current_date(), p.dob) < 65*365 THEN 'Adults(25-64)'
    ELSE 'Seniors(>65 years)'
  END AS Age_group,
  COUNT(t.treatmentid) AS Total_Treatments
FROM
  patient p
  RIGHT JOIN treatment t ON p.patientid = t.patientid
GROUP BY
  CASE
    WHEN datediff(current_date(), p.dob) >= 0 AND datediff(current_date(), p.dob) < 15*365 THEN 'Children(0-14)'
    WHEN datediff(current_date(), p.dob) >= 15*365 AND datediff(current_date(), p.dob) < 25*365 THEN 'Youth(15-24)'
    WHEN datediff(current_date(), p.dob) >= 25*365 AND datediff(current_date(), p.dob) < 65*365 THEN 'Adults(25-64)'
    ELSE 'Seniors(>65 years)'
  END;

-- create mysql table 
create table age_group_analysis(
    age_group varchar(30),
    total_treatments BIGINT
);

-- sqoop export to mysql

sqoop export \
--connect jdbc:mysql://localhost:3306/healthcare_out \
--username sqoop \
--password sqoop \
--table age_group_analysis \
--export-dir /user/training/hive_4/000000_0 \
--input-fields-terminated-by ',';