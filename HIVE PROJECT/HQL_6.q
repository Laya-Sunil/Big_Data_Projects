## Subquery with window function in HIVE

/*
Problem Statement 2: The State of Alabama (AL) is trying to manage its healthcare resources
 more efficiently. For each city in their state, they need to identify the disease for 
which the maximum number of patients have gone for treatment.
Assist the state for this purpose.
Note: The state of Alabama is represented as AL in Address Table.
*/
-- using subquery and window function
select city, `diseaseName`, cnt_disease from
(
	select *, dense_rank()over(partition by city order by cnt_disease desc) rnk
	from (
			select distinct a.city, d.`diseaseName`, count(*)over(partition by t.`diseaseID`, a.city)cnt_disease
			from treatment t inner join disease d on t.`diseaseID` = d.`diseaseID`
			inner join patient p on p.`patientID` = t.`patientID`
			inner join person pr on pr.`personID` = p.`patientID`
			inner join address a on a.`addressID` = pr.`addressID`
			where a.state = 'AL'
		)a
)b
where rnk = 1;

-- equivalent Hive QL
SELECT city, diseaseName, cnt_disease
FROM (
  SELECT *, dense_rank() OVER (PARTITION BY city ORDER BY cnt_disease DESC) rnk
  FROM (
    SELECT DISTINCT a.city, d.diseaseName, count(*) OVER (PARTITION BY d.diseaseName, a.city) cnt_disease
    FROM treatment t
    JOIN disease d ON t.diseaseID = d.diseaseID
    JOIN patient p ON p.patientID = t.patientID
    JOIN person pr ON pr.personID = p.patientID
    JOIN address a ON a.addressID = pr.addressID
    WHERE a.state = 'AL'
  ) a
) b
WHERE rnk = 1;






-- create hive external table
CREATE EXTERNAL TABLE city_wise_disease_count(
    city STRING,
    disease_name STRING,
    count INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/training/hive_6';

-- insert the query output to external table
INSERT OVERWRITE TABLE city_wise_disease_count
SELECT city, diseaseName, cnt_disease
FROM (
  SELECT *, dense_rank() OVER (PARTITION BY city ORDER BY cnt_disease DESC) rnk
  FROM (
    SELECT DISTINCT a.city, d.diseaseName, count(*) OVER (PARTITION BY d.diseaseName, a.city) cnt_disease
    FROM treatment t
    JOIN disease d ON t.diseaseID = d.diseaseID
    JOIN patient p ON p.patientID = t.patientID
    JOIN person pr ON pr.personID = p.patientID
    JOIN address a ON a.addressID = pr.addressID
    WHERE a.state = 'AL'
  ) a
) b
WHERE rnk = 1;


-- create mysql table
CREATE TABLE city_wise_disease_count(
    city varchar(30),
    disease_name varchar(40),
    count INT
);

-- export to client DB
sqoop export \
--connect jdbc:mysql://localhost:3306/healthcare_out \
--username sqoop \
--password sqoop \
--table city_wise_disease_count \
--export-dir '/user/training/hive_6' \
--input-fields-terminated-by ',';


