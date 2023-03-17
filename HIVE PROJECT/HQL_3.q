## CTE in HIVE

 /*
 Problem Statement 3: Jacob, from insurance management, has noticed that insurance claims are not made for all the treatments.
 He also wants to figure out if the gender of the patient has any impact on the insurance claim. Assist Jacob in this situation 
 by generating a report that finds for each gender the number of treatments, number of claims, and treatment-to-claim ratio. 
 And notice if there is a significant difference between the treatment-to-claim ratio of male and female patients.
 */
with cte as (
	 select pr.gender, count(t.treatmentid) 'treatments', count(t.claimid) 'claims'
	 from treatment t left join claim c on t.claimid=c.claimid
     inner join patient p on p.patientid = t.patientid
	 inner join person pr on pr.personid = p.patientid
	 group by pr.gender
     )
select *, treatments/claims as 'treatment-to-claim ratio'
from cte;


CREATE EXTERNAL TABLE treatment_to_claim_ratio (
  gender STRING,
  treatments BIGINT,
  claims BIGINT,
  treatment_to_claim_ratio DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/training/hive_3';


CREATE TEMPORARY TABLE cte_results AS
WITH cte AS (
  SELECT
    pr.gender,
    COUNT(t.treatmentid) AS treatments,
    COUNT(t.claimid) AS claims
  FROM
    treatment t
    LEFT JOIN claim c ON t.claimid=c.claimid
    JOIN patient p ON p.patientid=t.patientid
    JOIN person pr ON pr.personid=p.patientid
  GROUP BY pr.gender
)
SELECT
  cte.gender,
  cte.treatments,
  cte.claims,
  cte.treatments / cte.claims AS `treatment-to-claim ratio`
FROM
  cte;

INSERT OVERWRITE TABLE treatment_claim_ratio
SELECT * FROM cte_results;

DROP TABLE cte_results;

-- OR

CREATE EXTERNAL TABLE treatment_claim_ratio (
  gender STRING,
  treatments BIGINT,
  claims BIGINT,
  treatment_to_claim_ratio DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/training/hive_3';

INSERT OVERWRITE TABLE treatment_claim_ratio
SELECT
  cte.gender,
  cte.treatments,
  cte.claims,
  cte.treatments / cte.claims AS `treatment-to-claim-ratio`
FROM
  (SELECT
     pr.gender,
     COUNT(t.treatmentid) AS treatments,
     COUNT(t.claimid) AS claims
   FROM
     treatment t
     LEFT OUTER JOIN claim c ON t.claimid=c.claimid
     JOIN patient p ON p.patientid=t.patientid
     JOIN person pr ON pr.personid=p.patientid
   GROUP BY pr.gender
  ) cte;

-- create mysql table
CREATE TABLE treatment_claim_ratio (
  gender varchar(30),
  treatments BIGINT,
  claims BIGINT,
  treatment_to_claim_ratio DOUBLE
);

--- export
sqoop export --connect jdbc:mysql://localhost:3306/healthcare_out \
--username sqoop \
--password sqoop \
--table treatment_claim_ratio \
--export-dir /user/training/hive_3/000000_0 \
--input-fields-terminated-by ',';
