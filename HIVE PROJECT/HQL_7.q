/*
Problem Statement 3: Insurance companies want to assess the performance of their insurance
 plans. Generate a report that shows each insurance company's name with their most and 
 least claimed insurance plans.
*/

with cte_claim_count as (

    select ic.`companyName`, ip.`planName`,count(c.uin) `Total_claims`
    from insuranceplan ip join insurancecompany ic on ip.`companyID`= ic.`companyID`
    join claim c on ip.uin = c.uin
    group by ic.`companyName`, ip.`planName`
),
cte_least_most_claimed as (
    select *, max(c.Total_claims)over(partition by `companyName`) max_claim, 
            min(c.Total_claims)over(partition by `companyName`) min_claim
    from cte_claim_count c
)
select `companyName`,`planName`,Total_claims, `Least claimed` as status from cte_least_most_claimed
where Total_claims = min_claim
union 
select `companyName`,`planName`,Total_claims, `Most claimed` from cte_least_most_claimed
where Total_claims = max_claim
order by `companyName`;

-- create a view with query output
CREATE VIEW claim_summary_view AS
WITH cte_claim_count AS (
  SELECT ic.companyName, ip.planName, COUNT(c.uin) AS total_claims
  FROM insuranceplan ip
  JOIN insurancecompany ic ON ip.companyID = ic.companyID
  JOIN claim c ON ip.uin = c.uin
  GROUP BY ic.companyName, ip.planName
)
SELECT *, 
  MAX(total_claims) OVER (PARTITION BY companyName) AS max_claim, 
  MIN(total_claims) OVER (PARTITION BY companyName) AS min_claim
FROM cte_claim_count;

-- create external table
CREATE EXTERNAL TABLE claim_summary(
    company_name STRING,
    plan_name STRING,
    total_claims BIGINT,
    status STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/training/hive_7';

-- inert the query output to external table
INSERT OVERWRITE TABLE claim_summary
select `companyName`,`planName`,Total_claims, 'Least claimed' as status from claim_summary_view
where Total_claims = min_claim
union all
select `companyName`,`planName`,Total_claims, 'Most claimed' as status from claim_summary_view
where Total_claims = max_claim
order by `companyName`;

-- create mysql table
CREATE TABLE claim_summary(
    company_name varchar(60),
    plan_name varchar(50),
    total_claims BIGINT,
    status varchar(30)
);

-- export the external table data to client DB
sqoop export \
--connect jdbc:mysql://localhost:3306/healthcare_out \
--username sqoop \
--password sqoop \
--table claim_summary \
--export-dir /user/training/hive_7/000000_0 \
--input-fields-terminated-by ',';