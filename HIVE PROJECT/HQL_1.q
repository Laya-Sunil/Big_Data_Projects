sqoop import-all-tables \
--connect jdbc:mysql://localhost:3306/healthcare \
--username sqoop \
--password sqoop \
--hive-import \
--m 1

/*
Problem Statement 4: The Healthcare department wants a report about the inventory of pharmacies. Generate a report 
on their behalf that shows how many units of medicine each pharmacy has in their inventory, the total maximum retail 
price of those medicines, and the total price of all the medicines after discount. 
Note: discount field in keep signifies the percentage of discount on the maximum price.

*/

 select p.pharmacyid, p.pharmacyname, sum(quantity) 'Total stock', sum(maxprice) 'Total maxprice', 
			round(sum(maxprice*(discount/100)),2) as 'Discounted Price'
 from pharmacy p inner join keep k on p.pharmacyid = k.pharmacyid
 inner join medicine m on m.medicineid = k.medicineid
 group by pharmacyid, pharmacyname;

CREATE EXTERNAL TABLE pharmacy_summary (
    pharmacyid INT,
    pharmacyname STRING,
    total_stock INT,
    total_maxprice FLOAT,
    discounted_price FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/training/hive/';

INSERT OVERWRITE TABLE pharmacy_summary
SELECT 
    p.pharmacyid, 
    p.pharmacyname, 
    SUM(k.quantity) AS `Total_stock`, 
    SUM(m.maxprice) AS `Total_maxprice`, 
    ROUND(SUM(m.maxprice*(k.discount/100)),2) AS `Discounted_Price`
FROM 
    pharmacy p 
    JOIN keep k ON p.pharmacyid = k.pharmacyid
    JOIN medicine m ON m.medicineid = k.medicineid
GROUP BY 
    p.pharmacyid, 
    p.pharmacyname;

-- create mysql table
CREATE TABLE pharmacy_summary (
    pharmacyid INT,
    pharmacyname varchar(50),
    total_stock INT,
    total_maxprice FLOAT,
    discounted_price FLOAT
);

sqoop export \
--connect jdbc:mysql://localhost:3306/healthcare \
--username sqoop \
--password sqoop \
--table pharmacy_summary \
--export-dir /user/training/hive/000000_0 \
--input-fields-terminated-by ',';


