```SQL
-- Create an exterank table
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-course-412717.ny_taxi_eu.external_green_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://mage-zoomcamp-zhoogle/green/green_tripdata_2022-*.parquet']
);

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE dtc-de-course-412717.ny_taxi_eu.green_tripdata_non_partitoned AS
SELECT * FROM dtc-de-course-412717.ny_taxi_eu.external_green_tripdata;


SELECT count(*)
FROM dtc-de-course-412717.ny_taxi_eu.green_tripdata_non_partitoned;
--840402


SELECT distinct(PULocationID)
FROM dtc-de-course-412717.ny_taxi_eu.external_green_tripdata;
--0

SELECT distinct(PULocationID)
FROM dtc-de-course-412717.ny_taxi_eu.green_tripdata_non_partitoned;
--6.41 MB

SELECT count(*)
FROM dtc-de-course-412717.ny_taxi_eu.green_tripdata_non_partitoned
WHERE fare_amount = 0;
--1622

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE dtc-de-course-412717.ny_taxi_eu.green_tripdata_partitoned
PARTITION BY
  DATE(lpep_pickup_datetime)
  CLUSTER BY PULocationID AS
SELECT * FROM dtc-de-course-412717.ny_taxi_eu.external_green_tripdata;

SELECT distinct(PULocationID)
FROM dtc-de-course-412717.ny_taxi_eu.green_tripdata_partitoned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';
--1.12 MB

SELECT distinct(PULocationID)
FROM dtc-de-course-412717.ny_taxi_eu.green_tripdata_non_partitoned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';
--12.82
```