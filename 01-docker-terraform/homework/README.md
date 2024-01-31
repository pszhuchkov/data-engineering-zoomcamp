**Question 1. Knowing docker tags**
```bash
✗ docker run --help | grep "Automatically remove the container when it exits"
--rm Automatically remove the container when it exits
```

**Question 2. Understanding docker first run**
```bash
✗ docker run -it python:3.9 bash                                             
root@df4ef47b656e:/# pip list | grep wheel
wheel      0.42.0
```

**Question 3. Count records**
```SQL
SELECT count(*) FROM public.yellow_taxi_data
WHERE lpep_pickup_datetime::date = '2019-09-18'
AND lpep_dropoff_datetime::date = '2019-09-18';
--15612
```

**Question 4. Largest trip for each day**
```SQL
SELECT lpep_pickup_datetime::date
FROM public.yellow_taxi_data
WHERE trip_distance = (SELECT MAX(trip_distance) FROM public.yellow_taxi_data);
--2019-09-26
```

**Question 5. Three biggest pick up Boroughs**
```SQL
SELECT z."Borough", SUM(t."total_amount")
FROM public.yellow_taxi_data t, public.zones z
WHERE t."PULocationID" = z."LocationID"
AND lpep_pickup_datetime::date = '2019-09-18'
GROUP BY 1
HAVING SUM(t."total_amount") > 50000
ORDER BY 2 DESC
LIMIT 3;
--Brooklyn, Manhattan, Queens
```

**Question 6. Largest tip**
```SQL
SELECT d."Zone", t."tip_amount"
FROM public.yellow_taxi_data t, public.zones pu, public.zones d
WHERE t."PULocationID" = pu."LocationID"
AND t."DOLocationID" = d."LocationID"
AND pu."Zone" = 'Astoria'
ORDER BY t."tip_amount" DESC
LIMIT 1;
--JFK Airport
```

**Question 7. Creating Resources**
1. [basic](./terraform_basic)
2. [with variable](./terraform_with_variables)