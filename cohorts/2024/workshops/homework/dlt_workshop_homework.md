### Question 1. What is the sum of the outputs of the generator for limit = 5?
```Python
def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1

# Example usage:
limit = 5
generator = square_root_generator(limit)

print(sum(square_root_generator(limit)))
```

### Question 2. What is the 13th number yielded by the generator?
```Python
def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1

# Example usage:
limit = 13
generator = square_root_generator(limit)

print(list(square_root_generator(limit))[-1])
```

### Question 3. Append the 2 generators. After correctly appending the data, calculate the sum of all ages of people.
```Python
import dlt
import duckdb


def people_1():
    for i in range(1, 6):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 25 + i, "City": "City_A"}

for person in people_1():
    print(person)


# define the connection to load to.
pipeline = dlt.pipeline(destination='duckdb', dataset_name='people_generators')

# run the pipeline with default settings, and capture the outcome
info = pipeline.run(people_1,
                                        table_name="people",
                                        write_disposition="replace")


conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")

people_1 = conn.sql("SELECT SUM(age) FROM people_generators.people").df()
display(people_1)


def people_2():
    for i in range(3, 9):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 30 + i, "City": "City_B", "Occupation": f"Job_{i}"}


for person in people_2():
    print(person)

# run the pipeline with default settings, and capture the outcome
info = pipeline.run(people_2,
                    table_name="people",
                    write_disposition="append")

people_2 = conn.sql("SELECT SUM(age) FROM people_generators.people").df()
display(people_2)
```

### Question 4. Merge the 2 generators using the ID column. Calculate the sum of ages of all the people loaded as described above.
```Python
import dlt
import duckdb


def people_1():
    for i in range(1, 6):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 25 + i, "City": "City_A"}

for person in people_1():
    print(person)


# define the connection to load to.
pipeline = dlt.pipeline(destination='duckdb', dataset_name='people_generators')

# run the pipeline with default settings, and capture the outcome
info = pipeline.run(people_1,
                    table_name="people",
                    write_disposition="replace",
                    primary_key="ID")


conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")

people_1 = conn.sql("SELECT SUM(age) FROM people_generators.people").df()
display(people_1)


def people_2():
    for i in range(3, 9):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 30 + i, "City": "City_B", "Occupation": f"Job_{i}"}


for person in people_2():
    print(person)

# run the pipeline with default settings, and capture the outcome
info = pipeline.run(people_2,
                    table_name="people",
                    write_disposition="merge",
                    primary_key="ID")

people_2 = conn.sql("SELECT SUM(age) FROM people_generators.people").df()
display(people_2)
```
