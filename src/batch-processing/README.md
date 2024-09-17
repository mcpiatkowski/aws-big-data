# Reading structured and semi-structured data

Develop code for reading structured and semi-structured data
Build a Spark application that reads structured and semi-structured data from a dataset from references (alternatively, use a MySQL database).

It is necessary to:

- Retrieve all names of created_by with the status Cancelled.  
- Retrieve all origin_country with popularity higher than 5.0.  
- Retrieve all names of series with the number_of_episodes less than 100.

Optionally:

- Write code to establish a connection to a MySQL database using the JDBC driver.  
- Implement a query to retrieve data from a specific table in the MySQL database.  
- Join the retrieved data from the MySQL database with the processed JSON data based on a common key.  

NOTE: While performing the task, use a dataset provided in the references.

Acceptance criteria:

- Spark is used to read a JSON file from the local file system or a specified path.
- Relevant fields are extracted from the JSON data and transformed into a DataFrame or Dataset.
- Basic data aggregation operations are performed on the joined data, such as grouping by a specific column and calculating a count or sum.
- Results are displayed or written to a specified output location.