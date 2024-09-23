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

[All TV Series Details Dataset](https://www.kaggle.com/datasets/bourdier/all-tv-series-details-dataset)

# Develop code for a simple Spark application

Develop a simple Spark application that reads data from a dataset from references, performs transformations, and saves the result to a Parquet file.

It is necessary to identify:

- How many cali_ckn pizzas were ordered on 2015-01-04?
- What ingredients does the pizza ordered on 2015-01-02 at 18:27:50 have?
- What is the most sold category of pizza between 2015-01-01 and 2015-01-08?

NOTE: While performing the task, use a dataset provided in the references.

Acceptance criteria:

- Spark session is set up and configured to run in local mode.
- A CSV file containing sample data is loaded into a DataFrame.
- Transformations, such as filtering, aggregation, or column manipulation, are applied to the DataFrame.
- Action on the transformed DataFrame is triggered to perform the computation and collect the result.
- Result is saved to a Parquet file.
- Application handles exceptions or errors and provides appropriate error messages or logging.
- The data is correctly processed and stored in a Parquet file.

[Pizza Sales Dataset](https://www.kaggle.com/datasets/ylenialongo/pizza-sales)

# Cluster mode

Run a Spark job in the cluster mode using the Standalone deploy mode.

Set up a Spark cluster and submit a Spark application to the cluster for execution that reads data from the dataset.

It is necessary to:

- Count the total number of words that start with abs.
- Count the total number of words that have the third letter o.
- Change the combination of letters ou with uou in all words that end with the letter s.

NOTE: While performing the task, use a dataset provided in the references.

Acceptance criteria:

- A Spark cluster is set up in Standalone deploy mode.
- Spark application that achieves a basic data processing task, such as word count or data aggregation, is written.
- The Spark application is packaged into a JAR file.
- The JAR file is submitted to the Spark cluster using the spark-submit command.
- The expected output is produced, and the Spark job is completed successfully.

[English Words Dataset](https://github.com/dwyl/english-words/blob/master/words.txt)
