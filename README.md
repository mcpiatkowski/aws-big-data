# aws-big-data #
 
This is a collection of artefacts needed for Peex evaluation.

# Spark Setup #

Download Spark

https://spark.apache.org/downloads.html

### Environment Variables ###

SPARK_HOME: <spark_home_dir>  
Path: %SPARK_HOME%\bin

HADOOP_HOME: <hadoop_home_dir>  
Path: %HADOOP_HOME%\bin

## Windows Dependencies ##

To run spark scripts, we need to handle additional dependencies:
- hadoop winutils (windows only)

### Winutils ###

Spark was originally designed for Unix-like systems and relies on some Unix utilities that are not natively available on Windows.  
Winutils provides Windows-compatible versions of these Unix utilities, allowing Spark to run properly on Windows systems.

We need to copy winutils into the `hadoop/bin` directory.

https://github.com/steveloughran/winutils/tree/master

# Local Cluster Setup

We can set up a spark cluster in standalone mode locally.

The following commands are used on macOS with Spark installed via Homebrew.

Start Spark master:

`spark-class org.apache.spark.deploy.master.Master`

After the master node has started it will print the url of the UI.

You can find url of the master node in the UI. You will need it to start worker nodes.

Start worker nodes:

`spark-class org.apache.spark.deploy.worker.Worker spark://localhost:7077`

Submit a script to the cluster:

`spark-submit --master spark://192.168.1.2:7077 src/batch-processing/reading/necessary.py`

# MySQL Setup #

Steps to follow:
- Install MySQL using Homebrew: `brew install mysql`
- Start the MySQL service: `brew services start mysql`
- Set a root password: `mysql_secure_installation`
- Connect to MySQL: `mysql -u root -p`

New user:

`CREATE USER 'newuser'@'localhost' IDENTIFIED BY 'password';`

`GRANT ALL PRIVILEGES ON *.* TO 'newuser'@'localhost' WITH GRANT OPTION;`

`FLUSH PRIVILEGES;`

Changing password:

`ALTER USER 'user'@'localhost' IDENTIFIED BY 'new_password';`

`FLUSH PRIVILEGES;`

## JDBC Connection ##

Download the driver:

[MySQL JDBC Driver](https://dev.mysql.com/downloads/connector/j/)

There is additional configuration to be done and can be found inside the script. Make sure your database is running.

Read the data using Spark:

`spark-submit --jars mysql-connector-j-9.0.0.jar src/batch_processing/tv/optional_mysql_read.py`