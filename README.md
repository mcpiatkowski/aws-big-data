# aws-big-data #
 
This is a collection of artefacts needed for Peex evaluation.

## Dependencies ##

To run spark scripts, we need to handle additional dependencies:
- hadoop winutils (windows only)
- mysql connector

### Winutils ###

Spark was originally designed for Unix-like systems and relies on some Unix utilities that are not natively available on Windows.  
Winutils provides Windows-compatible versions of these Unix utilities, allowing Spark to run properly on Windows systems.

We need to copy winutils into the `hadoop/bin` directory.

https://github.com/steveloughran/winutils/tree/master

### Spark Download ###

https://spark.apache.org/downloads.html

### Environment Variables ###

SPARK_HOME: <spark_home_dir>  
Path: %SPARK_HOME%\bin

HADOOP_HOME: <hadoop_home_dir>  
Path: %HADOOP_HOME%\bin
