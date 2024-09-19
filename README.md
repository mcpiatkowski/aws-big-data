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

## MySQL Setup ##

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