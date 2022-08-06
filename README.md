# Sparkify DataLake Project using AWS EMR

In this project, the goal is to create a database for Sparkify startup using two datasets log_data and songs_data stored in an S3 bucket. This is achieved by building an ETL pipeline and using Spark on AWS EMR cluster to process the data. In the ETL script, the input data is downloaded from an S3 bucket, transformed and loaded back into the s3 bucket. The database generated from the process is designed using STAR schema and it includes one fact table - songplays_table and four dimensional tables songs_table, artists_table, time_table and users_table. 


###  How to run the Python script on EMR cluster
- Remove code on configparser/credentials
- Copy the Python file to an s3 bucket
  - <code>aws s3 cp <script_dir> <s3_bucket_link> </code>
- Create an EMR instance
- Connect to the EMR using SSH or an SSH client e.g Putty
- Copy the script from S3 to the EMR
  - <code>aws s3 cp <s3_bucket_link> etl.py</code>
- Run the script
  - <code>spark-submit etl.py</code> 

### How to run the Python script using Jupyter Notebook (local)
- Add AWS credentials to the dl.cfg file
- Ensure you have Spark and Hadoop installed in your machine
- Use configparser to import the credentials
- Start spark session and run shells individually

###  Summary of files

###### etl.py

* creates a spark session which processes the datasets
* extracts songs_data from s3 bucket, creates song_table and artists_table from it
* extracts log_data, generates time_table and users_table
* joins processed log_data to songs_data to create songplays_table
* loads the table in parquet format, partitions tables songs_table, time_table and songplays_table.    

###### etl.ipynb
* similar to etl.py file but not modular
* creates a spark session
* links to the aws using the credentials
* used to debug and analyse the etl process

###### dl.cfg
* contains the aws access id and access key 

### Future modifications
 - apply other data processing methods
 - use map reduce techniques