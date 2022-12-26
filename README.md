# ETL-pyspark-Postgres-Redshift
ETL with Pyspark from Postgres to Amazon Redshift in EMR
Here I will describe ETL process from Amazon RDS Postgres to Amazon Redshift to create some fact tables with Spark SQl scripts: 


1. Firstly we need to create a sample postgres DB. I found dvdrental DB and restored it in my Amazon RDS Postgres DB : https://www.postgresqltutorial.com/postgresql-getting-started/postgresql-sample-database/


2. Create Amazon Redshift cluster to load data from Postgres to redshift. 


3. Create an EMR cluster with Spark installation. And connect to your EMR cluster from your Terminal or use Amazon Cloud9 terminal: 

      3.1 Dowload Postgres driver to connect to Postgres DB in EMR cluster. wget https://jdbc.postgresql.org/download/postgresql-42.2.5.jar 

      3.2 Install boto3 in EMR: pip install boto3

      3.3 Add access to EMR cluster in RDS security group.

      3.4 Add access to EMR cluster in Amazon Redshift security group.



4. Create ETL-pyspark-Postgres-Redshift.py sfile and run spark-submit command in the EMR:

      4.1  Update the credentials for DBs and S3 bucket for tempdir in the ETL-pyspark-Postgres-Redshift.py, copy paste script in the EMR using: 
           nano ETL-pyspark-Postgres-Redshift.py

      4.2. Run the below commoan in the EMR: 
                        
      spark-submit ETL-pyspark-Postgres-Redshift.py --properties spark.driver.extraClassPath=postgresql42.2.5.jar,spark.jars.packages=org.postgresql:postgresql:42.2.5
