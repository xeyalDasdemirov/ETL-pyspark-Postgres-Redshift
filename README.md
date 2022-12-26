# ETL-pyspark-Postgres-Redshift
ETL with Pyspark from Postgres to Amazon Redshift in EMR
Here I will describe ETL process from Amazon RDS Postgres to Amazon Redshift to create some fact tables with Spark SQl scripts: 


1. Firstly we need to create a sample postgres DB. I found dvdrental DB and restored it in my Amazon RDS Postgres DB : https://www.postgresqltutorial.com/postgresql-getting-started/postgresql-sample-database/


2. Create an EMR cluster with Spark installation. And connect to your EMR cluster from your Terminal or use Amazon Cloud9 terminal: 

2.1 Dowload Postgres driver to connect to Postgres DB in EMR cluster. wget https://jdbc.postgresql.org/download/postgresql-42.2.5.jar 
2.2 Install boto3 in EMR: pip install boto3
2.3 Add access to EMR cluster in RDS security group.
2.4 Add access to EMR cluster in Amazon Redshift security group.


3. 
