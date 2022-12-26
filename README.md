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


            spark-submit ETL-pyspark-Postgres-Redshift.py --properties spark.driver.extraClassPath=postgresql-42.2.5.jar,spark.jars.packages=org.postgresql:postgresql:42.2.5
                        
<img width="1511" alt="1" src="https://user-images.githubusercontent.com/28351206/209580982-b8cb5f0f-13fc-4435-af95-f147817cae36.png">

<img width="734" alt="2" src="https://user-images.githubusercontent.com/28351206/209581144-568d7f56-a4b0-49c3-aae5-cacf527fa384.png">

<img width="943" alt="3" src="https://user-images.githubusercontent.com/28351206/209581152-4d33b2c1-d430-4dd1-8f6e-fabf6e5ca9b4.png">

<img width="1511" alt="4" src="https://user-images.githubusercontent.com/28351206/209581157-c97cbde4-1133-416b-bed2-f64787056a28.png">

<img width="1511" alt="5" src="https://user-images.githubusercontent.com/28351206/209581164-6bd71c45-a954-433e-b0bb-61b7f481e606.png">



5. Now we can see the result in Amazon Redshift: 

<img width="1511" alt="6" src="https://user-images.githubusercontent.com/28351206/209581513-040f37d6-adcb-4469-ae61-5dedb909e697.png">

<img width="1511" alt="7" src="https://user-images.githubusercontent.com/28351206/209581520-cf1c6d64-6c2a-46cd-a1b4-bbc62864f54a.png">

