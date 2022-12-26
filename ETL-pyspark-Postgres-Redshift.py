import boto3

import pyspark
 
from pyspark.sql import SparkSession
 
# creating sparksession and giving an app name
spark = SparkSession.builder.config("spark.jars", "postgresql-42.2.5.jar") \
.master("local").appName("PySpark_Postgres_test").getOrCreate()
 

df_payment_fact = spark.read.jdbc(url = "jdbc:postgresql://host:5432/dvdrental?ssl=false", 
                     table = "(select first_name, last_name, amount, a.phone, a.address, ct.city, cn.country from payment p \
                               inner join customer c on p.customer_id = c.customer_id \
                               inner join address a on a.address_id = c.address_id \
                               inner join city ct on ct.city_id = a.city_id \
                               inner join country cn on  cn.country_id = ct.country_id) AS payment_fact",
                     properties={"user": "postgres", "password": "password"}).createOrReplaceTempView('payment_fact')

df_nicer_but_slower_film_list = spark.read.jdbc(url = "jdbc:postgresql://host:5432/dvdrental?ssl=false", 
                     table = "(select * from nicer_but_slower_film_list) AS nicer_but_slower_film_list",
                     properties={"user": "postgres", "password": "password"}).createOrReplaceTempView('nicer_but_slower_film_list')


df_payment_fact = spark.sql("select * from payment_fact")
df_nicer_but_slower_film_list = spark.sql("select * from nicer_but_slower_film_list")


df_payment_fact.show()
df_nicer_but_slower_film_list.show()

df_payment_fact.write \
  .format("io.github.spark_redshift_community.spark.redshift") \
  .option("url", "jdbc:redshift://host:5439/dev?user=username&password=password") \
  .option("dbtable", "payment_fact") \
  .option("forward_spark_s3_credentials", "true") \
  .option("tempdir", "s3://your_bucket_name/logs/") \
  .mode("error").save()

df_nicer_but_slower_film_list.write \
  .format("io.github.spark_redshift_community.spark.redshift") \
  .option("url", "jdbc:redshift://host:5439/dev?user=username&password=password") \
  .option("dbtable", "nicer_but_slower_film_list") \
  .option("forward_spark_s3_credentials", "true") \
  .option("tempdir", "s3://your_bucket_name/logs/") \
  .mode("error").save()


