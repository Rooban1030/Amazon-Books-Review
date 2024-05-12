# Databricks notebook source
# MAGIC %md
# MAGIC Medallion Architecture For Books Details

# COMMAND ----------

# Reading data from Books_Details as a Stream.

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "/mnt/amazonbooksreviews/Transformed Data/Books_Details_cleaned")
    .load("/mnt/amazonbooksreviews/Transformed Data/Books_Details_cleaned")
    .createOrReplaceTempView("Books_Details_Raw"))

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC --Creating a Temporary view 
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Books_Details_temp AS (
# MAGIC   SELECT *
# MAGIC   FROM Books_Details_Raw
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Books_Details_temp

# COMMAND ----------

(spark.table("Books_Details_temp")
      .writeStream
      .format("delta")
      .option("checkpointLocation", "/mnt/amazonbooksreviews/Transformed Data/Books_Details_Bronze")
      .outputMode("append")
      .table("Books_Details_bronze"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM Books_Details_bronze;

# COMMAND ----------

# Reading data from Books_Reviews as a Stream.

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "/mnt/amazonbooksreviews/Transformed Data/Books_ratings_cleaned")
    .load("/mnt/amazonbooksreviews/Transformed Data/Books_ratings_cleaned")
    .createOrReplaceTempView("Books_ratings_Raw"))

# COMMAND ----------

# MAGIC %sql
# MAGIC --Creating a Temporary view 
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Books_Reviews_temp AS (
# MAGIC   SELECT *
# MAGIC   FROM Books_ratings_Raw )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Books_Reviews_temp

# COMMAND ----------

(spark.table("Books_Reviews_temp")
      .writeStream
      .format("delta")
      .option("checkpointLocation", "/mnt/amazonbooksreviews/Transformed Data/Books_reviews_Bronze")
      .outputMode("append")
      .table("Books_reviews_bronze"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM Books_reviews_bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED Books_reviews_bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED Books_Details_bronze;

# COMMAND ----------

# reading data from bronze table as a stream and creating a temp view

(spark.readStream
  .table("Books_Details_bronze")
  .createOrReplaceTempView("Books_Details_bronze_temp"))

# COMMAND ----------

(spark.readStream
  .table("Books_reviews_bronze")
  .createOrReplaceTempView("Books_reviews_bronze_temp"))


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --Creating Temp View for silver table
# MAGIC
# MAGIC CREATE OR REPLACE Temporary View Amazon_books_review_temp AS (
# MAGIC   SELECT b.Title, authors, publisher, YEAR(to_date(Year_of_publish)) as Year_of_publish , categories, Ratings_Count, User_id, profileName, Review_helpfulness, review_score, review_Time
# MAGIC   FROM Books_Details_bronze_temp a
# MAGIC   INNER JOIN Books_reviews_bronze_temp b
# MAGIC   ON a.Title = b.Title
# MAGIC   WHERE YEAR(review_Time) > 2010)

# COMMAND ----------

# Loading Data into Silver table

(spark.table("Amazon_books_review_temp")
  .writeStream
  .format("delta")
  .option("checkpointLocation", "/mnt/amazonbooksreviews/Transformed Data/Amazon_books_reviews_silver")
  .outputMode("append")
  .table("Amazon_Books_Reviews_Silver"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM Amazon_Books_Reviews_Silver

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED Amazon_Books_Reviews_Silver

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Amazon_Books_Reviews_Silver

# COMMAND ----------

# Reading Silver table as a Stream

(spark.readStream
  .table("Amazon_Books_Reviews_Silver")
  .createOrReplaceTempView("Amazon_Books_Reviews_Silver_Temp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- creating gold temp view with some aggregations
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Amazon_Books_Reviews_Gold_Temp AS (
# MAGIC   SELECT title, Year_of_publish, categories, Count(user_id) as users_count
# MAGIC   FROM Amazon_Books_Reviews_Silver_Temp
# MAGIC   GROUP BY title, Year_of_publish, categories
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Amazon_Books_Reviews_Gold_Temp

# COMMAND ----------

# Creating Gold delta table from temp view.
# using 'complete' as outputMode since streaming pipeline does not allow aggregations on 'append' mode.

(spark.table("Amazon_Books_Reviews_Gold_Temp")
  .writeStream
  .format("delta")
  .option("checkpointLocation", "/mnt/amazonbooksreviews/Transformed Data/books_users_count")
  .outputMode("complete")
  .table("gold__books_users_count"))

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED gold__books_users_count

# COMMAND ----------

# Writing gold table data to Azure data Lake storage

spark.read.format("delta").load("dbfs:/user/hive/warehouse/gold__books_users_count").write \
    .format("delta") \
    .mode("overwrite") \
    .save("/mnt/amazonbooksreviews/Transformed Data/Gold_Table/books_users_count")

# COMMAND ----------

# Writing Silver table data to Azure data Lake storage

spark.read.format("delta").load("dbfs:/user/hive/warehouse/amazon_books_reviews_silver")\
    .write \
    .format("delta") \
    .mode("overwrite") \
    .save("/mnt/amazonbooksreviews/Transformed Data/Silver_Table/Amazon_books_reviews_silver")
