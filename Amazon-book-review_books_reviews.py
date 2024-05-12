# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark import *

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "#{Application_id}",
"fs.azure.account.oauth2.client.secret": '#{Secret_key}',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/#{Tenant_key}/oauth2/token"}

dbutils.fs.mount(
source = "abfss://amzon-books-review@amazonbooksreview.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/amazonbooksreviews",
extra_configs = configs)

# COMMAND ----------

# MAGIC %fs
ls "/mnt/amazonbooksreviews"

# COMMAND ----------

#reading Books_rating file from Azure Storage Account

Books_ratings_raw = spark.read.format("csv").option("header" ,True).option("inferschema",True).load("/mnt/amazonbooksreviews/Raw Data/Books_rating.csv")
Books_ratings_raw.count()

# COMMAND ----------

Books_ratings_raw.display()

# COMMAND ----------

# the column "reviews/helpfulness" is not consistent so will convert into percentage for better understanding.

# splitting the column based on '/' to seperate the numerator and denominator values.

Books_ratings_cleaned_1 = Books_ratings_raw.withColumn("Reviews", split(Books_ratings_raw["review/helpfulness"], "/")[0]) \
             .withColumn("Helpfulness", split(Books_ratings_raw["review/helpfulness"], "/")[1])

# casting the seperate columns to FloatType to remove the string values.

Books_ratings_cleaned_1 = Books_ratings_cleaned_1.withColumn("Reviews",Books_ratings_cleaned_1.Reviews.cast(FloatType()))
Books_ratings_cleaned_1 = Books_ratings_cleaned_1.withColumn("Helpfulness",Books_ratings_cleaned_1.Helpfulness.cast(FloatType()))

# Taking the Percentage

Books_ratings_cleaned_1 = Books_ratings_cleaned_1.withColumn("Review_helpfulness",round(col("Reviews")/col("Helpfulness")*100,2))


# COMMAND ----------

Books_ratings_cleaned_1.select("review/helpfulness","Review_helpfulness").show()

# COMMAND ----------

Books_ratings_cleaned_1.display()

# COMMAND ----------

# Casting "review/score" as FloatType to remove bad data

Books_ratings_cleaned_2 = Books_ratings_cleaned_1.withColumn("review/score",Books_ratings_cleaned_1["review/score"].cast(FloatType()))

Books_ratings_cleaned_2 = Books_ratings_cleaned_2.dropna(subset = "review/score" )

# COMMAND ----------


# converting "review/time" column from unix timestamp to datetime

Books_ratings_cleaned_3 = Books_ratings_cleaned_2.withColumn("review_Time", from_unixtime(Books_ratings_cleaned_2["review/time"].cast("long")))

# COMMAND ----------

Books_ratings_cleaned_3.select("review/time","review_Time").show()

# COMMAND ----------

# checking how many rows of data is having null values in "Price" column

Books_ratings_cleaned_3.where(col("Price").isNull()).count()

# COMMAND ----------

# Since more than 84% of the data in "price" Column is 'Null' it will not give any meaningful insites. 
# So Dropping the column

Books_ratings_cleaned_4 = Books_ratings_cleaned_3.drop("Price") 

# COMMAND ----------

Books_ratings_cleaned_4.printSchema()

# COMMAND ----------

# Null Handling for All Columns
mean_value = Books_ratings_cleaned_4.agg(mean("review/score")).collect()[0][0]

replacement_values = {"Title": "N.A","User_id": "N.A","profileName": "N.A","review/summary": "N.A","review/text": "N.A",
                      "Review_helpfulness" : 0, "review/score": mean_value }

Books_ratings_cleaned_4 = Books_ratings_cleaned_4.fillna(replacement_values)

# COMMAND ----------

# final Transformed Data

Books_ratings_cleaned_final = Books_ratings_cleaned_4.select("Id","Title","User_id","profileName","Review_helpfulness",col("review/score").alias("review_score"),col("review/time").alias("review_time_unix"),"review_Time",col("review/summary").alias("review_summary"),col("review/text").alias("review_text"))

# COMMAND ----------

# Write the Transformed/Cleaned data into Azure Data lake
 
Books_ratings_cleaned_final.write.option("header",True).parquet("/mnt/amazonbooksreviews/Transformed Data/Books_ratings_cleaned")

# COMMAND ----------

# reading from data lake

Books_ratings_cleaned = spark.read.option("header",True).parquet("/mnt/amazonbooksreviews/Transformed Data/Books_ratings_cleaned")
Books_ratings_cleaned.display()

# COMMAND ----------


