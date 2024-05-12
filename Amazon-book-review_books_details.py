# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Books_details

# COMMAND ----------

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

# Custom Schema
Book_details_schema = StructType()\
                    .add("title", StringType(),False)\
                    .add("description",StringType(),True)\
                    .add("authors", ArrayType(StringType()),True)\
                    .add("image",StringType(),True)\
                    .add("previewLink",StringType(),True)\
                    .add("publisher",StringType(),True)\
                    .add("publishedDate",DateType(),True)\
                    .add("infoLink",StringType())\
                    .add("categories",ArrayType(StringType()),True)\
                    .add("ratingsCount",FloatType(),True)

# COMMAND ----------

#reading books_details file from Azure Storage Account
Book_details_raw = spark.read.format("csv").option("header" ,True).option("inferschema",True).load("/mnt/amazonbooksreviews/Raw Data/books_data.csv")
Book_details_raw.count()

# COMMAND ----------

Book_details_raw.printSchema()

# COMMAND ----------

Book_details_raw.display()

# COMMAND ----------

# we will remove unwanted characters like ('[',']',"'") in Authors Column
Book_details_cleaned = Book_details_raw.withColumn('authors',regexp_replace(col('authors'),"[\\[\]']",''))




# COMMAND ----------

# Since the "Authors" column is having multiple authors for a single book title we will convert it into a array and explode it as seperate rows.
Book_details_cleaned= Book_details_cleaned.withColumn("authors", split(Book_details_cleaned["authors"], ","))


# COMMAND ----------

Book_details_cleaned = Book_details_cleaned.withColumn('categories',regexp_replace(col('categories'),"[\\[\]']",''))

# COMMAND ----------

# Since the "categories" column is having multiple category for a single book title we will convert it into a array and explode it as seperate rows.
Book_details_cleaned = Book_details_cleaned.withColumn("categories", split(Book_details_cleaned["categories"], ","))

# COMMAND ----------

Book_details_cleaned.printSchema()

# COMMAND ----------

Book_details_cleaned.display()

# COMMAND ----------

# Remove bad data from "ratingsCount" Column
Book_details_cleaned_1 = Book_details_cleaned.withColumn("Ratings_Count",Book_details_cleaned.ratingsCount.cast(FloatType()))
Book_details_cleaned_1.count()

# COMMAND ----------

Book_details_cleaned_1.select("ratingsCount","Ratings_Count").show()

# COMMAND ----------

# Getting the publication year from "publishedDate" Column
# Spark 3.0 does not allow Date parsing of 'yyyy-mm-dd' and 'yyyy' in same column so we change the configuration here

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

Book_details_cleaned_2 = Book_details_cleaned_1.withColumn("Year_of_publish", year(to_date("publishedDate", "yyyy")))
Book_details_cleaned_2.select("publishedDate","Year_of_publish").show()

# COMMAND ----------

Book_details_cleaned_2.display()

# COMMAND ----------

Book_details_cleaned_2.printSchema()

# COMMAND ----------

# Dropping rows from Dataframe "Book_details_cleaned_2" which has Bad Data in Column "image"

Book_details_cleaned_3 = Book_details_cleaned_2.filter((col("image").startswith("http")) | (col("image").isNull()))
Book_details_cleaned_3.count()

# COMMAND ----------

# Dropping rows from Dataframe "Book_details_cleaned_2" which has Bad Data in Column "previewLink"

Book_details_cleaned_3 = Book_details_cleaned_3.filter((col("previewLink").startswith("http")) | (col("previewLink").isNull()))
Book_details_cleaned_3.count()

# COMMAND ----------

# Dropping rows from Dataframe "Book_details_cleaned_2" which has Bad Data in Column "infoLink"

Book_details_cleaned_3 = Book_details_cleaned_3.filter((col("infoLink").startswith("http")) | (col("infoLink").isNull()))
Book_details_cleaned_3.count()

# COMMAND ----------

Book_details_cleaned_3.display()

# COMMAND ----------

# Exploding "authors" and "categories" Columns

Book_details_cleaned_final = Book_details_cleaned_3.select("Title","description",explode("authors").alias("authors"),"image","previewLink","publisher","publishedDate","infoLink",explode("categories").alias("categories"),"Ratings_Count","Year_of_publish")

# COMMAND ----------

Book_details_cleaned_final.select("Title","authors","categories").show()

# COMMAND ----------

Book_details_cleaned_final.printSchema()

# COMMAND ----------

# Null Handling for String Columns

replacement_values = {"Title": "N.A","description" : "N.A","authors": "N.A","image": "N.A","previewLink": "N.A","publisher": "N.A","infoLink": "N.A","categories": "N.A", "publishedDate": "N.A", "Ratings_Count": 0 }

Book_details_cleaned_final = Book_details_cleaned_final.fillna(replacement_values)


# COMMAND ----------

# Dropping all further rows which has nulls and dropping Duplicate rows

Book_details_cleaned_final = Book_details_cleaned_final.dropna()
Book_details_cleaned_final = Book_details_cleaned_final.dropDuplicates()
Book_details_cleaned_final.count()

# COMMAND ----------

display(Book_details_cleaned_final.limit(10000))

# COMMAND ----------

# cleaning the column values by removing unwanted characters for one last time.

# "authors" column
Book_details_cleaned_final= Book_details_cleaned_final.withColumn("authors", regexp_replace(col("authors"), '\\\\', '')).withColumn("authors", regexp_replace(col("authors"), '"', ''))

# "publisher" column
Book_details_cleaned_final = Book_details_cleaned_final.withColumn("publisher", regexp_replace(col("publisher"), '\\\\', '')).withColumn("publisher", regexp_replace(col("publisher"), '"', ''))

# "categories" column
Book_details_cleaned_final= Book_details_cleaned_final.withColumn("categories", regexp_replace(col("categories"), '\\\\', '')).withColumn("categories", regexp_replace(col("categories"), '"', ''))



# COMMAND ----------

# Write the Transformed/Cleaned data into Azure Data lake
 
Book_details_cleaned_final.write.option("header",True).parquet("/mnt/amazonbooksreviews/Transformed Data/Books_Details_cleaned")

# COMMAND ----------

Book_details_cleaned = spark.read.option("header",True).parquet("/mnt/amazonbooksreviews/Transformed Data/Books_Details_cleaned")
Book_details_cleaned.count()
