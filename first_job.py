import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
import pyspark
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark import SparkConf  # Add this import

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
 

# reading locations table 

taxi_zone_lkp_df = spark.read.csv("s3://group2-final-project/nyc_taxi_raw_data/taxi_zone_lookup.csv", header=True, inferSchema=True)

jdbc_url = "jdbc:mysql://database-2.chniqnwzuk84.us-east-1.rds.amazonaws.com:3306/grp2"

# Reading data from RDS

# Set the connection properties
connection_properties = {
     "user": "admin",
     "password": "12345678"
}

table_name = "trips"


rds_df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)



#Reading data from s3

s3_df = spark.read.csv("s3://pro2grp/sam/s3_sam.csv", header=True, inferSchema=True)

# Loop through the columns and convert them to double in 'rds_df'

columns_to_convert = ["fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount", "congestion_surcharge"]


for column_name in columns_to_convert:
     rds_df = rds_df.withColumn(column_name, rds_df[column_name].cast(
 "double"))

# Loop through the columns and convert them to double in 's3_df'

for column_name in columns_to_convert:
     s3_df = s3_df.withColumn(column_name, s3_df[column_name].cast("double"))


# performing union on both dataframes

df=rds_df.union(s3_df)

# performing left join on combined table

joined_df = df.alias("df").join(taxi_zone_lkp_df.select("LocationID", "Borough", "Zone").alias("yt_merged1"),
     col("df.PULocationID") == col("yt_merged1.LocationID"),
     "left"
).join(
     taxi_zone_lkp_df.select("LocationID", "Borough", "Zone").alias("yt_merged2"),
     col("df.DOLocationID") == col("yt_merged2.LocationID"),
     "left"
)

# renaming columns from zones table

final_df = joined_df.withColumn("PULocation_borough", col("yt_merged1.Borough")) \
    .withColumn("DOLocation_borough", col("yt_merged2.Borough")) \
    .withColumn("PULocation_zone", col("yt_merged1.Zone")) \
    .withColumn("DOLocation_zone", col("yt_merged2.Zone"))

# Drop the specified columns

columns_to_drop = ["LocationID", "Zone", "Borough"]
df = final_df.drop(*columns_to_drop)

# writing data on s3 


output_path = "s3://terraform-nikhil-prac/nikhil"
df.write.parquet(output_path)

job.commit()
