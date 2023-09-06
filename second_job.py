import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql import functions as F 
from pyspark.sql.functions import col, month, from_unixtime, date_format, round, unix_timestamp, to_timestamp, year, hour,to_date, date_format, round



## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)




#1)
# Read the complete dataset.

df = spark.read.parquet("s3://terraform-nikhil-prac/nikhil/", inferSchema=True, header=True)

#--------------------------------------------------------------------------------------------

## Data Cleaning and Prep-Processing

#2)
# Drop duplicate rows
df = df.dropDuplicates()

#--------------------------------------------------------------------------------------------
#3)
# Drop rows with missing values
df = df.na.drop()

#--------------------------------------------------------------------------------------------

## Feature engineering

#4)
# Convert pickup and dropoff datetime columns to PySpark timestamp columns

df = df.withColumn("tpep_pickup_datetime", to_timestamp("tpep_pickup_datetime", "MM/dd/yyyy hh:mm:ss a")) 
df = df.withColumn("tpep_dropoff_datetime", to_timestamp("tpep_dropoff_datetime", "MM/dd/yyyy hh:mm:ss a")) 

#--------------------------------------------------------------------------------------------
#5) 
# Calculate trip duration in seconds
df = df.withColumn("Duration", (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")).cast("int"))

#--------------------------------------------------------------------------------------------
#6)
# Calculate speed in mph
df = df.withColumn("speed_mph", round(df["trip_distance"] / (df["Duration"] / 3600), 2))

#--------------------------------------------------------------------------------------------

#7)
# Add pickup_date, drop_date, year, month, pickup_day, dropoff_day, pickup_hour, dropoff_hour, Month_Year columns

df = df.withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))
df = df.withColumn("drop_date", to_date(col("tpep_pickup_datetime")))
df = df.withColumn("year", year(col("tpep_pickup_datetime")))
df = df.withColumn("month", from_unixtime(month(col("tpep_pickup_datetime")), "MMMM"))
df = df.withColumn("pickup_day", date_format(col("tpep_pickup_datetime"), "EEEE"))
df = df.withColumn("dropoff_day", date_format(col("tpep_dropoff_datetime"), "EEEE"))
df = df.withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
df = df.withColumn("dropoff_hour", hour(col("tpep_dropoff_datetime")))
df = df.withColumn("Month_Year", date_format(col("tpep_pickup_datetime"), "yyyy-MM"))

#----------------------------------------------------------------------------------------------

#8)
# Add time_of_day UDF (User-Defined Function)

def time_of_day(x):
    if 6 <= x < 12:
        return 'Morning'
    elif 12 <= x < 16:
        return 'Afternoon'
    elif 16 <= x < 22:
        return 'Evening'
    else:
        return 'Late night'

time_of_day_udf = spark.udf.register("time_of_day", time_of_day)

#9)
# Add pickup_timeofday and dropoff_timeofday columns

df = df.withColumn("pickup_timeofday", time_of_day_udf(col("pickup_hour")))
df = df.withColumn("dropoff_timeofday", time_of_day_udf(col("dropoff_hour")))

#----------------------------------------------------------------------------------------------
# Data Preprocessing

#10)
# Filter rows where PULocation_borough is Unknown.
df = df.filter(df["PULocation_borough"] != "Unknown")

#11)
# Filter rows where the year is not 2019, 2020, or 2021
df = df.filter((df["year"] == 2019) | (df["year"] == 2020) | (df["year"] == 2021))

#12)
# Filter rows where the payment_type is 1, 2, 3, 4, 5 or 6
df = df.filter(col("payment_type").isin([1, 2, 3, 4, 5, 6]))

#---------------------------------------------------------------------------------------------
#13)
# List of numeric columns to check for negative values
numeric_columns = [col_name for col_name, data_type in df.dtypes if data_type in ('int', 'double', 'float')]

# Removing negative values from numeric columns
for column in numeric_columns:
    df = df.filter(F.col(column) >= 0)

#14)
# Choose only those rows where passenger_count,fare_amount and trip_distance is not zero.
# Because it doesn't make any sense to have 0 in this columns.
df = df.filter((df["passenger_count"] != 0) & (df["fare_amount"] != 0) & (df["trip_distance"] != 0))

#15)
# Drop rows where Duration is 0 and speed_mph is null
df = df.filter(~((df['Duration'] == 0) & df['speed_mph'].isNull()))

#16)
# Drop rows with extreme values
# We are setting some threshold value for trip_distance, speed_mph and Duration based on some assumptions.
df = df.filter((df.trip_distance <= 180) & (df.trip_distance >= 0.05) & (df.speed_mph <= 65) & (df.Duration <= 40000))


# Convert int to bigint for specified columns
bigint_columns = ['vendorid', 'passenger_count', 'ratecodeid', 'pulocationid', 'dolocationid', 'payment_type']
for col_name in bigint_columns:
    df = df.withColumn(col_name, col(col_name).cast("bigint"))

# Convert trip_distance from string to double
decimal_columns = ['trip_distance', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount','congestion_surcharge']
for col_name in decimal_columns:
	df = df.withColumn(col_name, col(col_name).cast("decimal(10, 2)"))

#-------------------------------------------------------------------------------------------------
### LOADING CLEANED DATA IN REDSHIFT

spark._jsc.hadoopConfiguration().set("spark.jars.packages", "io.github.spark_redshift_community.spark.redshift_2.12:2.1.0")

redshift_url = 'jdbc:redshift://redshift-cluster-1.cxcwe1jbsaj9.us-east-1.redshift.amazonaws.com:5439/clean_data'
redshift_user = 'taxi'
redshift_password = 'Group123'

df.write.format("io.github.spark_redshift_community.spark.redshift") \
  .option("url", redshift_url) \
  .option("dbtable", "trips") \
  .option("tempdir","s3://terraform-nikhil-prac/") \
  .option("user", redshift_user) \
  .option("password", redshift_password) \
  .option("forward_spark_s3_credentials", True).mode("append").save()

job.commit()