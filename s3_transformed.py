from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, month, year

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("S3 Data Transformation") \
    .config("spark.hadoop.fs.s3a.access.key", "AWQUOZSKXYCE5YL6G") \
    .config("spark.hadoop.fs.s3a.secret.key", "OPHwKK7ASEltt7Wg0mMzIS5ErtvWQvIEpmr1f") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

# Read the raw dataset from S3
input_path = "s3://mini-project-ropavu/raw/used_car_dataset.csv"
data = spark.read.csv(input_path, header=True, inferSchema=True)

# Add Month and YearPosted columns
data = data.withColumn("Month", month(to_date(col("PostedDate"), "MMM-yy"))) \
           .withColumn("YearPosted", year(to_date(col("PostedDate"), "MMM-yy")))

# Write the transformed data back to S3
output_path = "s3://mini-project-ropavu/processed/transformed_data.csv"
data.write.csv(output_path, header=True, mode="overwrite")