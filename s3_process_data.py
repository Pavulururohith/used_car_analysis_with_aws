from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, regexp_replace, sum, avg

# Initialize Spark Session with S3 support
spark = SparkSession.builder \
    .appName("S3 Data Processing") \
    .config("spark.hadoop.fs.s3a.awsAccessKeyId", "AWQUOZSKXYCE5YL6G") \
    .config("spark.hadoop.fs.s3a.awsSecretAccessKey", "OPHwKK7ASEltt7Wg0mMzIS5ErtvWQvIEpmr1f") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
    .config("spark.hadoop.fs.s3a.attempts.max", "10") \
    .config("spark.hadoop.fs.s3a.paging.maximum", "1000") \
    .getOrCreate()

# Load dataset from S3
file_path = "s3a://mini-project-ropavu/raw/used_car_dataset.csv"
df = spark.read.csv("./used_car_dataset.csv", header=True, inferSchema=True)

# Handle missing values before transformations if necessary
df = df.dropna(subset=["PostedDate", "kmDriven", "AskPrice", "Brand", "Transmission"])

# Transform the data
df = df.withColumn("Month", split(col("PostedDate"), "-")[0]) \
       .withColumn("YearPosted", split(col("PostedDate"), "-")[1]) \
       .withColumn("kmDriven", regexp_replace(col("kmDriven"), "[^0-9]", "").cast("int")) \
       .withColumn("AskPrice", regexp_replace(col("AskPrice"), "[^0-9]", "").cast("int"))

# Optionally, cache the transformed dataframe if reused multiple times
df.cache()

# Perform Aggregations
total_revenue_by_brand = df.groupBy("Brand").agg(sum("AskPrice").alias("TotalRevenue"))
monthly_spending = df.groupBy("Month").agg(sum("AskPrice").alias("TotalMonthlySpending"))
top_transactions = df.orderBy(col("AskPrice").desc()).limit(10)
avg_age_by_brand = df.groupBy("Brand").agg(avg("Age").alias("AverageAge"))
total_distance_by_transmission = df.groupBy("Transmission").agg(sum("kmDriven").alias("TotalDistanceDriven"))

# Save Results to S3
output_path = "s3a://mini-project-ropavu/processed/"
df.write.csv(f"./transformed_data", header=True, mode="overwrite")
total_revenue_by_brand.write.csv(f"./total_revenue_by_brand", header=True, mode="overwrite")
monthly_spending.write.csv(f"./monthly_spending", header=True, mode="overwrite")
top_transactions.write.csv(f"./top_transactions", header=True, mode="overwrite")
avg_age_by_brand.write.csv(f"./avg_age_by_brand", header=True, mode="overwrite")
total_distance_by_transmission.write.csv(f"./total_distance_by_transmission", header=True, mode="overwrite")

print("Data processing completed successfully!")