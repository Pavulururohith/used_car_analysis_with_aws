from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("SQL Insights").getOrCreate()

# Load data into DataFrames
df_top_transactions = spark.read.csv("top_transactions/*.csv", header=True, inferSchema=True)
df_avg_age_by_brand = spark.read.csv("avg_age_by_brand/*.csv", header=True, inferSchema=True)
df_monthly_spending = spark.read.csv("monthly_spending/*.csv", header=True, inferSchema=True)
df_total_distance_by_transmission = spark.read.csv("total_distance_by_transmission/*.csv", header=True, inferSchema=True)
df_total_revenue_by_brand = spark.read.csv("total_revenue_by_brand/*.csv", header=True, inferSchema=True)

# Create temporary views for SQL queries
df_top_transactions.createOrReplaceTempView("top_transactions")
df_avg_age_by_brand.createOrReplaceTempView("avg_age_by_brand")
df_monthly_spending.createOrReplaceTempView("monthly_spending")
df_total_distance_by_transmission.createOrReplaceTempView("total_distance_by_transmission")
df_total_revenue_by_brand.createOrReplaceTempView("total_revenue_by_brand")

# Queries
queries = [
    # Query 1: Identify the top 5 brands by total revenue
    """
    SELECT Brand, TotalRevenue
    FROM total_revenue_by_brand
    ORDER BY TotalRevenue DESC
    LIMIT 5
    """,

    # Query 2: Analyze monthly spending trends
    """
    SELECT Month, TotalMonthlySpending
    FROM monthly_spending
    ORDER BY TotalMonthlySpending DESC
    """,

    # Query 3: Determine the top 5 car brands with the highest average age
    """
    SELECT Brand, AverageAge
    FROM avg_age_by_brand
    ORDER BY AverageAge DESC
    LIMIT 5
    """,

    # Query 4: Analyze the total distance driven grouped by transmission type
    """
    SELECT Transmission, TotalDistanceDriven
    FROM total_distance_by_transmission
    ORDER BY TotalDistanceDriven DESC
    """,

    # Query 5: Find the top 5 most expensive car transactions
    """
    SELECT Brand, model, AskPrice
    FROM top_transactions
    ORDER BY AskPrice DESC
    LIMIT 5
    """
    ]

# Execute and display results for each query
for i, query in enumerate(queries, 1):
    print(f"Query {i}:")
    result = spark.sql(query)
    result.show()