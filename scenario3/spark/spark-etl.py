from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, current_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType

def process_yelp_data():
    # Initialize Spark
    spark = SparkSession.builder \
    .appName("Yelp Data Transformation") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .enableHiveSupport() \
    .getOrCreate()

    # Define schema for Yelp business and review JSON
    business_schema = StructType([
        StructField("business_id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("stars", DoubleType(), True),
        StructField("review_count", IntegerType(), True)
    ])

    review_schema = StructType([
        StructField("review_id", StringType(), False),
        StructField("business_id", StringType(), False),
        StructField("stars", DoubleType(), True),
        StructField("date", TimestampType(), True)
    ])

    # Read raw JSON from HDFS
    business_df = spark.read.schema(business_schema).json("hdfs://namenode:8020/raw/yelp_business_json")
    review_df = spark.read.schema(review_schema).json("hdfs://namenode:8020/raw/yelp_review_json")

    # Save raw data as Parquet in HDFS for reference
    business_df.write.mode("overwrite").parquet("hdfs://namenode:8020/raw/yelp_business_parquet")
    review_df.write.mode("overwrite").parquet("hdfs://namenode:8020/raw/yelp_review_parquet")

    # Create Hive database and raw tables
    spark.sql("CREATE DATABASE IF NOT EXISTS yelp")
    spark.sql("""
    CREATE EXTERNAL TABLE IF NOT EXISTS yelp.business_raw (
        business_id STRING,
        name STRING,
        city STRING,
        state STRING,
        stars DOUBLE,
        review_count INT
    )
    STORED AS PARQUET
    LOCATION 'hdfs://namenode:8020/raw/yelp_business_parquet'
    """)

    spark.sql("""
    CREATE EXTERNAL TABLE IF NOT EXISTS yelp.review_raw (
        review_id STRING,
        business_id STRING,
        stars DOUBLE,
        date TIMESTAMP
    )
    STORED AS PARQUET
    LOCATION 'hdfs://namenode:8020/raw/yelp_review_parquet'
    """)

    # SCD Type 2 for businesses (track changes in city)
    businesses = business_df.select("business_id", "name", "city").distinct()
    
    # Initialize dim_business if it exists
    try:
        existing_businesses = spark.sql("SELECT * FROM yelp.dim_business")
    except:
        existing_businesses = spark.createDataFrame([], StructType([
            StructField("business_key", IntegerType(), False),
            StructField("business_id", StringType(), False),
            StructField("name", StringType(), False),
            StructField("city", StringType(), True),
            StructField("valid_from", TimestampType(), True),
            StructField("valid_to", TimestampType(), True),
            StructField("is_active", BooleanType(), False)
        ]))
    
    # Assign business_key and handle SCD Type 2
    max_key = existing_businesses.selectExpr("coalesce(max(business_key), 0)").collect()[0][0]
    new_businesses = businesses.withColumn("business_key", lit(max_key) + col("business_id").cast("int") % 100000 + 1)
    new_businesses = new_businesses.withColumn("valid_from", current_date().cast(TimestampType()))
    new_businesses = new_businesses.withColumn("valid_to", lit(None).cast(TimestampType()))
    new_businesses = new_businesses.withColumn("is_active", lit(True))

    # Update existing businesses
    updated_businesses = existing_businesses.join(
        new_businesses,
        ["business_id"],
        "left_outer"
    ).select(
        existing_businesses.business_key,
        existing_businesses.business_id,
        existing_businesses.name,
        when(new_businesses.city.isNotNull() & (existing_businesses.city != new_businesses.city),
             new_businesses.city).otherwise(existing_businesses.city).alias("city"),
        existing_businesses.valid_from,
        when(new_businesses.city.isNotNull() & (existing_businesses.city != new_businesses.city),
             current_date().cast(TimestampType())).otherwise(existing_businesses.valid_to).alias("valid_to"),
        when(new_businesses.city.isNotNull() & (existing_businesses.city != new_businesses.city),
             lit(False)).otherwise(existing_businesses.is_active).alias("is_active")
    )

    # Add new business records for changes
    new_records = new_businesses.join(
        existing_businesses,
        ["business_id"],
        "left_anti"
    ).union(
        new_businesses.join(
            existing_businesses,
            (new_businesses.business_id == existing_businesses.business_id) & 
            (new_businesses.city != existing_businesses.city),
            "inner"
        ).select(
            new_businesses.business_key,
            new_businesses.business_id,
            new_businesses.name,
            new_businesses.city,
            new_businesses.valid_from,
            new_businesses.valid_to,
            new_businesses.is_active
        )
    )

    final_businesses = updated_businesses.union(new_records)
    final_businesses.write.mode("overwrite").parquet("hdfs://namenode:8020/dim/business")

    # Create Hive table for dim_business
    spark.sql("""
    CREATE EXTERNAL TABLE IF NOT EXISTS yelp.dim_business (
        business_key INT,
        business_id STRING,
        name STRING,
        city STRING,
        valid_from TIMESTAMP,
        valid_to TIMESTAMP,
        is_active BOOLEAN
    )
    STORED AS PARQUET
    LOCATION 'hdfs://namenode:8020/dim/business'
    """)

    # Transform reviews: Aggregate stars by business
    transformed_df = review_df.groupBy("business_id").agg(
        {"stars": "avg", "review_id": "count"}
    ).withColumnRenamed("avg(stars)", "avg_rating") \
     .withColumnRenamed("count(review_id)", "review_count")

    # Join with active businesses
    transformed_df = transformed_df.join(
        final_businesses.where(col("is_active") == True),
        ["business_id"],
        "inner"
    ).select(
        transformed_df.business_id,
        final_businesses.business_key,
        transformed_df.avg_rating,
        transformed_df.review_count
    )

    # Save transformed data
    transformed_df.write.mode("overwrite").parquet("hdfs://namenode:8020/transformed/yelp_sales")

    # Create Hive table for transformed data
    spark.sql("""
    CREATE EXTERNAL TABLE IF NOT EXISTS yelp.yelp_sales (
        business_id STRING,
        business_key INT,
        avg_rating DOUBLE,
        review_count INT
    )
    STORED AS PARQUET
    LOCATION 'hdfs://namenode:8020/transformed/yelp_sales'
    """)

    spark.stop()

if __name__ == "__main__":
    process_yelp_data()