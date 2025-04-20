# ingest_yelp_data.py
from pyspark.sql import SparkSession

def ingest_yelp_data():
    spark = SparkSession.builder \
        .appName("Yelp Data Ingestion") \
        .master("spark://spark-master:7077") \
        .enableHiveSupport() \
        .config("hive.metastore.uris", "thrift://scenario3-hive-metastore-1:9083") \
        .getOrCreate()
    
    # Read from HDFS
    business_df = spark.read.json("hdfs://scenario3-namenode-1:8020/data/raw/yelp_academic_dataset_business.json")
    review_df = spark.read.json("hdfs://scenario3-namenode-1:8020/data/raw/yelp_academic_dataset_review.json")
    
    # Print schema and sample data
    print("Business data schema:")
    business_df.printSchema()
    print("Sample business data:")
    business_df.show(5)
    
    print("Review data schema:")
    review_df.printSchema()
    print("Sample review data:")
    review_df.show(5)
    
    # Create Hive tables
    business_df.write.mode("overwrite").saveAsTable("raw_business")
    review_df.write.mode("overwrite").saveAsTable("raw_review")
    
    # Save as parquet for Trino access
    business_df.write.mode("overwrite").parquet("hdfs://scenario3-namenode-1:8020/data/processed/business")
    review_df.write.mode("overwrite").parquet("hdfs://scenario3-namenode-1:8020/data/processed/review")
    
    print(f"Loaded {business_df.count()} businesses and {review_df.count()} reviews")
    
    spark.stop()

if __name__ == "__main__":
    ingest_yelp_data()