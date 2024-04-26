from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws



def write_to_kafka(csv_path, topic_name, bootstrap_servers):
    

    spark = SparkSession.builder \
        .appName("CSV to Kafka") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")\
        .getOrCreate()

    # Read CSV data as a DataFrame
    df = spark.read \
        .format("csv") \
        .option("header", True)  \
        .option("delimiter", ",")  \
        .load(csv_path)

   
        # Writing each row as a separate string 
        
    string_df = df.select(concat_ws(",", col("marketplace"), col("customer_id"),col("review_id"), col("product_id"),col("product_parent"), col("product_title"),col("product_category"),col("star_rating"), col("helpful_votes"),col("total_votes"),col("vine"),col("verified_purchase"),col("review_headline"),col("review_body"),col("review_date"),col("sentiment")).alias("value"))


    string_df.write \
            .format("kafka") \
            .option("topic", topic_name) \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .save()

    spark.stop()


if __name__ == "__main__":
    csv_path = "data.csv"  
    topic_name = "test" 
    bootstrap_servers = "localhost:9092"  


    write_to_kafka(csv_path, topic_name, bootstrap_servers)