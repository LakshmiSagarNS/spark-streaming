from pyspark.sql import SparkSession ,functions as F
from pyspark.conf import SparkConf
from pyspark.sql.functions import col,concat_ws


conf = SparkConf()
conf.set("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")

#start of kafka session

spark = SparkSession \
    .builder \
    .appName("amazon") \
    .master("local") \
    .config(conf=conf) \
    .getOrCreate()

#kafka configuration

bootstrap_servers = "localhost:9092"
topic = "test"

#defining the csv file path

file_path = "amazon.csv"
df = spark.read.format("csv").option("header", True).load(file_path)

#converting csv file values to string

string_df = df.select(concat_ws(",", col("marketplace"), col("customer_id"),col("review_id"), col("product_id"),col("product_parent"), col("product_title"),col("product_category"),col("star_rating"), col("helpful_votes"),col("total_votes"),col("vine"),col("verified_purchase"),col("review_headline"),col("review_body"),col("review_date"),col("sentiment")).alias("value"))

#writing to kafka
    
string_df.write\
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("topic", topic ) \
    

spark.stop()