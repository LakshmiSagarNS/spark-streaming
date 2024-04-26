from pyspark.sql import SparkSession ,functions as F
from pyspark.conf import SparkConf
from pyspark.sql.types import StructType,StructField,StringType,IntegerType





conf = SparkConf()
conf.set("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")

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

file_path = "data.csv"
df = spark.read.format("csv").option("header", True).load(file_path)

# try:
#   df = spark.read.format("csv").option("header", True).load(file_path)
#   df = df.select(F.to_json(F.struct(*df.columns)).alias("value"))
# except:  # Catch any errors during reading with header
#   pass

# if not df:
#  try:
#    selected_columns=["marketplace","customer_id","review_id","product_id","product_parent","product_title","product_category","star_rating","helpful_votes","total_votes","vine","verified_purchase","review_headline","review_body","review_date","sentiment"]
#    schema=StructType([
#    StructField("marketplace",StringType(),True),
#    StructField("customer_id",StringType(),True),
#    StructField("review_id",StringType(),True),
#    StructField("product_id",StringType(),True),
#    StructField("product_parent",StringType(),True),
#    StructField("product_title",StringType(),True),
#    StructField("product_category",StringType(),True),
#    StructField("star_rating",StringType(),True),
#    StructField("helpful_votes",StringType(),True),
#    StructField("total_votes",StringType(),True),
#    StructField("vine",StringType(),True),
#    StructField("verified_purchase",StringType(),True),
#    StructField("review_headline",StringType(),True),
#    StructField("review_body",StringType(),True),
#    StructField("review_date",StringType(),True),
#    StructField("sentiment",StringType(),True),
#   ])
#    df = spark.read.format("csv").option("header", True).schema(schema).load(csv_path)
#    df = df.select(*selected_columns)
   
#  except:  # Catch any errors during reading with schema
#     pass

df.writeStream\
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("topic", topic ) \
    

spark.stop()