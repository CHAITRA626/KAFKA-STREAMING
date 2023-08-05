import logging
import spacy
from pyspark.sql.functions import lower, udf, current_timestamp, date_trunc, regexp_replace, trim, col, explode, split, desc
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql.functions import lit

from pyspark.sql import SparkSession
from pyspark import SparkConf

from pyspark.sql.types import StringType
import shutil
import os
import nltk
from nltk.corpus import stopwords
import sys
nltk.download('averaged_perceptron_tagger')
nltk.download('stopwords')

args = sys.argv

if len(args) != 5:
    print("Pass sufficnent arguments.")
    print('Help: spark_connector.py <CHECKPOINT_DIR> <BOOTSTRAP_SERVER> <READ_TOPIC> <WRITE_TOPIC>')

# Reading the arguments
CHECKPOINT_DIR = args[1]
BOOTSTRAP_SERVER = args[2]
READ_TOPIC = args[3]
WRITE_TOPIC = args[4]

# If checkpoint directory exists delete the checkpoint directory
if os.path.exists(CHECKPOINT_DIR):
    print('Clearing checkpoint dir ', CHECKPOINT_DIR)
    shutil.rmtree(CHECKPOINT_DIR)

conf = SparkConf().setAppName("MyApp").set("spark.jars", "kafka-clients-3.4.0.jar")

spark = SparkSession.builder.appName("reddit-comments-stream-application").getOrCreate()

# supress the warnings, unwanted logging 
spark.sparkContext.setLogLevel("WARN")
logging.getLogger("py4j").setLevel(logging.ERROR)

# Create a DataFrame representing the stream of input lines from Kafka
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", READ_TOPIC).load().selectExpr("CAST(value AS STRING)")

df.printSchema()

words = df

#Tags are NN - noun singular, NNS - noun plural, NNP - proper noun singular, NNPS - proper noun plural
TAG_LIST = ["NN", "NNS", "NNP", "NNPS"]

#Retreived named entities by filtering words whose tags are NN, NNS, NNP, NNPS
@udf(returnType=StringType())
def identify_named_entities(text):
    tokens = nltk.word_tokenize(text)
    pos_tags = nltk.pos_tag(tokens)
    
    named_entities = [word for word, tag in pos_tags if tag in TAG_LIST]
    
    return ','.join(named_entities)

words = words.select(identify_named_entities("value").alias("words"))

# Remove stop words
stop_words = stopwords.words('english')
words = words.filter(~col("words").isin(stop_words))
words = words.select(explode(split(words.words, ',')))

# Remove empty & null words
words = words.filter(col("words").isNotNull() & (col("words") != ""))
words = words.selectExpr("col as words")

words.printSchema()

words = words.withColumn("words", lower("words"))\
            .withColumn("words", regexp_replace("words", "[^a-zA-Z0-9\\s]+", "")) \
            .withColumn("words", trim("words")) \
            .withColumn("count", lit(1))

# remove empty & null words
words = words.filter(col("words").isNotNull() & (col("words") != ""))
words.printSchema()

words.printSchema()

window_size = "5 seconds"

# Frequency of words
words_count = words.groupBy('words').count()

words_count = words_count.withColumn("timestamp", date_trunc("second", current_timestamp()))

words_count.printSchema()

# use this schema for pushing to the final topic
final_schema = StructType([
    StructField("words", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("count", IntegerType(), True)
])

console_query = words_count.orderBy(desc("count")).writeStream\
                    .outputMode("complete")\
                    .format("console")\
                    .start()

query = words\
        .selectExpr("CAST(words AS STRING) AS key",  "to_json(struct(*)) AS value") \
        .writeStream.format("kafka").option("kafka.bootstrap.servers", BOOTSTRAP_SERVER) \
        .outputMode("update") \
        .option("topic", WRITE_TOPIC).option("checkpointLocation", CHECKPOINT_DIR) \
        .start()

query.awaitTermination()
console_query.awaitTermination()
