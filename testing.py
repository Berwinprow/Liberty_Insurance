from pyspark.sql import SparkSession

# Initialize a SparkSession (this includes a SparkContext internally)
spark = SparkSession.builder.appName("SampleRun").getOrCreate()

# Access SparkContext from SparkSession
sc = spark.sparkContext

# Create an RDD
text_rdd = sc.parallelize(["hello world", "hello PySpark", "Apache Spark is fun"])

# Transform the RDD
word_counts = text_rdd.flatMap(lambda line: line.split(" ")) \
                       .map(lambda word: (word, 1)) \
                       .reduceByKey(lambda a, b: a + b)

# Collect and print results
print(word_counts.collect())

# Stop the Spark session
spark.stop()
