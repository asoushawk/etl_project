from pyspark.sql import SparkSession


spark = SparkSession.builder.master('spark://spark:7077').config('spark.jars', 'hadoop-common-3.3.2.jar,aws-java-sdk-bundle-1.12.419.jar,hadoop-aws-3.3.2.jar') \
                            .getOrCreate()


# spark = SparkSession.builder.config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.0.0,com.google.guava:guava:23.0').config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
#                             .getOrCreatMINIO_ACCESSKEY = "oy9WWsyFNw4lFX3P"
MINIO_ACCESSKEY = "oy9WWsyFNw4lFX3P"
MINIO_SECRETKEY = "ZQf5hwB9BYsN0fLjSTOlpZ560uXrZHZD"



spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", MINIO_ACCESSKEY)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", MINIO_SECRETKEY)
spark._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.path.style.access", "true")
spark._jsc.hadoopConfiguration().set("park.hadoop.fs.s3a.connection.ssl.enabled", "false")


df_cartorios = spark.read.csv('s3a://data-product-a/landing/')
df_cartorios.show()

spark.createDataFrame([dict(a='a')]).show()

#df_cartorios