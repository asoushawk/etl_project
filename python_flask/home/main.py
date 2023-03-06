from flask import Flask, render_template
import os

from pyspark.sql import SparkSession

from pyspark_test import job

app = Flask(__name__)

spark = SparkSession.builder.master('spark://spark:7077').config('spark.jars', 'aws-java-sdk-1.7.4.jar,hadoop-aws-2.7.4.jar,jets3t-0.9.4.jar') \
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


@app.route('/')
def home():
    return 'a'


@app.route('/run_job')
def run_job():
    
    job(spark)

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=True, host='0.0.0.0', port=port)