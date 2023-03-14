from flask import Flask, render_template, jsonify
import os

from pyspark.sql import SparkSession

from jobs.steps import *

app = Flask(__name__)

spark = SparkSession.builder.master('spark://spark:7077').config('spark.jars', './home/jars/aws-java-sdk-bundle-1.12.419.jar,./home/jars/hadoop-aws-3.3.2.jar') \
                            .getOrCreate()


MINIO_ACCESSKEY = "EPwfoNItjO2GV3eD"
MINIO_SECRETKEY = "Ubcoo70gmmpr5rkb4WbHc19k46eKgSUn"



spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", MINIO_ACCESSKEY)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", MINIO_SECRETKEY)
spark._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
#spark._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.fast.upload", "true") 
#spark._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.path.style.access", "true")
spark._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "true")
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
spark._jsc.hadoopConfiguration().set("fs.s3a.attempts.maximum", "1")
spark._jsc.hadoopConfiguration().set("fs.s3a.connection.establish.timeout", "5000")
spark._jsc.hadoopConfiguration().set("fs.s3a.connection.timeout", "10000")


df = spark.createDataFrame([dict(a=2)])
spark.read.csv('s3a://landing-a/spark/').show()


@app.route('/')
def home():
    return 'a'


@app.route('/run_job_lc')
def run_job():
    
    try:
        landing_to_cleaning(spark)
        return jsonify({"status": 200})
    except Exception as e:
        return jsonify({"status": 500, "error": str(e)})


@app.route('/run_job_lg')
def run_job_2():

    try:
        cleaning_to_gold(spark)
        return jsonify({"status": 200})
    except Exception as e:
        return jsonify({"status": 500, "error": str(e)})

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=True, host='0.0.0.0', port=port)




