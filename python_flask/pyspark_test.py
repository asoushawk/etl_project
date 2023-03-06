def job(spark):
    
    spark.createDataFrame([dict(a=1)]).show()

    return True

