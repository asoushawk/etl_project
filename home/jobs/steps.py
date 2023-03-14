def landing_to_cleaning(spark):

    #df = spark.read.csv('s3a://data-product-a/landing/teste_data/')

    #df.write.parquet('s3a://data-product-a/cleaning/teste_data/')

    df = spark.createDataFrame([dict(a=2)])
    df.write.csv('s3a://landing-a/data/')

def cleaning_to_gold(spark):

    df = spark.read.csv('s3a://data-product-a/cleaning/teste_data')

    df.write.parquet('s3a://data-product-a/gold/teste_data')