def job(spark):

    df_cartorios = spark.read.csv('s3a://data-product-a/landing/')

    df_cartorios.show()