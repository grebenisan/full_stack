# coding=utf-8
# Date: 06/25/2018
# Author: Dan Grebenisan
# Description: Provides a model to connect to Oracle via JDBC
# Note: You must supply the oracle jdbc jar's directory as a spark configuration when launching the applicaiton
# Spark Version: 2.1.1

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql.functions import lit, col
from datetime import datetime

spark = SparkSession. \
    builder. \
    appName("Write_To_Oracle"). \
    enableHiveSupport(). \
    getOrCreate()

spark.sparkContext.setSystemProperty("oracle.net.tns_admin","/data/commonScripts/Wallet/tedwload")

DBTABLE = "CHEVELLE.CBH1"
DRIVER = "oracle.jdbc.driver.OracleDriver"

schema = StructType([StructField("COL1", IntegerType())])

df = spark.createDataFrame([[3]], schema=schema)
df_column_added = df.withColumn('UPDATE_DT', lit(datetime.now()))

df_column_added.write.format("jdbc"). \
    option("url", "jdbc:oracle:thin:@chevelle_dev").\
    option("dbtable", DBTABLE). \
    option("driver", DRIVER). \
    save(mode="append")
