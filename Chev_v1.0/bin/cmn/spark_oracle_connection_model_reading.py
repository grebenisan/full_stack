# coding=utf-8
# Date: 06/25/2018
# Author: Dan Grebenisan
# Description: Provides a model to read from Oracle via JDBC
# Note: You must supply the oracle jdbc jar's directory as a spark configuration when launching the applicaiton
# Spark Version: 2.1.1

from pyspark.sql import SparkSession

spark = SparkSession. \
    builder. \
    appName("Read_From_Oracle"). \
    enableHiveSupport(). \
    getOrCreate()

spark.sparkContext.setSystemProperty("oracle.net.tns_admin","/data/commonScripts/Wallet/tedwload")

CONNECTION_URL = "jdbc:oracle:thin:@chevelle_dev"
DBTABLE = "CHEVELLE.CBH1"
DRIVER = "oracle.jdbc.driver.OracleDriver"

jdbc_data = spark.read.format("jdbc"). \
    option("url", CONNECTION_URL).\
    option("dbtable", DBTABLE). \
    option("driver", DRIVER). \
    load()

jdbc_data.show()