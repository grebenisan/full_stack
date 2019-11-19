# coding=utf-8
# Author: Dan Grebenisan
# Modified Date: 06/15/2018
# Filename: silkmoth.py
# Description: Creates a digital fingerprint of all columns in all tables
# Spark Version: 2.1.1

import sys
import math
import time
from datetime import datetime
from pyspark.sql.functions import lit, col, trim
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql import SparkSession

reload(sys)
sys.setdefaultencoding('utf-8')

spark = SparkSession.builder. \
    appName("Chevelle_Silkmoth"). \
    config("spark.task.cpus", 2). \
    config("spark.pyspark.python", '/opt/oss/anaconda/envs/py2.7/bin/python'). \
    enableHiveSupport().getOrCreate()

local_time = time.localtime()

app_name = 'Chevelle'
run_type = 'Silkmoth'

output_schema = StructType([
    StructField("app_name", StringType(), True),
    StructField("run_type", StringType(), True),
    StructField("schema", StringType(), True),
    StructField("table", StringType(), True),
    StructField("column", StringType(), True),
    StructField("signature", StringType(), True)])

file_schema = StructType([
    StructField("schema", StringType(), True),
    StructField("table", StringType(), True)])

length_test_query = \
    """
    select avg(length({0})) from {1}.{2}
    """
input_dir = "/{0}/EDW/DSODB/OPS/CHEVELLE/HYPER_PROF_RSLT/Input/silkmoth_tbl_list1.txt".format('DEV')
output_dir = "/{0}/EDW/DSODB/OPS/CHEVELLE/HYPER_PROF_RSLT/Output_Silk".format('DEV')
char_limit = 50

q = 3

def q_chunks(string):
    global q
    if len(string) >= 3:
        num_chunks = int(math.ceil(len(string) / q))
        return [string[i * q:(i + 1) * q] for i in range(num_chunks)]
    elif len(string) == 2:
        q = 2
        num_chunks = int(math.ceil(len(string) / q))
        return [string[i * q:(i + 1) * q] for i in range(num_chunks)]
    elif len(string) < 2:
        q = 1
        num_chunks = int(math.ceil(len(string) / q))
        return [string[i * q:(i + 1) * q] for i in range(num_chunks)]


def q_tokens(string):
    global q
    if len(string) >= 3:
        return [string[i:i + q] for i in range(len(string))]
    elif len(string) == 2:
        q = 2
        return [string[i:i + q] for i in range(len(string))]
    elif len(string) < 2:
        q = 1
        return [string[i:i + q] for i in range(len(string))]


def signature_multiset(set_):
    global q
    return [q_chunks(string) for string in set_]


def elements_with_token(token, _sets):
    elements = []
    for i, set_ in enumerate(_sets):
        for j, string in enumerate(set_):
            if token in q_tokens(string, q):
                elements.append((i, j))
    return elements


excluded_columns = 'src_sys_crt_ts', 'src_sys_id', 'src_sys_iud_cd', \
                   'src_sys_upd_by', 'src_sys_upd_gmt_ts', 'src_sys_upd_ts', \
                   'dw_anomaly_flg', 'dw_extract_ts', 'dw_ins_gmt_ts', \
                   'dw_job_id', 'dw_mod_ts', 'dw_sum_sk', 'gg_src_sys_iud_cd', \
                   'gg_txn_numerator', 'gg_txn_csn', 'gg_src_sys_upd_ts', 'dw_sum_sk'


data_file = spark.read.csv(input_dir, sep='\t', schema=file_schema)
data_file_rows = data_file.rdd.collect()

for row in data_file_rows:
    signature = []
    schema = row.schema
    table = row.table
    all_data = spark.table('{0}{1}{2}'.format(schema, '.', table))

    if not all_data.rdd.isEmpty():
        all_data.cache()
        table_columns = [x for x in all_data.columns if x not in excluded_columns and x in ('ucc_key', 'claim_id', 'claimtransaction_id')]
        all_tokens = set()

        for column in table_columns:
            avg_len = \
                spark.sql(length_test_query.format(column, schema, table)). \
                    rdd. \
                    flatMap(list).collect()[0]
            if avg_len > char_limit:
                print("Average string length of {0}.{1}.{2} is {3} chars long: Too large to tokenize".
                      format(schema, table, column, avg_len))
            else:
                signature_chunks = all_data.freqItems(['{0}'.format(column)]). \
                    rdd.flatMap(lambda x: signature_multiset(x[0]))

                chunks = signature_chunks.collect()

                for chunk_set in chunks:
                    for chunk in chunk_set:
                        signature.append(chunk)

                sig_df = spark.createDataFrame([(app_name, run_type, schema, table, column, ','.join(signature))],
                                               schema=output_schema)
                sig_df.withColumn('write_ts', lit(str(datetime.now()))).write.json(output_dir, mode='append')
                print("wrote {0}.{1}.{2} to hdfs".format(schema, table, column))
    else:
        print("{0}.{1} is empty".format(schema, table))
    spark.catalog.uncacheTable('silkmoth_base')