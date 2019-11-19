# coding=utf-8
# Author: Dan Grebenisan
# Original Creation Date: 03/18/2018
# Modified Date: 04/27/2018
# Filename: silkmoth.py
# Description: Creates signature of all columns in a table
# Spark Version: 2.x.x
##import string
import sys
from pyspark.sql.functions import lit, col, create_map, trim
from pyspark.sql.types import StringType, StructType, StructField, ArrayType
from pyspark.sql import SparkSession, functions
import time
from datetime import datetime
import math
import subprocess

reload(sys)
sys.setdefaultencoding('utf-8')

#Environment Variables
hdfs_cluster_nm = subprocess.Popen(['hdfs','getconf','-confKey','dfs.nameservices'], stdout=subprocess.PIPE)
out, err = hdfs_cluster_nm.communicate()
cluster_env = out[5:8]

if cluster_env == 'dev':
        env = 'dev'
        ENV = 'DEV'
elif cluster_env == 'tst':
        env = 'tst'
        ENV = 'TST'
elif cluster_env == 'crt':
        env = 'crt'
        ENV = 'CRT'
elif cluster_env == 'prd':
        env = 'prd'
        ENV = 'PRD'
else:
        print("I don't know what env I am on...exiting")
        sys.exit(1)

spark = SparkSession.builder.appName("Chevelle_Silkmoth").enableHiveSupport().getOrCreate()
#spark.sparkContext.setLogLevel("ERROR")

#Runtime Variables
local_time = time.localtime()
input_file = sys.argv[1]
input_dir = "/{0}/EDW/DSODB/OPS/CHEVELLE/HYPER_PROF_RSLT/Input/{1}".format(ENV,input_file)
output_dir = "/{0}/EDW/DSODB/OPS/CHEVELLE/HYPER_PROF_RSLT/Output_Silk".format(ENV)
char_limit = 50

#Algorithm Functions
relatedness_threshold = 0.8
max_q = int(relatedness_threshold / (1 - relatedness_threshold))
q = 3


def qChunks(string, q):
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


def qTokens(string, q):
    if len(string) >= 3:
        return [string[i:i + q] for i in range(len(string))]
    elif len(string) == 2:
        q = 2
        return [string[i:i + q] for i in range(len(string))]
    elif len(string) < 2:
        q = 1
        return [string[i:i + q] for i in range(len(string))]


def signatureMultiset(set_, q):
    return [qChunks(string, q) for string in set_]


def elementsWithToken(token, _sets):
    elements = []
    for i, set_ in enumerate(_sets):
        for j, string in enumerate(set_):
            if token in qTokens(string, q):
                elements.append((i, j))
    return elements


def tokenCost(token, inverse_index):
    if token in inverse_index:
        return len(inverse_index[token])
    else:
        print("not in dictionary")
        return 0


def tokenValue(token, set_, q):
    value = 0
    for string in set_:
        if token in qChunks(string, q):
            value += 1 / len(string)
    return value

#Application Variables
excluded_columns = 'src_sys_crt_ts', 'src_sys_id', 'src_sys_iud_cd', \
                   'src_sys_upd_by', 'src_sys_upd_gmt_ts', 'src_sys_upd_ts', \
                   'dw_anomaly_flg', 'dw_extract_ts', 'dw_ins_gmt_ts', \
                   'dw_job_id', 'dw_mod_ts', 'dw_sum_sk', 'gg_src_sys_iud_cd', \
                   'gg_txn_numerator', 'gg_txn_csn', 'gg_src_sys_upd_ts', 'dw_sum_sk'
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

data_file = spark.read.csv(input_dir, sep='\t', schema=file_schema)
data_file_rows = data_file.rdd.collect()

for row in data_file_rows:
    table_start_time = time.time()
    schema = row.schema
    table = row.table
    try:
        all_data = spark.table('{0}{1}{2}'.format(schema, '.', table))
        row_count = all_data.count()
        if row_count > 0:
            table_columns = [x for x in all_data.columns if x not in excluded_columns]
            all_data.registerTempTable('{0}'.format(table))
            all_tokens = set()
            for c in table_columns:
                avg_len = \
                    spark.sql(length_test_query.format(c, schema, table)). \
                    rdd. \
                    flatMap(list).collect()[0]
                if avg_len > char_limit:
                    print("Average string length of {0}.{1}.{2} is {3} chars long: Too large to tokenize".
                          format(schema, table, c, avg_len))
                else:
                    col_set = spark.sql("select {0} from {1}.{2}".format(c, schema, table)). \
                        replace('', 'N/A'). \
                        replace(' ', 'N/A'). \
                        dropna(). \
                        dropDuplicates(). \
                        rdd.flatMap(list).collect()
                    sets_ = [col_set]
                    multisets = [signatureMultiset(set_, q) for set_ in sets_]
                    # all_tokens = set()
                    for multiset in multisets:
                        for tokens in multiset:
                            for token in tokens:
                                all_tokens.add(token)

            for column in table_columns:
                column_start_time = time.time()
                sets = []
                avg_len = spark.sql(length_test_query.format(column, schema, table)). \
                    rdd. \
                    flatMap(list).collect()[0]
                if avg_len > char_limit:
                    print("Average string length of {0}.{1}.{2} is {3} chars long: Too large to index".
                          format(schema, table, column, avg_len))
                else:
                    values = spark.sql("select {0} from {1}.{2}".format(column, schema, table)). \
                            replace('', 'N/A'). \
                            replace(' ', 'N/A'). \
                            dropna(). \
                            dropDuplicates(). \
                            rdd.flatMap(list).collect()
                    for value in values:
                        sets.append(value)
                    # sets_ = [sets]

                    # Create inverse index of set against other sets
                    inverse_index = {}
                    for token in all_tokens:
                        inverse_index[token] = elementsWithToken(token, sets)

                    # Append contents of original set(column) to list
                    original_set = []
                    for s in sets:
                        original_set.append(s)

                    # Collect Signature and write results to Hadoop
                    matching_threshold = relatedness_threshold * len(original_set)
                    token_scores = {}
                    for string in original_set:
                        for token in qChunks(string, q):
                            token_scores[token] = tokenCost(token, inverse_index) / (
                                    tokenValue(token, original_set, q) + 0.0001)
                    priority_tokens = sorted(token_scores.keys(), key=lambda token: token_scores[token])
                    signature = []
                    unflattened_signature = {}
                    for string in original_set:
                        unflattened_signature[string] = []
                    score_contributions = {}
                    for string in original_set:
                        score_contributions[string] = 1
                    while (sum(score_contributions.values()) > matching_threshold):
                        token = priority_tokens[0]
                        signature.append(token)
                        del priority_tokens[0]
                        for string in original_set:
                            if token in qChunks(string, q):
                                unflattened_signature[string].append(token)
                                score_contributions[string] = len(string) / (
                                        len(string) + len(unflattened_signature[string]))

                    sig_df = spark.createDataFrame([(app_name, run_type, schema, table, column, ','.join(signature))],
                                                   schema=output_schema)
                    sig_df.withColumn('write_ts', lit(str(datetime.now()))).write.json(output_dir, mode='append')
                    column_process_time = str(time.time() - column_start_time)
                    print(schema, table, column, 'COLUMN', 'INFO', column_process_time)
        else:
            print(schema, table, 'TABLE', 'WARNING', 'NO_DATA', 'NOT_PROCESSED')
    except (RuntimeError, EnvironmentError, SystemExit, KeyboardInterrupt):
        raise
    except Exception as e:
        print(schema, table,'TABLE', 'ERROR', '0', e)
        sys.exit(1)
    print(schema, table, 'TABLE', 'INFO', str(time.time() - table_start_time))
