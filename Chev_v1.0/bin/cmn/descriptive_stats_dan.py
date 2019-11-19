# coding=utf-8
# Date: 06/20/2018
# Author: Dan Grebenisan
# Chevelle
# Calculates descriptive statistics over hyper ingested data, with datatype suggestion
# Spark Version: 2.1.1

import sys
import re
from datetime import datetime
from pyspark.sql.types import StringType, StructType, StructField, ArrayType, IntegerType
from pyspark.sql import SparkSession, functions
from pyspark.sql.functions import lit, col, create_map, trim

reload(sys)
sys.setdefaultencoding('utf-8')

spark = SparkSession.builder. \
    config("spark.task.cpus", 2). \
    config("spark.pyspark.python", '/opt/oss/anaconda/envs/py2.7/bin/python'). \
    appName("Chevelle"). \
    enableHiveSupport(). \
    getOrCreate()

app_name = 'Chevelle'
plumbing_list = 'src_sys_crt_ts', 'src_sys_id', 'src_sys_iud_cd', \
                'src_sys_upd_by', 'src_sys_upd_gmt_ts', 'src_sys_upd_ts',\
                'dw_anomaly_flg', 'dw_extract_ts', 'dw_ins_gmt_ts', \
                'dw_job_id', 'dw_mod_ts', 'dw_sum_sk', 'gg_src_sys_iud_cd',\
                'gg_txn_numerator', 'gg_txn_csn', 'gg_src_sys_upd_ts', 'dw_sum_sk'
base_sql = \
    """
    select 
    '{4}' as schema_name, 
    '{2}' as table_name, 
    '{0}' as column_name, 
    '{3}' as run_type, 
    count({0}) as total_count, 
    count(DISTINCT {0}) as unique_rows, 
    sum(case when ({0}) in (' ','n/a','unknown','unk','unspecified','no match row id','__not_applicable__') 
    then 1 else 0 end) as unknown, 
    sum(case when {0} is null then 1 else 0 end) as null_count, 
    max(trim(length({0}))) as max_length, 
    avg(trim(length({0}))) as avg_length, 
    '{8}' as mean_value, 
    '{6}' as std_deviation, 
    trim(min({0})) as min_value, 
    trim('{7}') as max_value,
    '{5}' as suggested_type 
    from {1}
    """

file_name = "tbl_list2.txt"
input_dir = "/DEV/EDW/DSODB/OPS/CHEVELLE/HYPER_PROF_RSLT/Input/{0}".format(file_name)
output_dir = "/DEV/EDW/DSODB/OPS/CHEVELLE/HYPER_PROF_RSLT_DAN/Data"

file_schema = StructType([StructField("schema", StringType()),StructField("table", StringType())])

def suggest_data_type(mean, mx):
    yyyymmdd = '(([0-9]{4}[\./-]?)([0][1]|[0][2]|[0][3]|[0][4]|[0][5]|[0][6]|[0][7]|[0][8]|[0][9]|[1][0]|[1][1]|[1][2])[\./-]?([0-9]{2}.*)'
    ddmmyyyy = '(^[0-9]{2}[\./-]?)([0][1]|[0][2]|[0][3]|[0][4]|[0][5]|[0][6]|[0][7]|[0][8]|[0][9]|[1][0]|[1][1]|[1][2])[\./-]?([0-9]{4}.*))'
    if mean not in ('NaN') and re.match(r'(?=[^A-Za-z].)(?=^[-]?[\d]+[.]+[\d]*$).*', mx):
        return 'FLOAT'
    elif mean not in ('NaN') and re.match(r'(?=[^A-Za-z])(?=^[-]?[\d]+$).*', mx):
        return 'INT'
    elif re.match(r'({0}|{1})'.format(yyyymmdd, ddmmyyyy), mx):
        return 'DATE'
    else:
        return 'STRING'

data_file = spark.read.csv(input_dir, sep='\t', schema=file_schema)
data_file_rows = data_file.rdd.collect()

for row in data_file_rows:
    schema = row.schema
    table = row.table
    try:
        all_data = spark.table('{0}{1}{2}'.format(schema, '.', table))
        columns = [x for x in all_data.columns if x not in plumbing_list]
        if not all_data.rdd.isEmpty():
            all_data.registerTempTable("current_table")
            spark.catalog.cacheTable("current_table")
            for column_name in columns:
                type_mapping = \
                    all_data.select(trim(col(column_name)).alias(column_name)). \
                        describe([column_name]).\
                    na.fill("None").select(create_map(col('summary'), col(column_name))).rdd.flatMap(list).collect()
                mean = 'NaN' if type_mapping[1]['mean'] in (None,"None") else type_mapping[1]['mean']
                mx = type_mapping[4]['max']
                stddev = 'NaN' if type_mapping[2]['stddev'] in (None,"None") else type_mapping[2]['stddev']
                suggested_type = suggest_data_type(mean, mx)
                results = \
                    spark.sql(base_sql.format(column_name,
                        "current_table",table,'basic stats',schema,suggested_type,stddev,mx,mean)). \
                    na.fill('unknown')
                results.\
                    withColumn('write_ts', lit(str(datetime.now()))).\
                    write.\
                    json("{0}".format(output_dir), mode='append')
                print('Table Complete\t{0}\t{1}\t{2}\t1'.format(schema, table, column_name))
        else:
            print("table {0}.{1} has no rows".format(schema,table))
    except (RuntimeError, EnvironmentError, SystemExit, KeyboardInterrupt):
        raise
    except Exception as e:
        print("Error\t{0}\t{1}\t{2}".format(schema, table, e))
        sys.exit(1)
    spark.catalog.uncacheTable("current_table")

