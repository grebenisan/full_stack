# coding=utf-8
# Date: 06/14/2018
# Author: Dan Grebenisan
# Chevelle
# Calculates the cosine similarity between columns of a base table, and a compare table
# Spark Version: 2.1.1

import re
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql.functions import levenshtein, regexp_replace, max, trim, col, length, lit, create_map
from pyspark.ml.feature import VectorAssembler, Normalizer
from pyspark.mllib.linalg.distributed import RowMatrix
from pyspark.mllib.linalg import Vectors

spark = SparkSession.builder. \
    appName("Chevelle_Silkmoth"). \
    config("spark.pyspark.python", '/opt/oss/anaconda/envs/py2.7/bin/python'). \
    enableHiveSupport().getOrCreate()

base_table_nm = 'dl_edge_base_tg_15414_base_tgartp_dcidb.cars_siebel_ucc'
compare_table_nm = 'dl_edge_base_qis_12702_base_qisprodp_qis2owner.fact_claim'

table_name1 = base_table_nm.split('.')[1]
table_name2 = compare_table_nm.split('.')[1]
#if len [spark.table(t1).first()] > 0 and len[spark.table(t2).first()] > 0:
base_table = spark.sql("select * from {0} limit 1000".format(base_table_nm))
compare_table = spark.sql("select * from {0} limit 1000".format(compare_table_nm))

base_table.registerTempTable("{0}_temp".format(table_name1))
compare_table.registerTempTable("{0}_temp".format(table_name2))

spark.catalog.cacheTable("{0}_temp".format(table_name1))
spark.catalog.cacheTable("{0}_temp".format(table_name2))

excluded_columns = 'src_sys_crt_ts', 'src_sys_id', 'src_sys_iud_cd', \
                   'src_sys_upd_by', 'src_sys_upd_gmt_ts', 'src_sys_upd_ts', \
                   'dw_anomaly_flg', 'dw_extract_ts', 'dw_ins_gmt_ts', \
                   'dw_job_id', 'dw_mod_ts', 'dw_sum_sk', 'gg_src_sys_iud_cd', \
                   'gg_txn_numerator', 'gg_txn_csn', 'gg_src_sys_upd_ts', 'dw_sum_sk'

table1_columns = [x for x in base_table.columns if x not in excluded_columns]
table2_columns = [x for x in compare_table.columns if x not in excluded_columns]

def suggest_data_type(mean, mx):
    yyyymmdd = '(([0-9]{4}[\./-]?)([0][1]|[0][2]|[0][3]|[0][4]|[0][5]|[0][6]|[0][7]|[0][8]|[0][9]|[1][0]|[1][1]|[1][2])[\./-]?([0-9]{2}.*)'
    ddmmyyyy = '(^[0-9]{2}[\./-]?)([0][1]|[0][2]|[0][3]|[0][4]|[0][5]|[0][6]|[0][7]|[0][8]|[0][9]|[1][0]|[1][1]|[1][2])[\./-]?([0-9]{4}.*))'
    if mean is not None and re.match(r'(?=[^A-Za-z].)(?=^[-]?[\d]+[.]+[\d]*$).*', mx):
        return 'FLOAT'
    elif mean is not None and re.match(r'(?=[^A-Za-z].)(?=^[-]?[\d]+$).*', mx):
        return 'INT'
    elif re.match(r'({0}|{1})'.format(yyyymmdd, ddmmyyyy), mx):
        return 'DATE'
    else:
        return 'STRING'

def store_features(base, compare, column1, column2, results):
    base_tbl_col = '{0}.{1}'.format(base, column1)
    compare_tbl_col = '{0}.{1}'.format(compare, column2)
    output = spark.createDataFrame([(base_tbl_col, compare_tbl_col, results)],
                          schema=['base_col', 'compare_col', 'similarity_score'])
    output.write.json('/DEV/EDW/DSODB/OPS/CHEVELLE/HYPER_PROF_RSLT/Output_Silk', mode='append')
    print('Wrote {0}, and {1} to HDFS'.format(base_tbl_col, compare_tbl_col))

normalizer = Normalizer(inputCol="features", outputCol="normFeatures", p=1.0)
assembler = VectorAssembler(inputCols=["dist_cnt", "min_len", "max_len", "avg_len"], outputCol="features")
final_assembler = VectorAssembler(inputCols=["meta_features", "data_type"], outputCol="final_features")

for col_v in table1_columns:
    col_name = col_v
    for col_v2 in table2_columns:
        col_name2 = col_v2
        features = \
            spark.sql(
                """
                select 
                    case 
                    when count({0}) > 0 then
                    cast(count(distinct {0}) as int) else 0
                    end as dist_cnt,
                    cast(min(trim(length({0}))) as int) as min_len,
                    cast(max(trim(length({0}))) as int) as max_len, 
                    cast(avg(trim(length({0}))) as int) as avg_len
                    from {1}_temp
                """.format(col_v, table_name1)).replace('', 'N/A').fillna('N/A',
                                                                          subset=['dist_cnt', 'min_len', 'max_len',
                                                                                  'avg_len'])

        type_mapping = base_table.select(trim(col(col_name)).alias(col_name)).describe([col_name]). \
            na.fill("None").select(create_map(col('summary'), col(col_name))).fillna(0).rdd.flatMap(list).collect()

        mean = 'NaN' if type_mapping[1]['mean'] in (None, 'null', "None") else type_mapping[1]['mean']
        mx = type_mapping[4]['max']
        suggested_type = suggest_data_type(mean, mx)

        if suggested_type == 'FLOAT':
            d_type = 1
        elif suggested_type == 'INT':
            d_type = 2
        elif suggested_type == 'DATE':
            d_type = 3
        else:
            d_type = 4

        stats_feat = assembler.transform(features)
        stats_feat_results = stats_feat.select("features")

        l1NormData = normalizer.transform(stats_feat_results, {normalizer.p: float("inf")})
        normfeatures = l1NormData.select("normFeatures").rdd.flatMap(list).collect()[0]

        features_combined = [(normfeatures, d_type)]

        final_feat_frame = spark.createDataFrame(features_combined, ["meta_features", "data_type"])
        final_results = final_assembler.transform(final_feat_frame)
        combined_features = final_results.select("final_features").rdd.flatMap(list).collect()

        features2 = \
            spark.sql(
                """
                select 
                    case 
                    when count({0}) > 0 then
                    cast(count(distinct {0}) as int) else 0 end as dist_cnt,
                    cast(min(trim(length({0}))) as int) as min_len,
                    cast(max(trim(length({0}))) as int) as max_len, 
                    cast(avg(trim(length({0}))) as int) as avg_len
                    from {1}_temp
                """.format(col_v2, table_name2)).replace('', 'N/A').fillna(0, subset=['dist_cnt', 'min_len', 'max_len',
                                                                                      'avg_len'])

        type_mapping2 = compare_table.select(trim(col(col_name2)).alias(col_name2)).describe([col_name2]). \
            na.fill("None").select(create_map(col('summary'), col(col_name2))).fillna('N/A').rdd.flatMap(list).collect()

        mean2 = 'NaN' if type_mapping2[1]['mean'] in (None, 'null', "None") else type_mapping2[1]['mean']
        mx2 = type_mapping2[4]['max']
        suggested_type2 = suggest_data_type(mean2, mx2)

        if suggested_type2 == 'FLOAT':
            d_type2 = 1
        elif suggested_type2 == 'INT':
            d_type2 = 2
        elif suggested_type2 == 'DATE':
            d_type2 = 3
        else:
            d_type2 = 4

        stats_feat2 = assembler.transform(features2)
        stats_feat_results2 = stats_feat2.select("features")

        l1NormData2 = normalizer.transform(stats_feat_results2, {normalizer.p: float("inf")})
        normfeatures2 = l1NormData2.select("normFeatures").rdd.flatMap(list).collect()[0]

        features_combined2 = [(normfeatures2, d_type2)]

        final_feat_frame2 = spark.createDataFrame(features_combined2, ["meta_features", "data_type"])
        final_results2 = final_assembler.transform(final_feat_frame2)
        combined_features2 = final_results2.select("final_features").rdd.flatMap(list).collect()

        combined_vectors = [combined_features, combined_features2]
        vectors = spark.sparkContext.parallelize(combined_vectors)
        matrix = RowMatrix(vectors)
        similarity_score = matrix.columnSimilarities()
        score_results = similarity_score.entries.first().value
        store_features(table_name1, table_name2, col_v, col_v2, score_results)