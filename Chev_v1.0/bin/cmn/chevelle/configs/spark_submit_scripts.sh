SPARK SUBMIT SCRIPTS

##CONFIG FOR LOCAL TESTING##
/usr/bin/spark-submit \
--conf spark.pyspark.python=/opt/oss/anaconda/envs/py3.5/bin/python \
--conf spark.pyspark.driver.python=/opt/oss/anaconda/envs/py3.5/bin/python \
--files /etc/spark2/conf/hive-site.xml \
--py-files /data/commonScripts/util/__init__.py,\
/data/commonScripts/util/hyper_profiling_1_1.py \
/data/commonScripts/util/unit_test_runner.py

/usr/bin/spark-submit \
--conf spark.pyspark.python=/opt/oss/anaconda/envs/py3.5/bin/python \
--conf spark.pyspark.driver.python=/opt/oss/anaconda/envs/py3.5/bin/python \
--files /etc/spark2/conf/hive-site.xml \
--py-files /data/commonScripts/util/__init__.py,\
/data/commonScripts/util/hyper_profiling.py \
/data/commonScripts/util/end_to_end_test_runner.py

##CONFIG FOR CLUSTER TESTING##
/usr/bin/spark-submit \
--conf spark.pyspark.python=/opt/oss/anaconda/envs/py3.5/bin/python \
--conf spark.pyspark.driver.python=/opt/oss/anaconda/envs/py3.5/bin/python \
--conf spark.yarn.maxAppAttempts=0 \
--master yarn \
--deploy-mode cluster \
--num-executors 6 \
--executor-cores 4 \
--executor-memory 2g \
--driver-memory 2g \
--files /etc/hive/conf/hive-site.xml \
--py-files /data/commonScripts/util/__init__.py,\
/data/commonScripts/util/hyper_profiling_1_1.py \
/data/commonScripts/util/unit_test_runner.py

/usr/bin/spark-submit \
--conf spark.pyspark.python=/opt/oss/anaconda/envs/py3.5/bin/python \
--conf spark.pyspark.driver.python=/opt/oss/anaconda/envs/py3.5/bin/python \
--conf spark.yarn.maxAppAttempts=0 \
--master yarn \
--deploy-mode cluster \
--num-executors 6 \
--executor-cores 4 \
--executor-memory 2g \
--driver-memory 2g \
--files /etc/hive/conf/hive-site.xml \
--py-files /data/commonScripts/util/__init__.py,\
/data/commonScripts/util/hyper_profiling.py \
/data/commonScripts/util/end_to_end_test_runner.py

/usr/bin/spark-submit \
--conf spark.pyspark.python=/opt/oss/anaconda/envs/py3.5/bin/python \
--conf spark.pyspark.driver.python=/opt/oss/anaconda/envs/py3.5/bin/python \
--files /etc/spark2/conf/hive-site.xml \
--py-files /data/commonScripts/util/__init__.py,\
/data/commonScripts/util/hyper_profiling.py \
/data/commonScripts/util/data_classification.py 20180919_143055__j1__e010__t100__dc_list1.txt

/usr/bin/spark-submit \
--conf spark.pyspark.python=/opt/oss/anaconda/envs/py3.5/bin/python \
--conf spark.pyspark.driver.python=/opt/oss/anaconda/envs/py3.5/bin/python \
--files /etc/spark2/conf/hive-site.xml \
--py-files /data/commonScripts/util/__init__.py,\
/data/commonScripts/util/hyper_profiling_elk_logging.py \
/data/commonScripts/util/data_classification_elk_logging.py 20180919_143055__j1__e010__t100__dc_list1.txt

/usr/bin/spark-submit \
--conf spark.pyspark.python=/opt/oss/anaconda/envs/py3.5/bin/python \
--conf spark.pyspark.driver.python=/opt/oss/anaconda/envs/py3.5/bin/python \
--files /etc/spark2/conf/hive-site.xml \
--py-files /data/commonScripts/util/__init__.py,\
/data/commonScripts/util/hyper_profiling_1_1.py \
/data/commonScripts/util/data_classification_1_1.py 20180919_143055__j1__e010__t100__dc_list1.txt

/usr/bin/spark-submit \
--conf spark.pyspark.python=/opt/oss/anaconda/envs/py3.5/bin/python \
--conf spark.pyspark.driver.python=/opt/oss/anaconda/envs/py3.5/bin/python \
--conf spark.yarn.maxAppAttempts=0 \
--master yarn \
--deploy-mode cluster \
--num-executors 6 \
--executor-cores 4 \
--executor-memory 2g \
--driver-memory 2g \
--files /etc/hive/conf/hive-site.xml \
--py-files /data/commonScripts/util/__init__.py,\
/data/commonScripts/util/hyper_profiling.py \
/data/commonScripts/util/data_classification.py 20180919_143055__j1__e010__t100__dc_list1.txt


/data/commonScripts/util/hyper_profile_desc_stats.sh          -T DC              -s local 1
/data/commonScripts/util/hyper_profile_desc_stats.sh          -T DC  2
/data/commonScripts/util/hyper_profile_desc_stats.sh          -T DC  1           -R 1
