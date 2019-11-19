
hdfs_cluster_nm=`hdfs getconf -confKey dfs.nameservices`
cluster_env=${hdfs_cluster_nm:5:3}

if [ ${cluster_env} = 'dev' ]
then
        env='dev'
        ENV='DEV'
elif [ ${cluster_env} = 'tst' ]
then
        env='tst'
        ENV='TST'
elif [ ${cluster_env} = 'crt' ]
then
        env='crt'
        ENV='CRT'
elif [ ${cluster_env} = 'prd' ]
then
        env='prd'
        ENV='PRD'
else
        echo "I don't know what env I am on...exiting"
        exit 1
fi

file_name="prfl"$1".txt"
hdfs_file_path=/$ENV/EDW/DSODB/OPS/CHEVELLE/HYPER_PROF_RSLT/Input/$file_name
if hdfs dfs -test -e $hdfs_file_path
then
	echo "Processing "$file_name
	/usr/bin/spark-submit \
	--name CHEVELLE \
	--master yarn \
	--deploy-mode cluster \
	--driver-memory 1g \
	--executor-memory 1g \
	--executor-cores 1 \
	--num-executors 200 \
	--files /usr/hdp/current/hive-client/conf/hive-site.xml \
	--conf spark.yarn.appMasterEnv.HADOOP_USER_NAME=$USERID \
	--conf spark.yarn.maxAppAttempts=1 \
	/data/commonScripts/util/hyper_profile.py $1

        echo "JOB Completed"
        x=`date +%Y%m%d%H%M%S`
        hadoop fs -mv /$ENV/EDW/DSODB/OPS/CHEVELLE/HYPER_PROF_RSLT/Input/$file_name /$ENV/EDW/DSODB/OPS/CHEVELLE/HYPER_PROF_RSLT/Archive/$file_name_$x.txt
        echo "file archived"
        hadoop fs -chmod 775 /$ENV/EDW/DSODB/OPS/CHEVELLE/HYPER_PROF_RSLT/Output_Desc/
else
	echo "source file not exists to process"
fi

#x=`date +%Y%m%d%H%M%S`
#/usr/bin/spark-submit --master yarn --deploy-mode cluster --num-executors 50 --executor-memory 1g --executor-cores 2 /data/commonScripts/util/hyper_profile.py $1
#echo $1 | mailx -s "logfile" -a /data/commonScripts/util/hyperr_profile_$1_$x.log surendar.pandilla@gm.com
#rm -f /data/commonScripts/util/hyperr_profile_$1_$x.log
