# Surendar Dan Grebenisan
# Created on: 10-20-2017
# Purpose of creation: to find suspected columns to encrypt the data

from pyspark.sql import SparkSession, functions, Row
spark = SparkSession.builder.appName("Chevelle").enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
#from time import time
import time
timestr = time.strftime("%Y%m%d%H%M%S")

import sys
file_name="prfl"+sys.argv[1]+".txt"
#ip_dir='hdfs:///tmp/chev_files/'+file_name
#op_dir='/tmp/chev_output/'
#hdfs_file_path='/tmp/chev_files/'+file_name
#!/bin/sh
cluster_env=spark._jsc.hadoopConfiguration().get('dfs.internal.nameservices')[5:-3]

if(cluster_env == 'dev'): 
	env='dev'
	ENV='DEV'
elif(cluster_env == 'tst'):
        env='tst'
        ENV='TST'
elif(cluster_env == 'crt'):
        env='crt'
        ENV='CRT'
elif(cluster_env == 'prd'):
        env='prd'
        ENV='PRD'
else:
        print("I don't know what env I am on...exiting")
        exit

#ip_dir='hdfs:///'+ENV+'/EDW/DSODB/OPS/CHEVELLE/HYPER_PROF_RSLT/Input/'+file_name
#'op_dir='/'+ENV+'/EDW/DSODB/OPS/CHEVELLE/HYPER_PROF_RSLT/Output/'
#'hdfs_file_path='/'+ENV+'/EDW/DSODB/OPS/CHEVELLE/HYPER_PROF_RSLT/Input/'+file_name

ip_dir='hdfs:///'+ENV+'/EDW/DSODB/OPS/CHEVELLE/HYPER_PROF_RSLT/Input/'+file_name
op_dir='hdfs:///'+ENV+'/EDW/DSODB/OPS/CHEVELLE/HYPER_PROF_RSLT/Output/'
hdfs_file_path='hdfs:///'+ENV+'/EDW/DSODB/OPS/CHEVELLE/HYPER_PROF_RSLT/Input/'+file_name

print(ip_dir)
print(op_dir)
print(hdfs_file_path)


from time import time
import subprocess
def run_cmd(args_list):
	print('Running system command: {0}'.format(' '.join(args_list)))
	proc = subprocess.Popen(args_list, stdout=subprocess.PIPE,stderr=subprocess.PIPE)
	proc.communicate()
	return proc.returncode

#cmd = ['hadoop', 'fs', '-test', '-e', hdfs_file_path]
file = 1	
if (file):
	df_rdd=spark.sparkContext.textFile(ip_dir).map(lambda x: x.split("\t"))
	df=df_rdd.toDF(['asms','schema','table','sus_data_clasf','column','regexp'])
	for row in df.rdd.collect():
		try:
			tbl_st = time()
			asms=row.asms
			schema=row.schema
			tbl = row.table
			sus_data_clasf=row.sus_data_clasf
			column=row.column
			regex=row.regexp
			#regex=row.regexp[1:-1]
			table = row.schema+'.'+row.table
			sql="select "+column+" from "+table
			#print(sql)
			tbl_data=spark.sql(sql)
			if tbl_data.count()>0:
				tbl_data.persist()
				for col in [c for c in tbl_data.columns]:
					df_c=tbl_data.select(col).where("length("+col+")>0 and lower(trim("+col+ ")) not in (' ','null','n/a','unknown','unk','unspecified','no match row id','__not_applicable__')")
					col_count=df_c.count()
					col_smpl_count=col_count
					if col_count>=1000000:
						df_s=df_c.sample(True,0.1)
						col_smpl_count=df_s.count()
						df_s.registerTempTable('temp')
						s="select '"+asms+"' as asms,'" +schema+"' as schema,'"+tbl+"' as tbl,'"+col+"' as col,'"+sus_data_clasf+"' as data_clasf,'"+str(col_count)+"' as col_count,'"+str(col_smpl_count)+"' as sample_count,count(distinct("+col+")) as uq_count,sum(case when "+col+" REGEXP '"+regex+"' then 1 else 0 end) as hits ,max("+col+") as max_val,min("+col+") as min_val,avg(length("+col+")) as avg_len,'"+timestr+"' as ins_gmt_ts from temp"
						print(s)
						r=spark.sql(s)
						r.write.json(op_dir,mode='append')
						tt=str(time() - tbl_st)
						summary_list=sus_data_clasf,asms,schema,tbl,col,str(col_count),str(col_smpl_count),tt
						print(summary_list)
						tbl_st = time()
					if col_count>0:
						df_c.registerTempTable('temp')
						df_c.registerTempTable('temp')
						#s="select '"+asms+"' as asms,'" +schema+"' as schema,'"+tbl+"' as tbl,'"+col+"' as col,'"+sus_data_clasf+"' as data_clasf,'"+str(col_count)+"' as col_count,'"+str(col_smpl_count)+"' as sample_count,sum(case when "+col+" REGEXP '"+regex+"' then 1 else 0 end) as hits ,max("+col+") as max_val,min("+col+") as min_val,avg(length("+col+")) as avg_len,'"+timestr+"' as ins_gmt_ts from temp"
						s="select '"+asms+"' as asms,'" +schema+"' as schema,'"+tbl+"' as tbl,'"+col+"' as col,'"+sus_data_clasf+"' as data_clasf,'"+str(col_count)+"' as col_count,'"+str(col_smpl_count)+"' as sample_count,count(distinct("+col+")) as uq_count,sum(case when "+col+" REGEXP '"+regex+"' then 1 else 0 end) as hits ,max("+col+") as max_val,min("+col+") as min_val,avg(length("+col+")) as avg_len,'"+timestr+"' as ins_gmt_ts from temp"
						print(s)
						r=spark.sql(s)
						r.write.json(op_dir,mode='append')
						tt=str(time() - tbl_st)
						summary_list=sus_data_clasf,asms,schema,tbl,col,str(col_count),str(col_smpl_count),tt
						print(summary_list)
						tbl_st = time()
					else:
						tt=str(time() - tbl_st)
						summary_list=sus_data_clasf,asms,schema,tbl,col,str(col_count),ZERO,tt
						print(summary_list)
						tbl_st = time()
						tbl_data.unpersist()
			else:
				tt=str(time() - tbl_st)
				summary_list=sus_data_clasf,asms,schema,tbl,"TABLE",'ZERO','ZERO',tt
				print(summary_list)
		except:
			tt=str(time() - tbl_st)
			summary_list=sus_data_clasf,asms,schema,tbl,column,regex,"ERROR",tt
			print(summary_list)
	#hdfs_file_archive="/tmp/chev_archive/"+"prfl_"+timestr+ '.txt'
	#hdfs_file_archive='/'+ENV+'/EDW/DSODB/OPS/CHEVELLE/HYPER_PROF_RSLT/Archive/prfl_'+sys.argv[1]+'_'+timestr+ '.txt'
	#cmd_mv = ['hadoop', 'fs', '-mv',hdfs_file_path,hdfs_file_archive]
	#mvc=run_cmd(cmd_mv)	
	#cmd_perm = ['hadoop', 'fs', '-chmod','775',op_dir]
	#run_cmd(cmd_perm)
