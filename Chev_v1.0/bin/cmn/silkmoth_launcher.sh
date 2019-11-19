#!/bin/bash
#--------------------------------------------------------------------------------------
# Usage:   ./$0   FileNum   [BytesChunk]
# Ex1:     ./$0   10        
# Ex2:     ./$0   10        100000000
#--------------------------------------------------------------------------------------

function RoundUpToNext10
{
	#Round up to next decimal value (ex: 2=> 10; 12=> 20; 22=> 30;  119=>120 ...)
	#This allows better grouping to submit to Spark Cluster
	local str="$1";
	str="0${str}";				#Add 1 leading 0 (in case it is single digit)
	local len=${#str}
	local s1=${str:0:len-1};		#Remove last (rightmost) digit (for roundup)
	local s2=$(printf $s1|sed 's:^0*::');	#Remove leading 0s
	local s3="$((s2+1))0";            	#Add 1, then and trailing 0 (because it was removed above)
	#printf "str=$str; len=$len; s1=$s1; s2=$s2; s3=$s3; numExecutors=$numExecutors\n"
	numExecutors=$s3
	#printf "\t\tAFTER  RoundUpToNext10: numExecutors=$numExecutors\n"
}


function CalculateNumExecutor
{
        fileSize="$1";
	local chunkSize="$2";
	#Calculate numExecutors based on fileSize
	numExecutors=$((($fileSize+$chunkSize-1)/$chunkSize))
        if   [[ $numExecutors -lt 1     ]] ; then let numExecutors=1;
        elif [[ $numExecutors -gt 200   ]] ; then let numExecutors=200;
        fi;

	#printf "\t\tBEFORE RoundUpToNext10: numExecutors=$numExecutors\n"
	RoundUpToNext10 $numExecutors;  #If this function not wanted, just comment this 1 line out
}


function DescribeAllTables 
{
	inputFile="$1";
	outputFile_HQL="$2";
	outputFile_TXT="$3";
	local RC;
	printf "==> function DescribeAllTables
		inputFile	= $inputFile
		outputFile_HQL	= $outputFile_HQL
		outputFile_TXT	= $outputFile_TXT\n\n";

	rm $outputFile >/dev/null 2>&1
	totalFiles=$(wc -l $inputFile|cut -d" " -f1);
	let N=0
	for table in $(sed -e 's:\t:.:' $inputFile)	#Combine 2 words into 1 word for FOR loop
	do
		N=$((N+1));
		wordscount=$(echo $table|awk -F"." 'END {print NF}');
		if [[ $wordscount -ne 2 ]]
		then
			printf "\t$N/$totalFiles)\tERROR: Bad input line: \"$table\" (wordscount=$wordscount) ==> IGNORE THIS LINE\n"
			continue;
		fi;
		newLine=$(echo $table | sed -e 's:\t:.:' -e 's:$:;:' -e 's:^:describe extended :' );
		# Ex: describe extended cca_edge_base_atds_58616_atds_atds_core.aftersales_contract;
		printf "\t$N/$totalFiles)\tDESCRIBE EXTENDED $table:\n"
		printf "$newLine\n" >> $outputFile_HQL
	done;

	hive -S -hiveconf hive.cli.errors.ignore=true  -f $outputFile_HQL > $outputFile_TXT 2>&1
	RC=$?
	#printf "\nRC=$RC\n\n\n"
	#Ignore errors.  DO NOT exit.  If table is not found then it just get default num_executors
	#if [[ $RC -ne 0 ]]
	#then
	#	printf "\n\n\nERROR: Hive failed when DESCRIBE tables; RC=$RC\n\n\n"
	#	exit -1;
	#fi;

	printf "<== function DescribeAllTables\n\n";
}


function ExtractFileSizes
{
	inputFile="$1";
	allTablesDescTXT="$2";
	chunkSize="$3";
	outputFile="$4";

	local maxTableLength=$( cut -f1 $inputFile | awk '{print length ($1)}' | sort -n -r|head -1)
	local maxSchemaLength=$(cut -f2 $inputFile | awk '{print length ($1)}' | sort -n -r|head -1)
	local totalLength=$(( maxTableLength + maxSchemaLength + 2))

	printf "==> function ExtractFileSizes;
		inputFile		= $inputFile
		allTablesDescTXT	= $allTablesDescTXT
		chunkSize		= $chunkSize
		outputFile		= $outputFile
		maxTableLength		= $maxTableLength
		maxSchemaLength		= $maxSchemaLength
		totalLength		= $totalLength\n";

	rm $outputFile >/dev/null 2>&1
	totalFiles=$(wc -l $inputFile|cut -d" " -f1);
	printf "\n\tExtracting Table Size from dictionary and calculate num-executors based on chunkSize=$chunkSize:\n\n";
	let N=0
	for line in $(sed -e 's:\t:.:' $inputFile)	#Combine into 1 words for FOR loop
	do
		N=$((N+1));
		wordscount=$(echo $line|awk -F"." 'END {print NF}');
		if [[ $wordscount -ne 2 ]]
		then
			printf "\t$N/$totalFiles)\tERROR: Bad input line: \"$line\" (wordscount=$wordscount) ==> IGNORE THIS LINE\n"
			continue;
		fi;

		let numExecutors=0
		schema=$(    echo $line   |cut -d"." -f1)
		table=$(     echo $line   |cut -d"." -f2)
		schema_lc=$( echo $schema |tr "[A-Z]" "[a-z]" );	# LowerCase to match hives Describe Extended
		table_lc=$(  echo $table  |tr "[A-Z]" "[a-z]" );

		#Example:   FAILED: SemanticException [Error 10001]: Table not found E.f
		badcount=$(grep -c "FAILED: SemanticException \[Error 10001\]: Table not found ${schema}.${table}" $allTablesDescTXT)

		printf "\t$N/$totalFiles)\t%-${totalLength}s:"  "${schema}.${table}"
		if [[ $badcount -gt 0 ]]
		then
			printf "  TABLE NOT FOUND IN HIVE ==> Ignore this table (badcount=$badcount)\n"
			continue;
		fi;

		# Sometimes the Hive's DESCRIBE EXTENDED splits into 2 or more lines if table has too many columns
		# ==> Use awk to go across multiple lines
		# Note: Case-sensitive of these words
		fileSize=$(	awk  "/tableName:${table_lc}, dbName:${schema_lc}/,/tableType:EXTERNAL_TABLE)/"   $allTablesDescTXT |\
				grep "totalSize=" |\
				sed -e 's:^\(.*totalSize=\)\([0-9]*\).*$:\2:'
			);
		RC=$?
		if [[ $RC -eq 0 ]]
		then
			if [[ "$fileSize" == "" ]]
			then	#Sometines there is No fileSize info for this table from the DESCRIBE command(ie, has not been analyzed yet)
				let numExecutors=40;	#set to default = 40
				printf "  fileSize='$fileSize'\t\t==> numExecutors=$numExecutors (default due to Null Size)\n"

				# Possible Improvement:  If fileSize=="" then combine them and re-issue Analyze commmands (in 1 hive call)
			else
				CalculateNumExecutor  $fileSize  $chunkSize
				printf "  fileSize='$fileSize'\t==> numExecutors=$numExecutors (after roundup)\n"
			fi;
			printf "$numExecutors,${schema}.$table,$fileSize\n" >> $outputFile
		else
			printf "Table $table NOT EXIST.  Ignore!\n"
		fi;
	done;
	printf "<== function ExtractFileSizes\n\n"
}


function SortFileSizes
{
	inputFile="$1"
	outputFile="$2"
	sort -r -t"," -k1 $inputFile > $outputFile
}


function Execute
{
	inputFile="$1"
	distinctNumExecutors="$2"
	sameGroupFile="/tmp/Silkmoth_SameExecutorGroup_$$.txt";

	printf "==> function Execute
	inputFile            = $inputFile
	distinctNumExecutors = $distinctNumExecutors
	sameGroupFile        = $sameGroupFile\n";

	#printf "inputFile:\n"; head -10 $inputFile printf "\n";

	printf "\n\nEXECUTE ...\n"
	let n=0;
	for num_excutors in $(cat $distinctNumExecutors)
	do
		n=$((n+1))
		grep "^${num_excutors},"  $inputFile  > $sameGroupFile
		printf "\n\tGroup #$n) Num-Executors = $num_excutors\n"
		printf "\n\t\tA) List of tables for this Group:\n"
		sed -e "s:^:\t\t\t:" $sameGroupFile

		outFile="Silkmoth_$$_NumExeGroup_${num_excutors}.txt";  
			# Do not put /tmp in this file name as it will go into HDFS also
		printf "\n\t\tB) ReFormat file ($outFile)\n"
		cut -d"," -f2 $sameGroupFile | sed "s:\.:\t:" > $outFile
		sed -e "s:^:\t\t\t:" $outFile
	
		printf "\n\t\tC) Put file, $outFile, into HDFS:\n"
		hadoop fs -put $outFile /${ENV}/EDW/DSODB/OPS/CHEVELLE/HYPER_PROF_RSLT/Input/
		RC1=$?
		printf "\t\t\tRC1=$RC1\n"
		if [[ $RC1 -ne 0 ]]
		then
			printf "\n\nERROR:  Cannot put file ($outFile) into Hadoop; RC1=$RC1\n\n";
			exit -1;
		fi;
	
		printf "\n\t\tD) Submit File, $outfile, to Spark-Cluster with num_excutors=$num_excutors\n"
		/usr/bin/spark-submit			\
			--name            Chevelle_Silkmoth	\
			--master          yarn		\
			--deploy-mode     cluster	\
			--driver-memory   2g		\
			--executor-memory 2g		\
			--executor-cores  2			\
			--num-executors   $num_excutors	\
			--files /usr/hdp/current/hive-client/conf/hive-site.xml	\
			--conf spark.yarn.appMasterEnv.HADOOP_USER_NAME=$USERID	\
			--conf spark.yarn.maxAppAttempts=1			\
			/data/commonScripts/util/silkmoth.py $outFile

		RC2=$?
		if [[ $RC2 -ne 0 ]]
		then
			printf "\n\nERROR: Python Job failed; RC2=$RC2\n\n";
			exit -1;
		else
			printf "\t\t\tRC2=$RC2; Python Job completed successfully\n";
		fi;

		x=`date +%Y%m%d_%H%M%S`
		bn=$(basename $outFile .txt)
		archiveName="${bn}_${x}.txt";
		printf "\n\t\tE) Archiving file:  ${archiveName}\n";
		hadoop fs -mv	/$ENV/EDW/DSODB/OPS/CHEVELLE/HYPER_PROF_RSLT/Input/$outFile	\
				/$ENV/EDW/DSODB/OPS/CHEVELLE/HYPER_PROF_RSLT/Archive/${archiveName}
		hadoop fs -chmod 775 /$ENV/EDW/DSODB/OPS/CHEVELLE/HYPER_PROF_RSLT/Output_Desc/
	
		rm $outFile >/dev/null 2>&1
		printf "\n\n";
	done;
	printf "<== function Execute\n\n";
}


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

if [[ "$2" == "" ]]
then
	chunkSize=100000000;	#100M
else
	chunkSize=$2;
	if [[ $chunkSize -lt 100000000 ]]
	then
		chunkSize=100000000;	#Minimum: 100,000,000 = 100M
	fi;
fi;


inputFile="/tmp/silkmoth_tbl_list${1}.txt"
rm $inputFile >/dev/null 2>&1

#Extract file from HDFS (containing list of schema.tables) to calculate num_executors for each table
printf "\n\nGet input file from HDFS silkmoth_tbl_list${1}.txt   into file:  ${inputFile}\n"
hadoop fs -get /${ENV}/EDW/DSODB/OPS/CHEVELLE/HYPER_PROF_RSLT/Input/silkmoth_tbl_list${1}.txt    $inputFile
RC=$?
printf "RC=$RC\n"
if [[ $RC -ne 0 ]]
then
	printf "\n\nWARNING:  Cannot get inputFile, silkmoth_tbl_list${1}.txt, from Hadoop; RC=$RC\n\n";
	exit 0;
fi;


allTablesDesc_HQL="/tmp/Silkmoth_allTablesDesc_$$.hql"
allTablesDesc_TXT="/tmp/Silkmoth_allTablesDesc_$$.txt"
allFileSizes="/tmp/Silkmoth_allFileSizes_$$.txt"
allFileSizesSorted="/tmp/Silkmoth_allFileSizesSorted_$$.txt"
distinctNumExecutors="/tmp/Silkmoth_distinctNumExecutors_$$.txt"

printf "
chunkSize            = $chunkSize bytes
inputFile            = $inputFile
allTablesDesc_HQL    = $allTablesDesc_HQL
allTablesDesc_TXT    = $allTablesDesc_TXT
allFileSizes         = $allFileSizes
allFileSizesSorted   = $allFileSizesSorted
distinctNumExecutors = $distinctNumExecutors
\n";

DescribeAllTables $inputFile     $allTablesDesc_HQL   $allTablesDesc_TXT
ExtractFileSizes  $inputFile     $allTablesDesc_TXT   $chunkSize   $allFileSizes
SortFileSizes     $allFileSizes  $allFileSizesSorted
cut -d"," -f1     $allFileSizesSorted | sort -u >$distinctNumExecutors

printf "\n\nList of Tables and their num-executors: ($allFileSizesSorted)\n";     sed 's:^:\t:' $allFileSizesSorted
printf "\n\nDistinct list of Num_Executors Groups:  ($distinctNumExecutors)\n";   sed 's:^:\t:' $distinctNumExecutors
printf "\n\n";

Execute  $allFileSizesSorted   $distinctNumExecutors

x=`date +%Y%m%d_%H%M%S`
bn=$(basename $inputFile .txt)
archiveName="${bn}_${x}.txt";
printf "\n\nMove HDFS input file to HDFS Archive with name  $archiveName:\n"
hadoop fs -mv /${ENV}/EDW/DSODB/OPS/CHEVELLE/HYPER_PROF_RSLT/Input/silkmoth_tbl_list${1}.txt    /${ENV}/EDW/DSODB/OPS/CHEVELLE/HYPER_PROF_RSLT/Archive/${archiveName}
RC=$?
if [[ $RC -ne 0 ]]
then
	printf "\n\n\nERROR: Cannot move input file ($bn) to Archive ($archiveName); RC=$RC\n\n\n"
	exit -1;
fi;

rm $allTablesDesc_HQL $allTablesDesc_TXT $allFileSizes $allFileSizesSorted $distinctNumExecutors >/dev/null 2>&1
echo "JOB Completed"

