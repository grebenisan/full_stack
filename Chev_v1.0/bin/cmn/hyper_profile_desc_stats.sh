#!/bin/bash
#-----------------------------------------------------------------------------------------------------
# Authors:
# Dan Grebenisan		Oct-20-2017	v1      
#-----------------------------------------------------------------------------------------------------
# How to run:
#
# 1)  Put inputfile into HDFS:
#	hadoop fs -put -f tbl_list5.txt    "/DEV/EDW/DSODB/OPS/CHEVELLE/HYPER_PROF_RSLT/Input";
#	or:
#	hadoop fs -put -f dc_list5.txt     "/DEV/EDW/DSODB/OPS/CHEVELLE/HYPER_PROF_RSLT/Input";
#
# 2)  Execute this shell script, using one of these:
#	A) Run in CLUSTER: (There are many options.  These are just 2 for sample)
#		/data/commonScripts/util/hyper_profile_desc_stats.sh        5		#<== must be same # as in step 1
#		/data/commonScripts/util/hyper_profile_desc_stats.sh -d     5		# -d = turn on Debug
#		/data/commonScripts/util/hyper_profile_desc_stats.sh    -R  5		# Reset (igore all prev checkpoints)
#		/data/commonScripts/util/hyper_profile_desc_stats.sh -d -R  5		# Reset and turn on Debug
#
# OR	B) Run in LOCAL MODE:
#		/data/commonScripts/util/hyper_profile_desc_stats.sh    -s local    -d  -R  5
#
#-----------------------------------------------------------------------------------------------------

function GetElapsedTime
{
	local l_startTime_EPOC=$1

	#local l_startTime_EPOC="$(date +%s)"
	#endTime="$(date +"%Y%m%d %H:%M:%S")"
	l_endTime_EPOC="$(date +%s)"
	l_seconds=$((l_endTime_EPOC - l_startTime_EPOC));
	printf "(Took: $l_seconds secs)"
}

function ConvertSECONDSToDayHHMMSS
{
	local secs=$1;

	local REMAIN
	local DAYS=$((secs/86400));	#printf "$DAYS days\n"; 
	REMAIN=$((secs%86400));		#echo $REMAIN; 
	HOURS=$((REMAIN/3600));		#printf "$DAYS days $HOURS hrs\n"; 
	REMAIN=$((REMAIN%3600)); 	#echo $REMAIN; 
	MINS=$((REMAIN/60));		#printf "$DAYS days $HOURS:$MINS\n"; 
	REMAIN=$((REMAIN%60));		#echo $REMAIN; 
	printf "%s days %02d:%02d:%02d"   "$DAYS"  "$HOURS"  "$MINS"   "$REMAIN"
		#02d  to display 2 digits, as in:  HH:MI:SS
}


function GetCurrTime 
{
	local howmany=$1
	Austin="$(TZ=US/Central date +"%Y/%m/%d %H:%M:%S")";
	Eastern="$(TZ=US/Eastern date +"%Y/%m/%d %H:%M:%S")"
	GMT="$(date --utc +"%Y/%m/%d %H:%M:%S")";
	if [[ -z "$1" ]]
	then let howmany=1;
	else let howmany=$1;
	fi;

	case $howmany in
		0) printf "[Austin=${Austin}]";;
		1) printf "[Austin=${Austin}]";;
		2) printf "[Austin=${Austin}] [Local=${Eastern}]";;
		*) printf "[Austin=${Austin}] [Local=${Eastern}] [GMT=${GMT}]";;
	esac;
}


function Usage
{
	local bn=$(basename $0)
        printf "\n\n\n
Usage:  $bn [options]   HDFSscriptName

Options:	-h = Help
		-c = DF_COLUMNS_BATCH_SIZE	(Default: 100 columns)
		-d = Debug			(Default: NO;             2 valid values:  YES, NO)
		-e = Mail List			(Default: None;           Email names to send emails)
		-H = HDFS RootDir 		(Default: /\$ENV/EDW/DSODB/OPS/CHEVELLE/HYPER_PROF_RSLT/)
		-n = DeployMode			(Default: CLUSTER;        2 valid values:  Cluster or Client)
		-L = Root Log Dir		(Default: CHEVELLE_STAT;  Based on Project)
		-p = Python Program		(Default: hyper_profile_desc_stats.py;)
		-P = ProjectName		(Default: CHEVELLE_STAT;  Used to build OS LogDir)
		-r = Max Retry			(Deafult: NO;             Remove all previous generated scripts (OS and HDFS) to start over.)
		-r = ResetNoRun			(Deafult: NO;             Remove all previous generated scripts (OS and HDFS) to start over.)
		-R = Reset 			(Deafult: NO;             Remove all previous generated scripts (OS and HDFS) to start over.)
		-s = SubmitMaster		(Default: YARN;           2 valid values:  Local   or Yarn)
		-t = Max_Retry_Threshold 
		-Y = Yarn SparkName		(Defualt: CHEVELLE_STAT;  Show up in YARN page, for easy query)
	\n\n\n";

	exit 0;
} #function Usage


function Func_Trap_INT
{
	printf "\n\n==> function Func_Trap_INT\t\t$(GetCurrTime))\t\t$(GetCurrTime)\n"
	local job_Status  RETURN_CODE   N
	local totalFilesOfInputFile=0;

	if [[ -f $G_INPUT_LIST ]]; then totalFilesOfInputFile=$(wc -l $G_INPUT_LIST | cut -d" " -f1); fi;

	printf "
	HDFS_INPUT_DIR			= hadoop fs -ls -R $G_HDFS_INPUT_DIR
	HDFS_ARCHIVE_DIR		= hadoop fs -ls -R $G_HDFS_ARCHIVE_DIR
	HDFS_OUTPUT_DIR	(Json)		= hadoop fs -ls -R $G_HDFS_OUTPUT_DIR
	HDFS_CHECKPOINT_ROOT_DIR	= hadoop fs -ls -R $G_HDFS_CHECKPOINT_ROOT_DIR

	OS CHECKPOINT_DIR		= ls -R $G_CHECKPOINT_DIR
	OS CHECKPOINT_ARCHIVE_DIR	= ls -R $G_CHECKPOINT_ARCHIVE_DIR

	(Linux)
	TORUN_DIR	= ls -ltr $G_TORUN_DIR
	ARCHIVE_DIR	= ls -ltr $G_ARCHIVE_LINUX_DIR
	LOG_DIR		= ls -ltr $G_LOG_DIR
	PRIOR_TORUN_DIR	= ls -ltr $G_PRIOR_TORUN_DIR
	PRIOR_APP_DIR   = ls -ltr $G_PRIOR_APP_DIR

	LAST STEP TO COMPLETE	= \"$G_LAST_STEP_TO_COMPLETE\"
	STEP COMPLETED SO FAR	= \"$G_JOB_STEPS_COMPLETED_SO_FAR\"\n"

	job_Status="FAIL";
	RETURN_CODE=1;
	if [[ "$G_JOB_STEPS_COMPLETED_SO_FAR" == "$G_LAST_STEP_TO_COMPLETE" ]] && [[ "${G_JOB_STATUS}" == "SUCCESS" ]]
	then	job_Status="SUCCESS";
		RETURN_CODE=0;
	fi;

	if [[ -f $G_EMAIL_FILE ]]
	then
		# Display status of SplitFiles:
		if [[ -s $G_EMAIL_TEMP_FILE ]] 
		then	printf "Status of SplitFiles:\n"	>> $G_EMAIL_FILE
			cat $G_EMAIL_TEMP_FILE  | sed 's:^:\t:'	>> $G_EMAIL_FILE
			printf "\n"				>> $G_EMAIL_FILE
		else	printf "Cannot find file containing Status of SplitFiles: \"$G_EMAIL_TEMP_FILE\"\n\n"	>> $G_EMAIL_FILE
			printf "\n"				>> $G_EMAIL_FILE
		fi;

		# Summary File to Email:
		printf "\n\n
	FINAL SUMMARY:

		Input File:	$G_INPUT_LIST ($totalFilesOfInputFile tables)
		START:		$G_SESSION_DATE_STR
		COMPLETE:	$(date +"%Y%m%d %H:%M:%S")

		Total Split Files Submitted = $G_TOTAL_SPLITFILES
	
		SUCCESS	SplitFiles = $G_TOTAL_SUCCESS_SPLITFILES
		FAILED	SplitFiles = $G_TOTAL_FAIL_SPLITFILES
	
		====================================================
		===== FINAL  JOB  STATUS     =     $job_Status =====
		===== BATCH_ID = $G_BATCH_ID  
		====================================================
		\n" >> $G_EMAIL_FILE
		SendMail "$G_MAIL_LIST" "$G_EMAIL_FILE"  "$job_Status"
	fi;


	case "$G_PROGRAM_TYPE" in	#--{
	"BSTAT")	
		if [[ -s $G_ALL_YARN_LOGS ]]
		then
			printf "\n\nFINAL TABLE SUMMARY of each SplitFile: (G_ALL_YARN_LOGS = view  $G_ALL_YARN_LOGS )\n"
			let N=0;
			for f in $(cat $G_ALL_YARN_LOGS | sort -u)
			do
				N=$((N+1));
				printf "$N) Yarn log:  view $f\n"
				sed -n '/BEGIN_FINAL_SUMMARY/,/END_FINAL_SUMMARY/p' $f| grep -E "^\+|^\|" 
				printf "\n\n"
			done;
		else
			printf "\n\n\n(Cannot display FINAL TABLE_SUMMARY as file G_ALL_YARN_LOGS is NOT available ($G_ALL_YARN_LOGS)\n\n\n"
		fi;
		;;

	*)	:
		;;
	esac;	#--}

	if [[ -s $G_ALL_YARN_LOGS ]]
	then
		printf "\n\nThis file contains all the successful YARN Logs for this run:  view $G_ALL_YARN_LOGS\n\n\n"
	fi;

	printf "\t===================================================================\n";
	printf "\t\tFINAL JOB STATUS =  $job_Status\t$G_JOB_STATUS_COMPLEMENT_STR\n" 
	printf "\t\tBATCH_ID         =  $G_BATCH_ID\n"
	printf "\t===================================================================\n";
	printf "<== function Func_Trap_INT;   Exit Code = $RETURN_CODE\n\n"
	exit  $RETURN_CODE
}


function PrintFinalSQLforRoadMasterQueueAndOutputTables
{
	local l_PROGRAM_TYPE=$1
	local l_BATCH_ID=$2
	local itab1="$3";   if [[ -z "$itab1" ]]; then itab1=""; fi; 
	local itab2="${itab1}\t"; local itab3="${itab2}\t"; local itab4="${itab3}\t";

	printf "${itab1}==> function PrintFinalSQLforRoadMasterQueueAndOutputTables\t\t$(GetCurrTime)\n"

	# Also, some tables have NOT  complete yet.  Must reset the BATCH_ID
	# For BSTAT: it will:  -Restart on its own, and -Use its own Checkpoint HDFS info (SUCCESS/ATTEMPTED) to skip completed tables, and start on the remaining BATCH_ID
	# For other apps, that might not be the case. For those that have not completed We must reset the BATCH_ID 
	# For now we just provide the manual UPDATE.
	#        AVAIL_CD= 0; RUNNING_CD= 1; SUCCESS_CD= 2; FAILED_CD= 3;
	#
	#

	case "$l_PROGRAM_TYPE" in
	"BSTAT")
		printf "
-- 1) BSTAT: Check for tables that are marked RUNNING (Code=1):
--    (NOTE:  This is assuming it has got onto to the RUNNING queue for the MicroService to populate these 2 tables (QUEUE/OUTPUT))
SELECT	PRTY_NBR, table_id, batch_id, task_stat_cd, fail_cnt, crt_by, crt_ts, upd_By, upd_ts, srvr_nm
FROM	CHEVELLE.PROF_TABLE_STAT_QUE
WHERE	batch_id = $l_BATCH_ID
AND	task_stat_cd = 1 ;


-- 2) Reset BATCH_ID for these above tables (so they can be re-select in next run):
UPDATE	CHEVELLE.PROF_TABLE_STAT_QUE
SET	TASK_STAT_CD = 0,
	BATCH_ID     = NULL
WHERE	
	batch_id     = $l_BATCH_ID
AND	task_stat_cd = 1 ;"
		;;

	"DC")
		printf " 
-- 1) DC: Check for tables that are marked RUNNING/ASSIGNED (Code=1):
--    (NOTE:  This is assuming it has got onto to the RUNNING queue for the MicroService to populate these 2 tables (QUEUE/OUTPUT))
SELECT	PRTY_NBR, table_id, batch_id, task_stat_cd, fail_cnt, crt_by, crt_ts, upd_By, upd_ts, srvr_nm
FROM	CHEVELLE.PROF_COL_CLS_QUE
WHERE	batch_id = $l_BATCH_ID
AND	task_stat_cd = 1 ;


-- 2) Reset BATCH_ID for these above tables:
UPDATE  CHEVELLE.PROF_COL_CLS_QUE
SET	TASK_STAT_CD = 0,
	BATCH_ID     = NULL
WHERE	
	batch_id     = $l_BATCH_ID
AND	task_stat_cd = 1 ;"
		;;

	esac;

	printf "${itab1}<== function PrintFinalSQLforRoadMasterQueueAndOutputTables\n"
}


function SendMail
{
	local l_MailList="$1"
	local l_MailFile="$2";
	local l_JobStatus="$3";
	if [[ -z $l_MailList ]]
	then	printf "\n\tfunction SendMail:  MailList is empty:  MailList=\"$l_MailList\"\n";
		return;
	fi;

	if [[ ! -f $l_MailFile ]]
	then	printf "\n\tfunction SendMail:  MailFile is missing:  MailFile=\"$l_MailFile\"\n";
		return;
	fi;

	for em in $(echo $l_MailList|sed 's:,: :g')
	do
		printf "\t\tSend $l_MailFile to user: $em\n"
		cat $l_MailFile | mailx -s "CHEVELLE BASIC STAT ($ENV): $l_JobStatus"  $em
	done
}


function ExcludeSecretASMS
{
	#PURPOSE: Remove all ASMS (from l_ASMS_EXCLUDE_FILE) from Original Input file (before Splitting File)

	local l_ASMS_EXCLUDE_FILE=$1;	#Absolute_Path
	local l_INPUT_FILE=$2;		#Absolute_Path
	local l_OUTPUT_FILE=$3;		#Absolute_Path
	local l_LOG_DIR=$4
	local itab1="$5";   if [[ -z "$itab1" ]]; then itab1=""; fi; 
	local itab2="${itab1}\t"; local itab3="${itab2}\t"; local itab4="${itab3}\t";
	local N   bn  l_TEMP_FILE_1   l_TEMP_FILE_2   l_inputLines   l_asmsLines

	bn="$(basename $l_INPUT_FILE)"
	l_TEMP_FILE_1="${l_LOG_DIR}/${bn}_ASMS_temp1.txt"
	l_TEMP_FILE_2="${l_LOG_DIR}/${bn}_ASMS_temp2.txt"

	local l_inputLines=$(wc -l $l_INPUT_FILE        | cut -d" " -f1)
	local l_asmsLines=$(grep -v "^#" $l_ASMS_EXCLUDE_FILE |grep -v "^[ 	]*$"| wc -l | cut -d" " -f1)

	printf "
${itab1}==> function ExcludeSecretASMS\t\t$(GetCurrTime))
${itab2}l_ASMS_EXCLUDE_FILE	= $l_ASMS_EXCLUDE_FILE
${itab2}l_INPUT_FILE		= $l_INPUT_FILE
${itab2}l_LOG_DIR		= $l_LOG_DIR
${itab2}l_TEMP_FILE_1		= $l_TEMP_FILE_1
${itab2}l_TEMP_FILE_2		= $l_TEMP_FILE_2

${itab2}Input File:        $l_inputLines\tlines
${itab2}ASMS_Exclude_File: $l_asmsLines\tlines
\n"

	cp $l_INPUT_FILE  $l_TEMP_FILE_1;	#Copy to temp file so not to mess up original file
	cp $l_INPUT_FILE  $l_TEMP_FILE_2;	#Also copy to TEMP_FILE_2 so in case the FOR loop exit immediately 
	let N=0;
	let totalRemoved=0;
	for asms in $(grep -v "^#" $l_ASMS_EXCLUDE_FILE |grep -v "^[ 	]*$")
	do	#For loop automatically trims leading/trailing blanks surrounding ASMS

		N=$((N+1))
		totalLinesToRemove=$(grep "_${asms}_" $l_TEMP_FILE_1|wc -l |cut -d" " -f1) 
			#                  ^       ^
			#                  |       | Must have "_" surrounding asms
		if [[ $totalLinesToRemove -gt 0 ]]
		then
			grep -v "_${asms}_" $l_TEMP_FILE_1  > $l_TEMP_FILE_2
			remainingLines=$(wc -l $l_TEMP_FILE_2 | cut -d" " -f1)
			printf "${itab2}Line %2d) ASMS=\"$asms\";\t\tTables to exclude: $totalLinesToRemove;\t\tLines After Removal: $remainingLines\n"  "$N"
			cp $l_TEMP_FILE_2 $l_TEMP_FILE_1
			totalRemoved=$((totalRemoved + totalLinesToRemove))
			printf "${itab2}Line %2d) Found Secret ASMS=\"$asms\"\t\tTables to exclude for this ASMS: $totalLinesToRemove; totalRemoved so far=$totalRemoved\n" "$N"
		else
			:
			#printf "${itab2}Line %2d) ASMS=\"$asms\"\t\tTables to exclude: $totalLinesToRemove\n" "$N"
		fi;
	done;
	cp $l_TEMP_FILE_2 $l_OUTPUT_FILE
	remainingLines=$(wc -l $l_OUTPUT_FILE | cut -d" " -f1)
	printf "\n${itab2}OUTPUT FILE=$l_OUTPUT_FILE;  Input Lines=$l_inputLines\ttotal Lines Removed=$totalRemoved;\tRemaining Lines=$remainingLines\n\n";
	printf "${itab1}<== function ExcludeSecretASMS\t\t$(GetCurrTime))"
}


function ResetToStartFresh
{
	local l_TORUN_Dir=$1;
	local l_PRIOR_TORUN_Dir=$2;
	local l_PRIOR_APP_Dir=$3;
	local l_FLAGS_Dir=$4;
	local l_HDFSCheckpoint_Dir=$5;

	local itab1="$5";   if [[ -z "$itab1" ]]; then itab1=""; fi; 
	local itab2="${itab1}\t"; local itab3="${itab2}\t"; local itab4="${itab3}\t";

	printf "${itab1}==> function ResetToStartFresh\t\t$(GetCurrTime)\n\n"
	printf "${itab2}*******************************************************************************\n";
	printf "${itab2}********** RESET option specified at command line (G_RESET=$G_RESET) **********\n";
	printf "${itab2}*******************************************************************************\n";

	printf "\n${itab2}WARNING: This Reset (-R) option removes all Checkpoints, causing PySpark to recalculate statistics for every table in this Input file.\n";
	printf "${itab2}It's OK to use this option, but it should RARELY be used!\n\n\n";

	printf "${itab2}Step 1/5) Remove All OS files from TORUN_DIR downward: $l_TORUN_Dir:\n";
	rm -r -f ${l_TORUN_Dir}/*       2>/dev/null 

	printf "${itab2}Step 2/5) Remove All OS files from PRIOR_TORUN_DIR downward: $l_PRIOR_TORUN_Dir:\n";
	rm -r -f ${l_PRIOR_TORUN_Dir}/* 2>/dev/null 

	printf "${itab2}Step 3/5) Remove All OS files from PRIOR_APP_DIR downward: $l_PRIOR_APP_Dir:\n";
	rm -r -f ${l_PRIOR_APP_Dir}/* 2>/dev/null 

	printf "${itab2}Step 4/5) Remove All OS files from FLAGS_DIR downward: $l_FLAGS_Dir:\n";
	rm -r -f ${l_FLAGS_Dir}/*       2>/dev/null 

	printf "${itab2}Step 5/5) Remove HDFS dir/files downward: $l_HDFSCheckpoint_Dir\n";
	hadoop fs -rm -f -R -skipTrash ${l_HDFSCheckpoint_Dir} 2>/dev/null
		# Since it returns non-Zero if there is no file to remove
		# We cannot check for RC.  Just ignore it

	printf "${itab1}<== function ResetToStartFresh\t\t$(GetCurrTime)\n"
}

function GetInputFileFromMicroService
{
	local l_PROGRAM_TYPE="$1"
	local l_outputFileName="$2"
	local itab1="$3";   if [[ -z "$itab1" ]]; then itab1=""; fi; local itab2="${itab1}\t"; 
	local itab3="${itab2}\t";    local itab4="${itab3}\t";    local itab5="${itab4}\t";

	local H1="Accept: text/plain";
	local H2="Cache-Control: no-cache";
	local H3="Authorization: $G_MS_AUTHORIZATION_KEY";

	local jobName="Job${G_JOB_NUM}";
	local HN="$(hostname)";
	local jobNameHostName="Job${G_JOB_NUM}__${HN}";

	#local URL="${G_MS_GET_WORK_UNIT_URL}/${jobName}/$G_TABLES_TO_GET_FROM_QUEUE/text";

	local l_totalLines  numTries;

	case "$l_PROGRAM_TYPE" in
	"BSTAT") END_POINT="${G_MS_GET_WORK_UNIT_URL}/${jobNameHostName}/$G_TABLES_TO_GET_FROM_QUEUE/text";;
	"DC")    END_POINT="${G_MS_GET_WORK_UNIT_URL}/${HN}/${jobName}/$G_TABLES_TO_GET_FROM_QUEUE/text";;
	esac;

	printf "
${itab1}==> function GetInputFileFromMicroService\t\t$(GetCurrTime)
${itab2}l_PROGRAM_TYPE              = $l_PROGRAM_TYPE
${itab2}l_outputFileName            = $l_outputFileName
${itab2}G_TABLES_TO_GET_FROM_QUEUE  = $G_TABLES_TO_GET_FROM_QUEUE	 (Default=200, or from option -N);

${itab2}JobName   	= $jobName
${itab2}jobNameHostName	= $jobNameHostName
${itab2}HN		= $HN
${itab2}END_POINT	= $END_POINT\n\n"


	let numTries=1;
	printf "${itab2}TRY #$numTries/3:\n"

	#curl -X GET -H "$H1" -H "$H2" -H "$H3" "${G_MS_GET_WORK_UNIT_URL}/${jobName}/$G_TABLES_TO_GET_FROM_QUEUE/text" >$l_outputFileName
	curl -X GET -H "$H1" -H "$H2" -H "$H3" "${END_POINT}" >$l_outputFileName

	RC=$?
	l_totalLines=$(wc -l "$l_outputFileName" | cut -d" " -f1);
	printf "\n"
	printf "${itab2}Curl RC    = $RC\n"
	printf "${itab2}OutputFile = $l_outputFileName\n"
	printf "${itab2}TotalLines = $l_totalLines\n"
	ls -ltr $l_outputFileName
	printf "\n"

	# For some reason sometimes MicroService returns 0 rows (even thought the Queue has available rows for selection)
	# The next call it returns rows.  So to solve that i just make 2 more calls, if needed
	if [[ $l_totalLines -lt 1 ]]
	then
		sleep 1;
		printf "\n"
		let numTries=2;
		printf "${itab3}RETRY #$numTries/3:\n"

		#curl -X GET -H "$H1" -H "$H2" -H "$H3" "${G_MS_GET_WORK_UNIT_URL}/${jobName}/$G_TABLES_TO_GET_FROM_QUEUE/text" >$l_outputFileName
		curl -X GET -H "$H1" -H "$H2" -H "$H3" "${END_POINT}" >$l_outputFileName

		RC=$?
		l_totalLines=$(wc -l "$l_outputFileName" | cut -d" " -f1);
		printf "\n"
		printf "${itab3}Curl RC    = $RC\n"
		printf "${itab3}OutputFile = $l_outputFileName\n"
		printf "${itab3}TotalLines = $l_totalLines\n"
		ls -ltr $l_outputFileName
		printf "\n"
	fi;

	if [[ $l_totalLines -lt 1 ]]
	then
		sleep 1;
		printf "\n"
		let numTries=3;
		printf "${itab4}RETRY #$numTries/3:\n"
		#curl -X GET -H "$H1" -H "$H2" -H "$H3" "${G_MS_GET_WORK_UNIT_URL}/${jobName}/$G_TABLES_TO_GET_FROM_QUEUE/text" >$l_outputFileName
		curl -X GET -H "$H1" -H "$H2" -H "$H3" "${END_POINT}" >$l_outputFileName
		RC=$?
		l_totalLines=$(wc -l "$l_outputFileName" | cut -d" " -f1);
		printf "\n"
		printf "${itab4}Curl RC    = $RC\n"
		printf "${itab4}OutputFile = $l_outputFileName\n"
		printf "${itab4}TotalLines = $l_totalLines\n"
		ls -ltr $l_outputFileName
		printf "\n"
	fi;
	printf "${itab1}<== function GetInputFileFromMicroService\n\n"
}


function GetFileFromHDFS
{
	local l_hdfsINPUTfilename="$1"
	local l_outputFile="$2"
	local itab1="$3";   if [[ -z "$itab1" ]]; then itab1=""; fi; local itab2="${itab1}\t"; local itab3="${itab2}\t";
	local RC
	printf "
${itab1}==> function GetFileFromHDFS\t\t$(GetCurrTime)
${itab2}l_hdfsINPUTfilename	= $l_hdfsINPUTfilename
${itab2}l_outputFile		= $l_outputFile\n"
	rm $l_outputFile >/dev/null 2>&1;	#Remove output file because if it already exists, Hadoop returns 1

	# Get input file from HDFS (containing list of schema.tables) to calculate num_executors for each table
	hadoop fs -get $l_hdfsINPUTfilename   $l_outputFile    2>&1 
	RC=$?;  
		# If INPUT HDFS file exist           ==> it returns RC=0, and create output file (could be 0 line)
		# If INPUT HDFS file does NOT exist  ==> it returns RC=1, and NOT create output file (not even 0 line)
		# If OUTPUT Linux file already exist ==> it returns RC=1, and NOT create output file 
	printf "${itab1}<== function GetFileFromHDFS\n"
	return $RC;
}


function GetInputFile
{
	local l_inputFileName="$1";	#Name of input file specified on Command Line3
	local l_hdfsDir="$2"
	local l_PROGRAM_TYPE="$3"
	local l_LOG_DIR="$4"
		#BEFORE: BSTAT has only 1 output : SchemaName<TAB>TableName 
		#NOW:    Make it same as DC: Both DC and BSTAT have 2 outputs:   
	local l_outputFileName1="$5";	#Name of output file retrieved from HDFS.  BSTAT uses 1st file
	local l_outputFileName2="$6";	#Name of output file retrieved from HDFS.  DC    uses 1st file and 2nd file

	local itab1="$7";   if [[ -z "$itab1" ]]; then itab1=""; fi; local itab2="${itab1}\t"; local itab3="${itab2}\t";
	local RC    l_totalLines1 l_totalLines2 
	local l_TEMP_FILE_1="${l_LOG_DIR}/GetInputFile__Temp1.txt";
	local l_TEMP_FILE_2="${l_LOG_DIR}/GetInputFile__Temp2.txt";


	printf "
${itab1}==> function GetInputFile\t\t$(GetCurrTime)
${itab2}l_inputFileName		= $l_inputFileName
${itab2}l_hdfsDir		= $l_hdfsDir
${itab2}l_PROGRAM_TYPE		= $l_PROGRAM_TYPE
${itab2}l_outputFileName1	= $l_outputFileName1
${itab2}l_outputFileName2	= $l_outputFileName2

${itab2}l_TEMP_FILE_1		= $l_TEMP_FILE_1
${itab2}l_TEMP_FILE_2		= $l_TEMP_FILE_2
\n"
	rm $l_TEMP_FILE_1 $l_TEMP_FILE_2 2>/dev/null
	l_totalLines1=0;

	#-----------------------------------------------------------------------------------------------------------------
	# Get input file from HDFS to calculate num_executors for each table
	# hadoop fs -get ${l_hdfsDir}/${l_inputFileName}    $l_TEMP_FILE_1 2>&1 
	# RC=$?;  # If there is no file to get, hadoop will NOT create the output file (not even 0-line output file)
	# 	# But if there is hadoop file, it could be 0 or more lines
	#
	#GetFileFromHDFS "${l_hdfsDir}/${l_inputFileName}"    $l_TEMP_FILE_1  "$itab2"
	#RC=$?
	#-----------------------------------------------------------------------------------------------------------------

	GetInputFileFromMicroService "$l_PROGRAM_TYPE"    "$l_TEMP_FILE_1"    "$itab3"
	l_totalLines1=$(wc -l "$l_TEMP_FILE_1" | cut -d" " -f1);
	printf "${itab2}l_totalLines1=$l_totalLines1\n"
	sed -i 's:\\x1c::g' $l_TEMP_FILE_1


	# Here, InputFile not found.  This is OK.  Just exit with Status=Successful.
	#if [[ $RC -ne 0 ]] 
	if [[ $l_totalLines1 -eq 0 ]] 
	then	
		G_JOB_STEPS_COMPLETED_SO_FAR="$G_LAST_STEP_TO_COMPLETE"
		G_JOB_STATUS_COMPLEMENT_STR="(NO INPUT FILE FOUND)"
		G_JOB_STATUS="SUCCESS"
		printf "\n\n\t\tWARNING:  Input File is EMPTY ($l_inputFileName) (totalLines=$l_totalLines1);\n\n\n";
		printf "${itab1}<== function GetInputFile;  <<<TOTAL LINES=$l_totalLines1>>>\n"
		exit 0;	# Just exit sucesss if not input file found
			# Its ok just to exit here because it already started reexecuting files from previous failed fun
	fi;
	
#------------------------------------------------------------------------------------------------------------------------------------------------------
# BSTAT Output File Format:
#	hive_schema_1\^table_name_1^\hdfs_location\^table_id_1\^batch_id_123\^srvr_abc\^fail_cnt
#
# DC Output File Format:	
#	hive_schema_1\^table_name_1^\hdfs_location\^col_name_1\^data_cls_nm_1\^regex_str_1\^table_id_1\^ col_id_1\^batch_id_123\^srvr_abc\^fail_cnt_1
#------------------------------------------------------------------------------------------------------------------------------------------------------

	if [[ -s $l_TEMP_FILE_1 ]]
	then
		printf "\n${itab2}===== Performing  dos2unix on output file:\n";  
		dos2unix $l_TEMP_FILE_1 2>&1
		RC=$?
		if [[ $RC -ne 0 ]]
		then
			printf "\n\n\n***** ERROR:  Cannot perform dos2unix on file:  $l_outputFileName1;  RC=$RC\n\n\n";
			printf "${itab1}<== function GetInputFile\n"
			exit -1;
		fi;

		printf "${itab2}===== UpperCase SchemaName/TableName of Output file:  ($l_outputFileName2)\n"
		grep  -v "^\s*$" $l_TEMP_FILE_1 |\
		awk   -F"$G_INPUT_FIELD_DELIMITER" '
			{ printf ("%s%s%s", toupper($1), FS, toupper($2));   for (i=3;i<=NF;++i) {printf ("%s%s", FS, $i)}; printf ("\n")
			}' >$l_outputFileName2

			#  hdfs://edwbidevwar/DEV/DL/EDGE_BASE/BIS-13426/OBISDGXP-BIS/BIS_ORDER_LINE/DATA 
			#	This 1st output is used to calculate SplitFile

		cp $l_outputFileName2 $l_TEMP_FILE_2;
		G_BATCH_ID="";
                G_BATCH_ID_COUNT=0
		printf "\n\n";
		printf "${itab2}===== 1) OutputFile1: $l_outputFileName1 (3 cols: Schema,Table,HDFSDir--for Bucketing)\n"
		#This 1st file is same for all App Type as it is only needed for bucketing, which is same for all prog types)
		awk -F"$G_INPUT_FIELD_DELIMITER" '{
			f3=$3; gsub(/\/$/, "", f3);
			printf ("%s%s%s%s%s\n", $1,FS,$2,FS,f3) 
		}' $l_TEMP_FILE_2 | sed 's:[ 	]::g' | sort -u  > $l_outputFileName1;
				#For this 1st file (only 3 col) there should not be any space/tab.  So use sed to remove them.

		case "$l_PROGRAM_TYPE" in
		"BSTAT")
			printf "${itab2}===== 2) OutputFile2: $l_outputFileName2 (3 cols: Schema,Table,TableID,--For Python Spark program)\n"
			awk -F"$G_INPUT_FIELD_DELIMITER" '{printf ("%s%s%s%s%s\n", $1,FS,$2,FS,$4) }' $l_TEMP_FILE_2 | sed 's:[ 	]::g' | sort -u  > $l_outputFileName2;

			export G_BATCH_ID=$(awk -F"$G_INPUT_FIELD_DELIMITER" '{f5=$5; gsub(/ /, "", f5); gsub(/	/,"", f5); printf ("%s\n", f5) }' $l_TEMP_FILE_2 | sort -u)
			export G_BATCH_ID_COUNT=$(printf "$G_BATCH_ID" | awk 'END {print NR}')
			printf "${itab2}===== 3) Extract BATCH_ID=\"$G_BATCH_ID\";   G_BATCH_ID_COUNT=\"$G_BATCH_ID_COUNT\"\n"
			# The entire file must have only 1 BATCH_ID.  Error otherwise
			if [[ $G_BATCH_ID_COUNT -gt 1 ]]
			then	printf "\n\n\nERROR: G_BATCH_ID_COUNT (=$G_BATCH_ID_COUNT)  cannot have more than 1 BATCH_ID per InputFile\n\n"	
				exit -1;
			fi;;
		"DC")	
			printf "${itab2}===== 2) OutputFile2: $l_outputFileName2 (--For Python Spark program)\n"


printf "\n============================================================================================================================================\n"
printf "Top 5 lines of File l_TEMP_FILE_2 = $l_TEMP_FILE_2\n"
head -5 $l_TEMP_FILE_2 | sed 's:^:	:' | cat -v
printf "============================================================================================================================================\n"

			awk -F"$G_INPUT_FIELD_DELIMITER" '{
					### gsub third parameter is not a changeable object
					### f1=gsub(/ /,"", f1); 	#Assignment returns how many occurances were replaced.
				f1=$1;	#SchemaName
				f2=$2;	#TableName
						# Col3  = HDFS location
				f4=$4;	#ColName
				f5=$5;	#Data ClassName
				f6=$6;	#RegExp
				f7=$7;	#Table_ID
				f8=$8;	#Col_ID
				f9=$9;	#Batch_ID
						# Col 10 = ServerName

				gsub(/ /, "", f1); 	gsub(/	/,"", f1);	#1st gsub: Remove all spaces; 2nd gsub: tabs
				gsub(/ /, "", f2); 	gsub(/	/,"", f2);
				gsub(/ /, "", f4); 	gsub(/	/,"", f4);

				## gsub(/ /, "", f5); 	gsub(/	/,"", f5);	# ClassName might have spaces/tabs in it
										# This is part of the PK columns, so do not modify it

				gsub(/ /, "", f7); 	gsub(/	/,"", f7);
				gsub(/ /, "", f8); 	gsub(/	/,"", f8);
				gsub(/ /, "", f9); 	gsub(/	/,"", f9);
					#Do not remove spaces/tabs on the Regex column 6
				printf ("%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s\n", f1,FS,f2,FS,f4,FS,$5,FS,$6,FS,f7,FS,f8,FS,f9) 
				}' $l_TEMP_FILE_2  > $l_outputFileName2

			export G_BATCH_ID=$(awk -F"$G_INPUT_FIELD_DELIMITER" '{f9=$9; gsub(/ /, "", f9);      gsub(/	/,"", f9); printf ("%s\n", f9) }' $l_TEMP_FILE_2 | sort -u)
			export G_BATCH_ID_COUNT=$(printf "$G_BATCH_ID" | awk 'END {print NR}')
			printf "${itab2}===== 3) Extract BATCH_ID=\"$G_BATCH_ID\";   G_BATCH_ID_COUNT=\"$G_BATCH_ID_COUNT\"\n"
			# The entire file must have only 1 BATCH_ID.  Error otherwise
			if [[ $G_BATCH_ID_COUNT -gt 1 ]]
			then	printf "\n\n\nERROR: G_BATCH_ID_COUNT (=$G_BATCH_ID_COUNT)  cannot have more than 1 BATCH_ID per InputFile\n\n"	
				exit -1;
			fi;;
		esac;

		printf "\nWrite BATCH_ID to file  $G_BATCHID_FILE:\n"
		printf "$G_BATCH_ID\n" > $G_BATCHID_FILE;	#There should only be 1 file in this BATCHID_DIR.  
								#Every new run that require (getWorkUnit from Queue) will overwrite this value
		RC=$?
		if [[ $RC -ne 0 ]]
		then
			printf "\n\n\nERROR: Cannot write to file G_BATCHID_FILE: $G_BATCHID_FILE\n\n\n"
			exit -1
		fi;

		l_totalLines1=$(wc -l $l_outputFileName1 | cut -d" " -f1);
		l_totalLines2=$(wc -l $l_outputFileName2 | cut -d" " -f1);

		printf "\n${itab2}===== Final LinesCount of Output file 1: $l_totalLines1\n";
		printf "\n${itab3}===== Sample OutputFile1 (top 5 lines):  $l_outputFileName1\n";
		head -5 $l_outputFileName1|cat -v; printf "\n";

		printf "\n${itab2}===== Final LinesCount of Output file 2: $l_totalLines2\n";
		printf "\n${itab3}===== Sample OutputFile2 (top 5 lines):  $l_outputFileName2\n";  
		head -5 $l_outputFileName2|cat -v; printf "\n";

		chmod 755 $l_outputFileName1 $l_outputFileName2;
	else
		printf "\n\n\n***** ERROR: HDFS output file is empty: $l_outputFileName1\n\n\n"
		printf "${itab1}<== function GetInputFile\n"
		exit -1;
	fi;
	printf "${itab1}<== function GetInputFile;\n"
}


function Make_1_HDFS_Dir
{
	local hdfsDir="$1";
	local message="$2";
	local itab1="$3";   if [[ -z "$itab1" ]]; then itab1=""; fi; local itab2="${itab1}\t"; local itab3="${itab2}\t";
	local log="$4";  	# name could be empty
	local RC;
	if [[ -s $log ]]
	then
		printf "
${itab1}==> function Make_1_HDFS_Dir\t\t$(GetCurrTime))
${itab2}message	= \"$message\"
${itab2}hdfsDir	= \"$hdfsDir\"\n" >> $log;
	else
		printf "
${itab1}==> function Make_1_HDFS_Dir\t\t$(GetCurrTime))
${itab2}message	= \"$message\"
${itab2}hdfsDir	= \"$hdfsDir\"\n";
	fi;

	hadoop fs -test -e $hdfsDir
	RC=$?
	if [[ $RC -eq 0 ]]
	then
		if [[ -s $log ]];
		then printf "${itab2}HDFS Dir already exists.  OK.  Skip to next step!\n" >> $log;
		else printf "${itab2}HDFS Dir already exists.  OK.  Skip to next step!\n";
		fi;
	else
		if [[ -s $log ]]; then	printf "${itab2}==> HDFS Dir Create:\t" >> $log; else	printf "${itab2}==> HDFS Dir Create:\t"; fi;
		hadoop fs -mkdir -p $hdfsDir  
		RC=$?
		if [[ $RC -ne 0 ]]
		then
			if [[ -s $log ]]
			then	printf "\n\n\n***** ERROR:  Cannot make HDFS Dir;  RC=$RC\n\n\n" >> $log;
				printf "${itab1}<== function Make_1_HDFS_Dir\n"                  >> $log;
			else	printf "\n\n\n***** ERROR:  Cannot make HDFS Dir;  RC=$RC\n\n\n";
				printf "${itab1}<== function Make_1_HDFS_Dir\n";
			fi;
			exit -1;
		else
			if [[ -s $log ]]; then	printf "SUCCESS!   (RC=$RC)\n" >> $log; else	printf "SUCCESS!   (RC=$RC)\n"; fi;
		fi

		if [[ -s $log ]]; then printf "${itab2}==> Chmod 755:\t" >> $log; else printf "${itab2}==> Chmod 755:\t"; fi;
		hadoop fs -chmod -R 755 $hdfsDir
		RC=$?
		if [[ $RC -ne 0 ]]
		then
			if [[ -s $log ]]
			then	printf "\n\n\n***** ERROR:  Cannot chmod 775 for HDFS Dir;  RC=$RC\n\n\n" >> $log;
				printf "${itab1}<== function Make_1_HDFS_Dir\n"                           >> $log;
			else	printf "\n\n\n***** ERROR:  Cannot chmod 775 for HDFS Dir;  RC=$RC\n\n\n";
				printf "${itab1}<== function Make_1_HDFS_Dir\n"
			fi;
			exit -1;
		else
			if [[ -s $log ]]; then	printf "SUCCESS!   (RC=$RC)\n" >> $log; else	printf "SUCCESS!   (RC=$RC)\n"; fi;
		fi
	fi;
	if [[ -s $log ]]
	then	printf "${itab1}<== function Make_1_HDFS_Dir\n" >> $log;
	else	printf "${itab1}<== function Make_1_HDFS_Dir\n"
	fi;
}


function RemoveOldFiles
{
	local daysOld="$1";
	local rootDir="$2";
	local logDir="$3";
	local itab1="$4";   if [[ -z "$itab1" ]]; then itab1=""; fi; local itab2="${itab1}\t"; local itab3="${itab2}\t";

	dateStr=$(date  -d "-$daysOld days" +%Y-%m-%d)

	printf "${itab1}==> function RemoveOldFiles;  daysOld=$daysOld\t\t$(GetCurrTime)\n"
	printf "${itab2}A) Remove files Older than: \"$dateStr\" days from ROOT_DIR:  $rootDir;\t\t$(GetCurrTime)\n"

	printf "${itab3}1) List of files older than $daysOld to be removed (under OS Dir: $rootDir):\t\t$(GetCurrTime)\n"
	find $rootDir  -type f    ! -newermt "$dateStr"    -exec ls -ltr {} \; 2>/dev/null
	printf "\n";

	printf "${itab3}2) Removing these OLD files (>= $daysOld days):\t\t$(GetCurrTime)\n"
	find $rootDir  -type f ! -newermt "$dateStr" -exec rm -f {} \; 
	printf "\n";

	printf "${itab2}B) Removing OLD LOG directories (>= $daysOld days) under $logDir:\t\t$(GetCurrTime)\n"
	# find $logDir  -type d ! -newermt "$dateStr" -exec rmdir {} \; 
	find $logDir  -type d ! -newermt "$dateStr" -exec rm -f -R {} \; 
	printf "\n";

	printf "${itab1}<== function RemoveOldFiles\t\t$(GetCurrTime)\n\n"
}


function GetENVvariable
{
	local itab1="$1";   if [[ -z "$itab1" ]]; then itab1=""; fi; local itab2="${itab1}\t"; local itab3="${itab2}\t";
	printf "${itab1}==> function GetENVvariable\t\t$(GetCurrTime)\n"

	hdfs_cluster_nm=`hdfs getconf -confKey dfs.nameservices 2>/dev/null`
	local RC=$?
	if [[ $RC -ne 0 ]]
	then
		printf "\n\n\n***** ERROR: Cannot execute \"hdfs  getconf\" ; RC=$RC\n\n\n"
		printf "${itab1}<== function GetENVvariable\n"
		exit -1;
	fi;

	cluster_env=${hdfs_cluster_nm:5:3}

	if   [[ ${cluster_env} = 'dev' ]]
	then	export env='dev'; export ENV='DEV';
	elif [[ ${cluster_env} = 'tst' ]]
	then	export env='tst'; export ENV='TST';
	elif [[ ${cluster_env} = 'crt' ]]
	then	export env='crt'; export ENV='CRT';
	elif [[ ${cluster_env} = 'prd' ]]
	then	export env='prd'; export ENV='PRD';
	else	
		printf "\n\n\n***** ERROR: I don't know what env I am on... Exiting\n\n\n"
		printf "${itab1}<== function GetENVvariable\n"
	        exit 1
	fi;
	printf "${itab2}hdfs_cluster_nm = \"${hdfs_cluster_nm}\"\n" 
	printf "${itab2}cluster_env     = \"${cluster_env}\"\n" 
	printf "${itab2}ENV             = \"${ENV}\"\n"
	printf "${itab2}env             = \"${env}\"\n"
	printf "${itab1}<== function GetENVvariable\n\n"
} #function GetENVvariable


function ValidateInputParameters
{
	local inputFileName="$1";	#FileName (HDFS) containing list of tables to profile
	local itab1="$2";   if [[ -z "$itab1" ]]; then itab1=""; fi; local itab2="${itab1}\t"; local itab3="${itab2}\t";
	local firstCHAR;

	printf "${itab1}==> function ValidateInputParameters\t\t$(GetCurrTime)\n"

	printf "${itab2}1) Check CHUNK_SIZE\n"
	if [[ $G_CHUNK_SIZE -lt $G_CHUNK_SIZE_MINIMUM ]]	#100,000,000
	then	printf "\n\n\nWARNING:  Input Chunksize TOO SMALL ($G_CHUNK_SIZE bytes).";
		G_CHUNK_SIZE=$G_CHUNK_SIZE_DEFAULT
		printf "  Reset to default: $G_CHUNK_SIZE bytes\n\n\n"
	fi;
	if [[ $G_CHUNK_SIZE -gt $G_CHUNK_SIZE_MAXIMUM ]]
	then	printf "\n\n\nWARNING:  Input Chunksize TOO BIG ($G_CHUNK_SIZE bytes).";
		G_CHUNK_SIZE=$G_CHUNK_SIZE_DEFAULT
		printf "  Reset to default: $G_CHUNK_SIZE bytes\n\n\n"
	fi;


	printf "${itab2}2) Check G_MIN_NUM_EXECUTORS\n"
	if [[ $G_MIN_NUM_EXECUTORS -lt 2 ]]
	then	printf "\n\n\nWARNING:  Input G_MIN_NUM_EXECUTORS TOO SMALL ($G_MIN_NUM_EXECUTORS bytes).";
		G_MIN_NUM_EXECUTORS=$G_MIN_NUM_EXECUTORS
		printf "  Reset to default: $G_MIN_NUM_EXECUTORS executors\n\n\n"
	fi;
	if [[ $G_MIN_NUM_EXECUTORS -gt $G_MAX_NUM_EXECUTORS ]]
	then	printf "\n\n\nWARNING:  Input G_MIN_NUM_EXECUTORS TOO BIG ($G_MIN_NUM_EXECUTORS bytes).";
		G_MIN_NUM_EXECUTORS=$G_MIN_NUM_EXECUTORS
		printf "  Reset to default: $G_MIN_NUM_EXECUTORS executors\n\n\n"
	fi;

	#Make sure user enter completes filename, not just a digit 
	#(so inputfilename could be anything instead of hard-code to same name)
	printf "${itab2}3) Check FILENAME\n"
	firstCHAR=${inputFileName:0:1}
	if [[ "$firstCHAR" =~ [0-9] ]]
	then	printf "\n\n\n***** ERROR: First letter of Input file MUST NOT be a digit (Entered: $inputFileName).\n\n\n"
		printf "${itab1}<== function ValidateInputParameters\t\t$(GetCurrTime)\n\n\n"
		exit -1;
	fi;

	printf "${itab2}4) Check G_DEPLOY_MODE:\t\tG_DEPLOY_MODE=\"$G_DEPLOY_MODE\"\n"
	if [[ $G_DEPLOY_MODE == "client" ]] || [[ $G_DEPLOY_MODE == "cluster" ]]
	then	:
	else	printf "\n\n\n***** ERROR: Invalid DEPLOY_MODE (\"$G_DEPLOY_MODE\").  Must be either:  client or cluster only!\n\n\n"
		printf "${itab1}<== function ValidateInputParameters\t\t$(GetCurrTime)\n\n\n"
		exit -1;
	fi;

	printf "${itab2}5) Check G_SUBMIT_MASTER:\tG_SUBMIT_MASTER=\"$G_SUBMIT_MASTER\"\n"
	if [[ $G_SUBMIT_MASTER == "local" ]] || [[ $G_SUBMIT_MASTER == "yarn" ]]
	then	
		:
	else	printf "\n\n\n***** ERROR: Invalid G_SUBMIT_MASTER (\"$G_SUBMIT_MASTER\").  Must be either:  local[*] or yarn only!\n\n\n"
		printf "${itab1}<== function ValidateInputParameters\t\t$(GetCurrTime)\n\n\n"
		exit -1;
	fi;

	if [[ $G_SUBMIT_MASTER == "local" ]]; then G_DEPLOY_MODE="client"  ; fi;
	if [[ $G_SUBMIT_MASTER == "yarn"  ]]; then G_DEPLOY_MODE="cluster" ; fi;


	printf "${itab2}7) Check MAX_RETRY_THRESHOLD:\n"
	if   [[ $G_MAX_RETRY_THRESHOLD       -lt   1 ]]
	then     let G_MAX_RETRY_THRESHOLD=1
	elif [[ $G_MAX_RETRY_THRESHOLD       -gt 100 ]]
	then     let G_MAX_RETRY_THRESHOLD=100
	fi;

	printf "${itab2}8) Check G_DF_COLUMNS_BATCH_SIZE:\n";	#Maximum: 1000 columns to send to SQL at a time
	if   [[ $G_DF_COLUMNS_BATCH_SIZE       -lt   1 ]]
	then     let G_DF_COLUMNS_BATCH_SIZE=1
	elif [[ $G_DF_COLUMNS_BATCH_SIZE       -gt 250 ]]
	then     let G_DF_COLUMNS_BATCH_SIZE=250
	fi;


	printf "${itab2}9) Check G_MUST_HAVE_HIVE_DATATYPES:\n";
	export  G_MUST_HAVE_HIVE_DATATYPES=$(echo $G_MUST_HAVE_HIVE_DATATYPES | tr a-z A-Z)
        if [[ "$G_MUST_HAVE_HIVE_DATATYPES" == "Y" ]]	|| [[ "$G_MUST_HAVE_HIVE_DATATYPES" == "YES" ]]	
	then	G_MUST_HAVE_HIVE_DATATYPES="YES"
	else	G_MUST_HAVE_HIVE_DATATYPES="NO"
	fi;

	printf "${itab2}8) Check G_TABLES_TO_GET_FROM_QUEUE:\n";	#Maximum: 1000 columns to send to SQL at a time
	if [[ -z "$p_G_TABLES_TO_GET_FROM_QUEUE" ]]
	then	G_TABLES_TO_GET_FROM_QUEUE=200;
	else	G_TABLES_TO_GET_FROM_QUEUE=$p_G_TABLES_TO_GET_FROM_QUEUE;
	fi;

	if   [[ $G_TABLES_TO_GET_FROM_QUEUE        -lt    1 ]]
	then     let G_TABLES_TO_GET_FROM_QUEUE=200
	elif [[ $G_TABLES_TO_GET_FROM_QUEUE        -gt 1000 ]]
	then     let G_TABLES_TO_GET_FROM_QUEUE=1000
	fi;

	printf "\n${itab2}All CommmandLine Parameters passed!\n"
	printf "${itab1}<== function ValidateInputParameters\n\n"
} #function ValidateInputParameters


function CreateInternalFileNames
{
	local logDir="$1";
	local fnStr="$2";
	local itab1="$3";   if [[ -z "$itab1" ]]; then itab1=""; fi; local itab2="${itab1}\t"; local itab3="${itab2}\t";
	printf "${itab1}==> function CreateInternalFileNames\t\t$(GetCurrTime)\n"

	export                     G_DC_INPUT_LIST="${logDir}/DC_INPUT_LIST.txt";
	export                  G_BSTAT_INPUT_LIST="${logDir}/BSTAT_INPUT_LIST.txt";
	export       G_INPUT_LIST_PER_PROGRAM_TYPE="${logDir}/INPUT_LIST_PER_PROGRAM_TYPE.txt";
	export                     G_TABLES_LIST_1="${logDir}/TablesList1.txt";
	export                     G_TABLES_LIST_2="${logDir}/TablesList2.txt";
	export                    G_TABLES_LIST_1B="${logDir}/TablesList1B.txt";
	export                    G_TABLES_LIST_2B="${logDir}/TablesList2B.txt";
	export                      G_TABLE_SIZE_1="${logDir}/Tables_Size1.txt";
	export                      G_TABLE_SIZE_2="${logDir}/Tables_Size2.txt";
	export           G_SHOW_CREATE_TABLE_1_HQL="${logDir}/SHOWCREATETABLE.hql"
	export           G_SHOW_CREATE_TABLE_1_TXT="${logDir}/SHOWCREATETABLE.txt"
	export        G_ALL_FILES_SIZES_BOTH_TRIES="${logDir}/AllFilesSize.txt"
	export G_ALL_FILES_SIZES_SORTED_BOTH_TRIES="${logDir}/AllFilesSizeSORTED.txt"
	export     G_DISTINCT_EXECUTORS_BOTH_TRIES="${logDir}/DistinctListOfExecutors.txt" 
	export                      G_SUMMARY_FILE="${logDir}/DistinctListOfExecutors.txt" 

	printf "${itab1}<== function CreateInternalFileNames\t\t$(GetCurrTime)\n\n"
} #function CreateInternalFileNames


function ClearDirectory
{
        local DirToClear="$1";
        local itab1="$2";   if [[ -z "$itab1" ]]; then itab1=""; fi; local itab2="${itab1}\t"; local itab3="${itab2}\t";
        printf "${itab1}==> function ClearDirectory;   $DirToClear\t\t$(GetCurrTime)\n"

	if [[ -d $DirToClear ]]
	then
		rm -r ${DirToClear}/* 2>/dev/null;	#OK if no files there
	else
		printf "\n\n\nERROR: Directory $DirToClear NOT FOUND\n\n\n";
        	printf "${itab1}<== function ClearDirectory\n\n"
		exit -1;
	fi;
        printf "${itab1}<== function ClearDirectory\n\n"
} #function ClearDirectory


function DisplayDirectory
{
        local DirToDisplay="$1";
        local msg1="$2";
        local msg2="$3";
        local itab1="$4";   if [[ -z "$itab1" ]]; then itab1=""; fi; local itab2="${itab1}\t"; local itab3="${itab2}\t";
        printf "${itab1}==> function DisplayDirectory;  Dir:  $msg1;  $DirToDisplay\t\t$(GetCurrTime)\n"

        printf "${itab2}=======================================================================================================================\n";
        printf "${itab2}Content of Directory $msg1:   $DirToDisplay:\n";
        if [[ ! -z "$msg2" ]]; then printf "${itab2}$msg2\n"; fi;

	local totalFiles=$(find $DirToDisplay -maxdepth 1 -type f -print |  wc -l | cut -d" " -f1);

	if [[ $totalFiles -eq 0 ]]
	then	printf "${itab3}(Empty - totalFiles=$totalFiles)\n"
	else	ls -ltr $DirToDisplay | grep -v "^total" | sort -k9 | nl | sed "s:^:${itab2}:"  #Sort by fileName (k9) which is in order of creation
	fi;

        printf "${itab2}=======================================================================================================================\n";
        printf "${itab1}<== function DisplayDirectory\n\n"
} #function DisplayDirectory


function MakeLinuxDir
{
	local DirToMake="$1";
	local itab1="$2";   if [[ -z "$itab1" ]]; then itab1=""; fi; local itab2="${itab1}\t"; local itab3="${itab2}\t";
	#printf "${itab1}==> function MakeLinuxDir;\t\t$(GetCurrTime)\n"

	#printf "${itab2}Make directory: $DirToMake\n";
	mkdir -p $DirToMake;
	local RC=$?;
	if [[ $RC -ne 0 ]]
	then	printf "\n\n\n***** ERROR: Cannot mkdir $DirToMake;  RC=$RC\n\n\n";
		printf "${itab1}<== function MakeLinuxDir\n";
		exit -1;
	fi;
	#printf "${itab1}<== function MakeLinuxDir\n\n"
} #function MakeLinuxDir


function DisplayGlobalVariables
{
	local itab1="$1";   if [[ -z "$itab1" ]]; then itab1=""; fi; local itab2="${itab1}\t"; local itab3="${itab2}\t";
	printf "${itab1}==> function DisplayGlobalVariables\t\t$(GetCurrTime)

	whoami		= \"$(whoami)\"
	G_THIS_JOB_PID	= \"$G_THIS_JOB_PID\"

Command Line Parameters:
        G_ASMS_EXCLUDE_FILE                       (-a) = \"$G_ASMS_EXCLUDE_FILE\"
        G_BIG_TABLE_STATCOLS_BATCH_SIZE           (-b) = \"$G_BIG_TABLE_STATCOLS_BATCH_SIZE\"
        G_BIG_TABLE_ROWS_TO_SWITCH                (-C) = \"$G_BIG_TABLE_ROWS_TO_SWITCH\"
        G_DEBUG                                   (-d) = \"$G_DEBUG\"
        G_DEPLOY_MODE                             (-n) = \"$G_DEPLOY_MODE\"
        G_DF_COLUMNS_BATCH_SIZE                   (-c) = \"$G_DF_COLUMNS_BATCH_SIZE\"
        G_HDFS_ROOT_DIR                           (-H) = \"$G_HDFS_ROOT_DIR\"
        G_INCLUDE_SKIP_DATA_IN_OUTPUT             (-I) = \"$G_INCLUDE_SKIP_DATA_IN_OUTPUT\"
        G_MAIL_LIST                               (-e) = \"$G_MAIL_LIST\"
        G_MAX_RETRY_THRESHOLD                     (-t) = \"$G_MAX_RETRY_THRESHOLD\"
        G_MUST_HAVE_HIVE_DATATYPES                (-o) = \"$G_MUST_HAVE_HIVE_DATATYPES\"
        G_PROJECT_DIRNAME                         (-P) = \"$G_PROJECT_DIRNAME\"
        G_PROGRAM_TYPE                            (-T) = \"$G_PROGRAM_TYPE\"
        G_RESET                                   (-R) = \"$G_RESET\"
        G_RESET_NORUN                             (-R) = \"$G_RESET_NORUN\"
        G_ROOT_PROGDIR                            (-L) = \"$G_ROOT_PROGDIR\"
        G_SUBMIT_MASTER                           (-M) = \"$G_SUBMIT_MASTER\"
        G_SPLIT_FILE_BATCH_SIZE                   (-S) = \"$G_SPLIT_FILE_BATCH_SIZE\"
        G_SLEEP_SECS_FOR_SPARK_JOB                (-w) = \"$G_SLEEP_SECS_FOR_SPARK_JOB\" secs
        G_SECS_TO_WAIT_FOR_SPARK_JOB_BEFORE_ABORT (-W) = \"$G_SECS_TO_WAIT_FOR_SPARK_JOB_BEFORE_ABORT\" secs
        G_YARN_SPARK_NAME                         (-Y) = \"$G_YARN_SPARK_NAME\"

Derived Variables:
	ENV				= $ENV
	G_THIS_JOB_PID			= $G_THIS_JOB_PID
	G_THIS_SCRIPT_NAME		= $G_THIS_SCRIPT_NAME
	G_SESSION_DATE			= $G_SESSION_DATE
	G_SESSION_DATE_STR		= $G_SESSION_DATE_STR

	G_INPUT_LIST			= $G_INPUT_LIST

        G_CHUNK_SIZE			= $G_CHUNK_SIZE  bytes
        G_MAX_RETRY_THRESHOLD		= $G_MAX_RETRY_THRESHOLD
        G_MIN_NUM_EXECUTORS		= $G_MIN_NUM_EXECUTORS
        G_NUM_EXECUTORS_DEFAULT		= $G_NUM_EXECUTORS_DEFAULT	

	G_ROOT_PROGDIR			= $G_ROOT_PROGDIR
	G_PROJECT_ROOT_DIR		= $G_PROJECT_ROOT_DIR
	G_ROOT_DIR			= $G_ROOT_DIR

	G_ASMS_DIR			= $G_ASMS_DIR
	G_LOG_DIR			= $G_LOG_DIR
	G_CHECKPOINT_DIR		= $G_CHECKPOINT_DIR
	G_CHECKPOINT_ARCHIVE_DIR	= $G_CHECKPOINT_ARCHIVE_DIR
	G_ARCHIVE_LINUX_DIR		= $G_ARCHIVE_LINUX_DIR

	G_PRIOR_APP_DIR			= $G_PRIOR_APP_DIR
	G_PRIOR_TORUN_DIR		= $G_PRIOR_TORUN_DIR
	G_TORUN_DIR			= $G_TORUN_DIR
	G_FLAGS_DIR			= $G_FLAGS_DIR
	G_BATCHID_DIR			= $G_BATCHID_DIR
	G_BATCHID_FILE			= $G_BATCHID_FILE

	G_HDFS_ROOT_DIR			= $G_HDFS_ROOT_DIR
	G_HDFS_INPUT_DIR		= $G_HDFS_INPUT_DIR
	G_HDFS_OUTPUT_DIR		= $G_HDFS_OUTPUT_DIR
	G_HDFS_ARCHIVE_DIR		= $G_HDFS_ARCHIVE_DIR
	G_HDFS_CHECKPOINT_ROOT_DIR	= $G_HDFS_CHECKPOINT_ROOT_DIR
	G_HDFS_HIVE_DATATYPES_DIR	= $G_HDFS_HIVE_DATATYPES_DIR

	G_MAIN_PROG_DIR			= $G_MAIN_PROG_DIR
	G_PYTHON_PROGRAM		= $G_PYTHON_PROGRAM

	G_MS_GET_WORK_UNIT_URL			= $G_MS_GET_WORK_UNIT_URL
	G_MS_WRITE_1TABLE_TO_OUTPUT_URL	= $G_MS_WRITE_1TABLE_TO_OUTPUT_URL
	G_MS_SAVE_BATCH_OUTPUT_URL		= $G_MS_SAVE_BATCH_OUTPUT_URL


	G_MS_AUTHORIZATION_KEY			= \"$G_MS_AUTHORIZATION_KEY\"
	G_MS_GET_TABLEBYID_URL			= \"$G_MS_GET_TABLEBYID_URL\"
	G_MS_GET_WORK_UNIT_URL			= \"$G_MS_GET_WORK_UNIT_URL\"
	G_MS_WRITE_1TABLE_TO_OUTPUT_URL	= \"$G_MS_WRITE_1TABLE_TO_OUTPUT_URL\"
	G_MS_SAVE_BATCH_OUTPUT_URL		= \"$G_MS_SAVE_BATCH_OUTPUT_URL\"
	G_MS_UPDATE_QUEUE_URL			= \"$G_MS_UPDATE_QUEUE_URL\"

	G_FILENAME_PREFIX		= $G_FILENAME_PREFIX
	G_ALL_YARN_LOGS			= $G_ALL_YARN_LOGS\n\n\n"

	printf "${itab1}<== function DisplayGlobalVariables\n\n"
} #function DisplayGlobalVariables


function RoundUpToNext10
{
	# Output is in global variable:  numExecutors
	local str="$1";
	local itab1="$2";   if [[ -z "$itab1" ]]; then itab1=""; fi; local itab2="${itab1}\t"; local itab3="${itab2}\t";

	#Round up to next decimal value (ex: 2=> 10; 12=> 20; 22=> 30;  119=>120 ...)
	#This allows better grouping to submit to Spark Cluster
	str="0${str}";				#Add 1 leading 0 (in case it is single digit)
	local len=${#str}
	local s1=${str:0:len-1};		#Remove last (rightmost) digit (for roundup)
	local s2=$(printf $s1|sed 's:^0*::');	#Remove leading 0s
	local s3="$((s2+1))0";            	#Add 1, then and trailing 0 (because it was removed above)
	#printf "str=$str; len=$len; s1=$s1; s2=$s2; s3=$s3; numExecutors=$numExecutors\n"
	numExecutors=$s3
	#printf "\t\tAFTER  RoundUpToNext10: numExecutors=$numExecutors\n"
} #function RoundUpToNext10


function CalculateNumExecutor
{
	# Output is in global variable:  numExecutors
        local fileSize="$1";
	local chunkSize="$2";
	local itab1="$3";   if [[ -z "$itab1" ]]; then itab1=""; fi; local itab2="${itab1}\t"; local itab3="${itab2}\t";

	#Calculate numExecutors based on fileSize:
	numExecutors=$((($fileSize+$chunkSize-1)/$chunkSize))
	#printf "\t\tBEFORE RoundUpToNext10: numExecutors=$numExecutors\n"
} #function CalculateNumExecutor


function GenerateSHOWCREATETABLECommands 
{
	local inputFile="$1";
	local outputFile_HQL="$2";
	local inputFileB="$3";
	local itab1="$4";   if [[ -z "$itab1" ]]; then itab1=""; fi; local itab2="${itab1}\t"; local itab3="${itab2}\t";
	local RC    l_SHOWCREATETABLE_Command   ISorARE  totalFilesSTR;

	printf "
${itab1}==> function GenerateSHOWCREATETABLECommands\t\t\t$(GetCurrTime)
${itab2}inputFile	= $inputFile
${itab2}inputFileB	= $inputFileB
${itab2}outputFile_HQL	= $outputFile_HQL\n\n";

	rm $outputFile_HQL >/dev/null 2>&1
	totalTables=$(wc -l $inputFile|cut -d" " -f1);
	if [[ $totalTables -le 1 ]]; then ISorARE="is"; totalTablesSTR="table";  else ISorARE="are"; totalTablesSTR="tables"; fi;
	printf "${itab2}There $ISorARE $totalTables $totalTablesSTR in this inputFile ==> Generate 1 script to contain ALL these files:\n";

	let N=0
	for table in $(sed -e 's:\t:.:' $inputFile)	#Combine 2 words into 1 word for FOR loop (a space cause FOR to think as 2 lines)
	do
		N=$((N+1));
		# Check: Valid Input line MUST have  2 words: schema table, separate by TAB
		wordscount=$(echo $table|awk -F"." 'END {print NF}');
		if [[ $wordscount -ne 2 ]]
		then
			# Check: In case input line does not have 2 words (schema table)
			printf "\n\n${itab2}$N/$totalTables)\t***** ERROR: Bad input line: \"$table\" (wordscount=$wordscount).  Must have 2:  \"SchemaName TableName\" ==> IGNORE **********\n\n\n"
			continue;  #Skip remaining of loop
		fi;
	
		echo $table | sed 's:\.:\t:' >> $inputFileB;	#Save Only "good" line (ie has 2 words on this line)
		l_SHOWCREATETABLE_Command=$(echo $table | sed -e 's:\t:.:' -e 's:$:;:' -e 's:^:SHOW CREATE TABLE :' );

		# Example Output line: describe extended cca_edge_base_atds_58616_atds_atds_core.aftersales_contract;
		printf "${itab2}$N/$totalTables)\t$l_SHOWCREATETABLE_Command\n"
		printf "$l_SHOWCREATETABLE_Command\n" >> $outputFile_HQL
	done;

	printf "\n${itab2}===== Sample of inputFile:  $inputFile\n";          head -5 $inputFile      |sed "s:^:$itab3:";  printf "\n";
	printf "\n${itab2}===== Sample of inputFileB: $inputFileB\n";         head -5 $inputFileB     |sed "s:^:$itab3:";  printf "\n";
		# Difference between inputFile and inputFileB:
		#    inputFile:  might have bad numbers of words per line (expected 2 per line)
		#    inputFileB: correct lines. All lines have 2 words
		# Hence, if inputFile does not have any bad lines, then inputFile = inputFileB

	printf "\n${itab2}===== Sample of outputFile_HQL: $outputFile_HQL\n"; head -5 $outputFile_HQL |sed "s:^:$itab3:";  printf "\n";
	printf "${itab1}<== function GenerateSHOWCREATETABLECommands\t\t$(GetCurrTime)\n"
} #function GenerateSHOWCREATETABLECommands 


function ExecuteHIVECommands 
{
	local inputScript="$1";
	local outputFile="$2";
	local FAIL_IF_NONZERO_RC="$3";
	local itab1="$4";   if [[ -z "$itab1" ]]; then itab1=""; fi; local itab2="${itab1}\t"; local itab3="${itab2}\t";
	local RC;

	printf "
${itab1}==> function ExecuteHIVECommands\t\t\t$(GetCurrTime)
${itab2}inputScript     = $inputScript
${itab2}outputFile      = $outputFile\n";

	local l_startTime_EPOC="$(date +%s)"
	printf "\n${itab2}Top 20 lines of inputScript: ($inputScript)\n";
	head -20 $inputScript | nl | sed "s:^:${itab3}:"
	printf "\n";

	printf "\n${itab2}Executing $inputScript\n";
	hive -hiveconf hive.cli.errors.ignore=true  -f $inputScript > $outputFile 2>&1
		# Use this option to it executes entire script, even if there are errors in middle
	RC=$?
	if [[ $RC -eq 0 ]]
	then
		printf "${itab3}RC=$RC;  OutputFile=$outputFile\n\n"
	else
		if [[ "$FAIL_IF_NONZERO_RC" == "FAIL" ]] || [[ "$FAIL_IF_NONZERO_RC" == "YES" ]]
		then
			printf "\n\n\n***** ERROR: RC=$RC.  Exiting (FAIL_IF_NONZERO_RC=\"$FAIL_IF_NONZERO_RC\")\n\n\n"
			printf "${itab1}<== function ExecuteHIVECommands\n\n";
			exit -1;
		else
			printf "\n\n\n\t\t********** WARNING: RC=$RC.  Ignore Error (FAIL_IF_NONZERO_RC=\"$FAIL_IF_NONZERO_RC\") **********\n\n\n"
			printf "${itab2}Read this log file to determine error:  $outputFile\n\n\n";
		fi;
	fi;
	printf "${itab1}<== function ExecuteHIVECommands\t\t$(GetElapsedTime $l_startTime_EPOC)\t\t$(GetCurrTime)\n\n";
} #function ExecuteHIVECommands 


function ExtractFileSizes_FromHDFS_FilesListing
{
	local inputFile="$1";
	local allTablesShowCreateTableTXT="$2";
	local chunkSize="$3";
	local output_tablesWithSIZE="$4";
	local default_NUM_EXCUTORS="$5"
	local userSpecified_NUM_EXCUTORS="$6"
	local l_LOG_DIR="$7"

	local itab1="$8";   if [[ -z "$itab1" ]]; then itab1=""; fi; 
	local itab2="${itab1}\t"; local itab3="${itab2}\t"; local itab4="${itab3}\t"; local itab5="${itab4}\t"; 

	local l_HIVE_Create_Tables_Commands="${l_LOG_DIR}/HIVE_Create_Tables_Commands.txt"
	local l_HIVE_FAILED_Create_Tables="${l_LOG_DIR}/HIVE_FAILED_Create_Tables.txt"
	local l_Schema_TO_HDFS_Dirs="${l_LOG_DIR}/Schema_TO_HDFS_Dirs.txt"
	local l_GetHDFSFileSizes_Script="${l_LOG_DIR}/GetHDFSFileSizes.sh"
	local l_GetHDFSFileSizes_OUTPUT="${l_LOG_DIR}/GetHDFSFileSizes.out"
	local l_GetHDFSFileSizes_ERR="${l_LOG_DIR}/GetHDFSFileSizes.err"

	local l_startTime1_EPOC
	local l_totalInputTables;
	local l_fileHDFSLocation;
	local l_total_HDFSDir_Lines;
	local l_startTime_EPOC="$(date +%s)"

	printf "
${itab1}==> function ExtractFileSizes_FromHDFS_FilesListing;\t\t\t$(GetCurrTime)
${itab2}\$1 inputFile			= $inputFile
${itab2}\$2 allTablesShowCreateTableTXT	= $allTablesShowCreateTableTXT
${itab2}\$3 chunkSize			= $chunkSize
${itab2}\$4 output_tablesWithSIZE	= $output_tablesWithSIZE
${itab2}\$5 default_NUM_EXCUTORS		= $default_NUM_EXCUTORS
${itab2}\$6 userSpecified_NUM_EXCUTORS	= $userSpecified_NUM_EXCUTORS
${itab2}\$7 l_LOG_DIR			= $l_LOG_DIR

${itab2}l_Schema_TO_HDFS_Dirs		= $l_Schema_TO_HDFS_Dirs
${itab2}l_GetHDFSFileSizesScript	= $l_GetHDFSFileSizes_Script
${itab2}l_GetHDFSFileSizes_OUTPUT	= $l_GetHDFSFileSizes_OUTPUT
${itab2}l_GetHDFSFileSizes_ERR		= $l_GetHDFSFileSizes_ERR\n\n";

# Sample Input file:
#	DL_EDGE_BASE_BIS_13426_BASE_OBISDGXP_BIS        BIS_ORDER_LINE
#	DL_EDGE_BASE_BIS_13426_BASE_OBISDGXP_BIS        BIS_REMOTE_LINK_USAGE
#	DL_EDGE_BASE_BIS_13426_BASE_OBISDGXP_BIS        BIS_SUBSCRIPTION_HIST

#### Ex: output_tablesWithSIZE=/data/commonScripts/util/CHEVELLE/STAT/MDB/tbl_list3000.txt/LOGS/20180703_200828/CP_TablesSize_Try1.txt


	l_totalInputTables=$(wc -l $inputFile|cut -d" " -f1);
	printf "${itab2}Input file: $inputFile;\n${itab2}l_totalInputTables=$l_totalInputTables\n\n\n";

	printf "\n${itab2}Step 1: Combine 3 fields into 1 field, separate by '.'  (Ex: \"schema.table.HDFS_Create_Table_Dir\"):\n"
	printf "${itab3}Input File:  allTablesShowCreateTableTXT = $allTablesShowCreateTableTXT\n"
	printf "${itab3}Output File: l_Schema_TO_HDFS_Dirs       = $l_Schema_TO_HDFS_Dirs\n\n"
	l_startTime1_EPOC="$(date +%s)"

	# awk -F"$G_INPUT_FIELD_DELIMITER" '{printf ("%s.%s.%s\n", $1,$2,$3)}' $inputFile | sed 's:\s::g' > $l_Schema_TO_HDFS_Dirs
	awk -F"$G_INPUT_FIELD_DELIMITER" '{printf ("%s|%s|%s\n", $1,$2,$3)}' $inputFile | sed 's:\s::g' > $l_Schema_TO_HDFS_Dirs
	l_total_HDFSDir_Lines=$(wc -l $l_Schema_TO_HDFS_Dirs | cut -d" " -f1)

#	DL_EDGE_BASE_BIS_13426_BASE_OBISDGXP_BIS.BIS_ORDER_LINE.hdfs://edwbidevwar/DEV/DL/EDGE_BASE/BIS-13426/OBISDGXP-BIS/BIS_ORDER_LINE/Data/

#			# Grep either of these 2 lines:   "CREATE TABLE" and  "hddf:://"
#			# NOTE: The \> in the Grep command:  Some table has the word "_TABLE" in the tablename
#			#       Hence needs this \> to delimited it correctly
#			# This is for:  CREATE TABLE, CREATE EXTERNAL TABLE
#	
#	
#			#---------------------------------------------------------------------------------------------------
#			# Step 1: Sample Input file: $allTablesShowCreateTableTXT
#			#	CREATE EXTERNAL TABLE `DL_EDGE_BASE_BIS_13426_BASE_OBISDGXP_BIS.BIS_ORDER_LINE`(
#			#	  `dw_mod_ts` string,
#			#	  `dw_job_id` string,
#			#---------------------------------------------------------------------------------------------------
#		grep -E "^CREATE.*TABLE\>|hdfs://" $allTablesShowCreateTableTXT	|\
#		awk '/^CREATE.*TABLE/   {
#			printf ("%s,", $0);
#			getline l
#			printf ("%s\n", l);
#		}' > $l_HIVE_Create_Tables_Commands
#	
#		sed	-e "s:	: :g"           \
#			-e "s:'::g"             \
#			-e "s:\`::g"            \
#			-e "s:(,::"             \
#			-e "s:CREATE.*TABLE ::" \
#			-e "s:^ *::"            \
#			-e "s: *$::"            \
#			-e "s:/$::"             \
#			-e "s:$:/:"		\
#			-e "s:  *: :"	$l_HIVE_Create_Tables_Commands  > $l_Schema_TO_HDFS_Dirs
#				# Sample output:        
#				#                       | one space only                          | Must have this LAST "/" to delimit names (no overlap)
#				#                       v                                         v like "MDB_2/" vs "MDB_20/" )
#				#	DEDWDLLOAD.MDB_2 hdfs://edwbidevwar/user/qzpftv/Data/MDB_2/
#		printf "${itab3}$(GetElapsedTime $l_startTime1_EPOC)\n\n";
#	
#	
#		l_total_HDFSDir_Lines=$(wc -l $l_Schema_TO_HDFS_Dirs | cut -d" " -f1)
#		printf "${itab3}l_totalInputTables=$l_totalInputTables;  l_total_HDFSDir_Lines=$l_total_HDFSDir_Lines\n"
#		if [[ $l_totalInputTables -ne $l_total_HDFSDir_Lines ]]
#		then
#			grep -w "FAILED:" $allTablesShowCreateTableTXT >$l_HIVE_FAILED_Create_Tables
#				# Example:  FAILED: SemanticException Unable to fetch table ATT_GM_FUNDED_DATA_PACKAGE. java.security.AccessControlException: Permission denied: user=dedwdlload, access=READ, inode="/DEV/EDW/CUST/EDGE_BASE/bis-13426/OBISDGXP-ATT/ATT_GM_FUNDED_DATA_PACKAGE/Data":dedwcust:gtedwcustn:drwxr-x--x
#	
#			if [[ -f $G_EMAIL_FILE ]]
#			then	printf "\n\nFrom function ExtractFileSizes_FromHDFS_FilesListing:\n" >> $G_EMAIL_FILE
#			fi;
#	
#			printf "\n\n\n
#	*****
#	***** WARNING: These 2 counts SHOULD match.  l_totalInputTables=$l_totalInputTables;  l_total_HDFSDir_Lines=$l_total_HDFSDir_Lines
#	*****
#		If they are different that could be because of:
#			Privilege issues, or
#			Table not exists, or 
#			Something similar
#	
#		Below are actual FAILED messages from CREATE TABLE Input file: ($l_HIVE_FAILED_Create_Tables)
#	\n"		 | tee -a $G_EMAIL_FILE
#	
#			printf "\t~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n"
#			nl  $l_HIVE_FAILED_Create_Tables | tee -a $G_EMAIL_FILE
#			printf "\t~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n\n\n" | tee -a $G_EMAIL_FILE
#			#Do not exit so it can continue on
#		fi;
#	


#---------------------------------------------------------------------------------------------------
# STEP 1: Sample OUTPUT File: $l_Schema_TO_HDFS_Dirs: /data/commonScripts/util/CHEVELLE/STAT/DC/dc_list800.txt/LOGS/20180910_214429/Schema_TO_HDFS_Dirs.txt
#	DL_EDGE_BASE_BIS_13426_BASE_OBISDGXP_BIS.BIS_ORDER_LINE hdfs://edwbidevwar/DEV/DL/EDGE_BASE/BIS-13426/OBISDGXP-BIS/BIS_ORDER_LINE/Data/
#---------------------------------------------------------------------------------------------------


	let batch=200
	printf "\n${itab2}Step 2: Generate HDFS Script containing \"hadoop fs -ls -R hdfs://...  \" (for all tables) to get filesizes (in batches: $batch):\n"
	printf "${itab3}Input File    = $l_Schema_TO_HDFS_Dirs\n";
	printf "${itab3}Output Script = $l_GetHDFSFileSizes_Script\n";
	printf "${itab3}l_total_HDFSDir_Lines = $l_total_HDFSDir_Lines\n";
	printf "${itab3}l_totalInputTables    = $l_totalInputTables\n\n\n";

	l_startTime1_EPOC="$(date +%s)"
	> $l_GetHDFSFileSizes_Script
	let N=0;
	let b=0;
	let e=0;
	while  [[ $b -le $l_total_HDFSDir_Lines ]]
	do
		N=$((N+1))
		b=$((b+1));             #b=BeginIndex
		e=$((b+batch-1));       #e=EndIndex

		printf "${itab3}BatchNum=$N;\tb=$b\te=$e\n"
		#Its ok if e is bigger than l_total_HDFSDir_Lines  Awk will limit to it
		#NOTE: Add line "/tmp_NON_EXISTENT_END_BATCH" as LAST LINE of file due to the previous "\"
		#### awk -F"."	'	BEGIN             {printf ("hadoop fs -ls -R\t%s\n",     "\\")}
		awk -F"|"	'	BEGIN             {printf ("hadoop fs -ls -R\t%s\n",     "\\")}
					NR=='$b',NR=='$e' {printf ("\t%s\t%s\n",             $3, "\\")}
					END               {printf("\t%s\n\n",                    "/tmp_NON_EXISTENT_END_BATCH")}
				' $l_Schema_TO_HDFS_Dirs  >> $l_GetHDFSFileSizes_Script
		b=$((e))
	done;
	printf "${itab3}$(GetElapsedTime $l_startTime1_EPOC)\n\n";

	#
	# Sample output: $l_GetHDFSFileSizes_Script
	#	hadoop fs -ls -R        \
	#	        hdfs://edwbidevwar/DEV/DL/EDGE_BASE/BIS-13426/OBISDGXP-BIS/BIS_ORDER_LINE/Data/ 	\
	#	        hdfs://edwbidevwar/DEV/DL/EDGE_BASE/BIS-13426/OBISDGXP-BIS/BIS_SURVEY_QUESTION/Data/    \
	#
	# The last line will give:
	#	ls: `/tmp_NON_EXISTENT_END_BATCH': No such file or directory

	printf "${itab2}Step 3: Execute HDFS script above to get actual HDFS filesizes (from ls -l)\n";
	printf "${itab3}Input File  = $l_GetHDFSFileSizes_Script\n"
	printf "${itab3}Output File = $l_GetHDFSFileSizes_OUTPUT\n"
	printf "${itab3}Err File    = $l_GetHDFSFileSizes_ERR\n\n"
	l_startTime1_EPOC="$(date +%s)"
	chmod 775 $l_GetHDFSFileSizes_Script 
	rm $l_GetHDFSFileSizes_OUTPUT $l_GetHDFSFileSizes_ERR >/dev/null 2>&1
	$l_GetHDFSFileSizes_Script   2> $l_GetHDFSFileSizes_ERR | grep "^-" >$l_GetHDFSFileSizes_OUTPUT 
	printf "${itab3}$(GetElapsedTime $l_startTime1_EPOC)\n\n";

	if [[ -s $l_GetHDFSFileSizes_ERR ]]
	then
		printf "\n\nWARNING: Found HDFS ERR file when attempting to calculate FILESIZES: $l_GetHDFSFileSizes_ERR\n"
		printf "\tThis is not ideal, but ok.  (It will be assigned 10 Executors.)  Please review file and correct any errors.\n\n\n"
		# NOT exit 
	fi;

# Sample ouptput: $l_GetHDFSFileSizes_OUTPUT:
#-rwxrwxrwx 3 xxx xxx  56473038 2018-08-02 10:02 /DEV/DL/EDGE_BASE/BIS-13426/OBISDGXP-BIS/BIS_VEHICLE_HIST/Data/BUSIN....dat
#-rwxrwxrwx 3 xxx xxx  56492800 2018-08-02 10:03 /DEV/DL/EDGE_BASE/BIS-13426/OBISDGXP-BIS/BIS_VEHICLE_HIST/Data/BUSINE.....dat
#-rwxrwxr-x 3 xxx xxx     15254 2017-10-11 15:21 /DEV/DL/EDGE_BASE/GM3PD-11150/GM3PD01P-GM3PD/BODY_STYLE_CD/Data/part-r-00000.dat
#

	l_startTime0_EPOC="$(date +%s)"
	printf "\n${itab2}Step 4: For each table: Extract actual HDFS FilesSizes from above output file:\n";
	printf "${itab3}l_Schema_TO_HDFS_Dirs     = \"$l_Schema_TO_HDFS_Dirs\"\n";
	printf "${itab3}l_GetHDFSFileSizes_OUTPUT = \"$l_GetHDFSFileSizes_OUTPUT\"\n\n";

	let N=0
	let totalTablesWithSize=0
	let totalTablesNOSize=0
	#for line in $(sed -e 's:\t:.:' $inputFile)	#Combine into 1 words for FOR loop
	for line in $(cat $l_Schema_TO_HDFS_Dirs)	#Already combined, separate by "."
	do	#--{
		N=$((N+1));
		#wordscount=$(echo $line|awk -F"." 'END {print NF}');	#Count how many words on line
		wordscount=$(echo $line|awk -F"|" 'END {print NF}');	#Count how many words on line

		### if [[ $wordscount -ne 2 ]]	# input line must have only 2 words per line, separated by tab
		if [[ $wordscount -ne 3 ]]	# input line must have 3 words per line: "schema.table.hdfsdir"
		then
			printf "\t\t$N/$l_totalInputTables)\tERROR: Bad input line: \"$line\" (wordscount=$wordscount) ==> IGNORE THIS LINE\n"
			continue;  # go to next line
		fi;

		let numExecutors=0
		# schema=$(    echo $line   |cut -d"." -f1)
		# table=$(     echo $line   |cut -d"." -f2)

		schema=$(    echo $line   |cut -d"|" -f1)
		table=$(     echo $line   |cut -d"|" -f2)

		schema_lc=$( echo $schema |tr "[A-Z]" "[a-z]" );	# LowerCase to match hives Describe Extended
		table_lc=$(  echo $table  |tr "[A-Z]" "[a-z]" );

		printf "
${itab3}schema    = \"$schema\";		schema_lc = \"$schema_lc\"
${itab3}table     = \"$table\";			table_lc  = \"$table_lc\"
		\n"
		#                                           |
		#                                           v

#l_fileHDFSLocation=$(grep "${schema}.${table}\>" $l_Schema_TO_HDFS_Dirs  |cut -d" " -f2);
#totalHDFSLocations=$(echo $l_fileHDFSLocation|wc -l|cut -d" " -f1);

		#l_fileHDFSLocation=$(grep "${schema}.${table}\>" $l_Schema_TO_HDFS_Dirs  |cut -d"." -f3);

		#l_fileHDFSLocation=$(   echo $line   |cut -d"." -f3)
		l_fileHDFSLocation=$(    echo $line   |cut -d"|" -f3)
		totalHDFSLocations=$(echo $l_fileHDFSLocation|wc -l|cut -d" " -f1);
		if [[ $totalHDFSLocations -gt 1 ]]
		then	printf "\n\n${itab3}***** ERROR: Found more than 1 locations for this table.  totalHDFSLocations=\"${totalHDFSLocations}\"\n\n";
			printf "\n${itab3}==> This is not correct, but NOT fatal (just using more executors than actually needed)\n\n"

		fi;

		printf "${itab3}Table $N/$l_totalInputTables)\t\"${schema}.${table}\";  totalHDFSLocations=$totalHDFSLocations\n"
		printf "${itab5}HDFSLocation: \"$l_fileHDFSLocation\"\n"

		if [[ -z "$l_fileHDFSLocation" ]]
		then	#--{
			# Don't save this table into output file
			printf "\n${itab4}WARNING: No HDFS location found in Extracted File ==> This means this \"schema.table\" does NOT exists in Hive MetaData\n"
			printf "\n${itab5}Per Requirement this table needs to show up in Final Output as \"TABLE NOT FOUND\"\n"
			printf "${itab4}==> Set Num_Executors=10 so it could be captured in PySpark's Output File\n\n\n"
			printf "10,${schema}.$table,0\n" >> $output_tablesWithSIZE

		else	#--}{
			#Display content first for user to see:

			######  hadoop fs -ls ${l_fileHDFSLocation}/*dat 	# This extension *dat is NOT correct, as some data files DO NOT have extension:
			# Examples:
			#	hdfs://edwbidevwar/DEV/DL/EDGE_BASE/BIS-13426/OBISDGXP-BIS/BIS_ORDER_LINE/Data/000000_0
			#	hdfs://edwbidevwar/DEV/DL/EDGE_BASE/BIS-13426/OBISDGXP-BIS/BIS_ORDER_LINE/Data/000000_0

			#------------------------------------------------------------------------------------------------------------------
			# This uses up too much time just to display for user that not really reading these.  So don't display to save time
			#	printf "\n${itab4}These are the data files found for this table:\n\n"
			#	#### hadoop fs -ls ${l_fileHDFSLocation}/* | sed "s:^:${itab4}:"
			#	hadoop fs -ls -R ${l_fileHDFSLocation} | grep "^-" | sed "s:^:${itab4}:"
			#		# Use -R because it is possible that there could be subdirectories that have data in it, 
			#		# such as in a UNION ALL operation.  The grep "^-" picks out files only, no directory
			#		# Most of the time there is only a single directory
			#	printf "\n"
			#------------------------------------------------------------------------------------------------------------------
	
				# Get filesize from HDFS ls command (not form DESCRIBE or ANALYZE Hive commands)
				# Most of the time there should be some file(s) there, so there should be fileSize="NNN"`
				# If table is created 1st time		=> No file available => fileSize="0"
				# If files are removed (ie truncate)	=> No file available => fileSize="0"


			#                                   |
			#                                   v delimiter
				#hdfs_dir="$(grep "${schema}.${table}\>"  $l_Schema_TO_HDFS_Dirs     | awk '{print $2}')"
			### hdfs_dir=$(    echo $line   |cut -d"." -f3)
			hdfs_dir=$(    echo $line   |cut -d"|" -f3)

			#fileSizeLines=$(grep -w "${hdfs_dir}$"   $l_GetHDFSFileSizes_OUTPUT | wc -l | cut -d" " -f1) ; 
			#fileSize=$(     grep -w "${hdfs_dir}$"   $l_GetHDFSFileSizes_OUTPUT | awk 'BEGIN{total=0} {total=total +$5} END {printf ("%d\n", total)}') ; 

			fileSizeLines=$(grep -w "${hdfs_dir}"   $l_GetHDFSFileSizes_OUTPUT | wc -l | cut -d" " -f1) ; 
			fileSize=$(     grep -w "${hdfs_dir}"   $l_GetHDFSFileSizes_OUTPUT | awk 'BEGIN{total=0} {total=total +$5} END {printf ("%d\n", total)}') ; 

# DEBUG
# fileSize=$((RANDOM%20*100000000))
# fileSize=0

			# Example: hdfs_dir="hdfs://edwbidevwar/user/qzpftv/Data/MDB1/"
			printf "${itab5}hdfs_dir=\"$hdfs_dir\";\t\tfileSizeLines=\"$fileSizeLines\";\t==>\tfileSize=\"$fileSize\"\n"

			RC=$?
			if [[ $RC -eq 0 ]]
			then	#--{
				if [[ "$fileSize" == "" ]] 	|| [[ "$fileSize" == "0" ]]
				then	# Could be empty if 1) this is 1st time (no data loaded yet), or 2) data files were removed
	
					if [[ -z $userSpecified_NUM_EXCUTORS ]]
					then
						let numExecutors=$default_NUM_EXCUTORS
						printf "\n${itab4}A. fileSize is empty='$fileSize'\t==> Set to default_NumExecutors=$default_NUM_EXCUTORS\n"
						printf "$numExecutors,${schema}.$table,$fileSize\n" >> $output_tablesWithSIZE
						totalTablesWithSize=$((totalTablesWithSize+1))
					else
						let numExecutors=$userSpecified_NUM_EXCUTORS
						printf "\n${itab4}B. fileSize is empty='$fileSize'\t==> Set to Default userSpecified_NUM_EXCUTORS=$userSpecified_NUM_EXCUTORS\n"
						printf "$numExecutors,${schema}.$table,$fileSize\n" >> $output_tablesWithSIZE
						totalTablesWithSize=$((totalTablesWithSize+1))
					fi;
				else
					CalculateNumExecutor  $fileSize  $chunkSize $itab2
					printf "\n${itab4}==> fileSize='$fileSize';  chunkSize=$chunkSize; \t==> numExecutors=\"$numExecutors\";"
					RoundUpToNext10 $numExecutors $itab2;  #If this function not wanted, just comment this 1 line out
	        			if   [[ $numExecutors -lt $G_MIN_NUM_EXECUTORS ]]; then let numExecutors=$G_MIN_NUM_EXECUTORS;
				        elif [[ $numExecutors -gt $G_MAX_NUM_EXECUTORS ]]; then let numExecutors=$G_MAX_NUM_EXECUTORS;
				        fi;
					printf "\tAfter RoundUp ==> numExecutors=\"$numExecutors\"\n\n"
					printf "$numExecutors,${schema}.$table,$fileSize\n" >> $output_tablesWithSIZE
				fi;
			else	#--}{
				printf "\n\n\n***** ERROR: RC=$RC\n\n\n"
				printf "${itab1}<== function ExtractFileSizes_FromHDFS_FilesListing;\t\t\t$(GetCurrTime)\n\n";
				exit -1
			fi;	#--}
		fi;	#--}
	done;	#--}
	printf "${itab3}Step 4: $(GetElapsedTime $l_startTime0_EPOC)\n\n";
	printf "${itab2}Output File containing 3 fields: Executors,Schema.TableName,FileSize: \"$output_tablesWithSIZE\"\n\n";
	printf "${itab1}<== function ExtractFileSizes_FromHDFS_FilesListing;\t\t$(GetElapsedTime $l_startTime_EPOC)\t\t$(GetCurrTime)\n\n";
}


function SortTablesByNumExecutors
{
	local inputFile="$1"
	local outputFile="$2"
	local itab1="$3";   if [[ -z "$itab1" ]]; then itab1=""; fi; local itab2="${itab1}\t"; local itab3="${itab2}\t";
	printf "
${itab1}==> function SortTablesByNumExecutors (sort by 1st field: NumExecutors);\t\t\t$(GetCurrTime)
${itab2}inputFile  = $inputFile
${itab2}outputFile = $outputFile\n"

	# Sample:
	#	|NumExecutors
	#	|  |Schema.Table                                               |FileSize
	#	v  v                                                           v
	#	20,cca_edge_base_atds_58616_atds_atds_core.aftersales_contract,1680000
	#	10,DEDWDLLOAD.MDB4,0
	#	^  ^               ^
	#	|  |Schema.Table   |FileSize
	#	|NumExecutors

	#sort -n -r -t"," -k1 $inputFile > $outputFile
	sort -n  -t"," -k1   $inputFile > $outputFile
	printf "${itab1}<== function SortTablesByNumExecutors\n\n"
} # function SortTablesByNumExecutors


function Move_OS_and_HDFS_SplitFiles_To_Archive
{
	local toRunDir="$1";
	local archiveLINUXDir="$2";
	local hdfsINPUTDir="$3";
	local hdfsARCHIVEDir="$4";
	local sessionDate="$5";

	local itab1="$6";   if [[ -z "$itab1" ]]; then itab1=""; fi; local itab2="${itab1}\t"; 
	local itab3="${itab2}\t"; local itab4="${itab3}\t"; local itab5="${itab4}\t"; local itab6="${itab5}\t"; 

	local n  RC totalTables  totalSplitFiles  ;
	local starTime  stopTime

	printf "\n${itab1}==> function Move_OS_and_HDFS_SplitFiles_To_Archive;\t\t\t$(GetCurrTime)
${itab2}toRunDir		= $toRunDir
${itab2}archiveLINUXDir		= $archiveLINUXDir
${itab2}hdfsINPUTDir		= $hdfsINPUTDir
${itab2}hdfsARCHIVEDir		= $hdfsARCHIVEDir
${itab2}sessionDate		= $sessionDate
\n";

	totalSplitFiles=$(ls -1 $toRunDir | wc -l|cut -d" " -f1)

	#Display at beging due to its long line:
	printf "\n${itab2}LOOP thru directory TORUN to move each Split File to OS and HDFS ArchiveDir:\n"
	let n=0;
	for splitFile in $(ls -1 $toRunDir | sort -k9)
	do	#{
		printf "${itab2}Logic:\n"
		printf "${itab2}	1- Each SplitFile has 1 or more tables within it\n"
		printf "${itab2}	2- Each PARA Group has 1 or more SplitFiles within it\n"
		printf "${itab2}	3- Each PARA Group execute SERIALLY:  GROUP1, then GROUP 2 ...\n"
		printf "${itab2}	4- All SplitFiles within each PARA Group execute PARALLELY.  That's why each SplitFile has its own YARN Log...\n\n"

		n=$((n+1))  
		printf "\n${itab3}SPLIT File $n/$totalSplitFiles) \"$splitFile\";  \n";

		printf "${itab4}A) Move HDFS Split_File, $splitFile, from HDFS INPUT Dir to HDFS Archive Dir: ($hdfsARCHIVEDir)\n";
		MoveHDFSFileToHDFSArchive  "$splitFile"  "$hdfsINPUTDir"  "$hdfsARCHIVEDir"  "$sessionDate"  "${itab5}"
			
		printf "\n${itab4}B) Move OS Split_File, $splitFile, from toRunDir ($toRunDir) to archive_LINUX_Dir ($archiveLINUXDir)\n";
		mv ${toRunDir}/${splitFile} $archiveLINUXDir
		RC=$?
		if [[ $RC -ne 0 ]]
		then	printf "\n\n\n***** ERROR: Cannot remove splitFile;  RC=$RC;\t\t$(GetCurrTime)\n\n";
			printf "${itab1}<== function Move_OS_and_HDFS_SplitFiles_To_Archive\n\n"
			exit -1;
		fi;
		printf "\n";
	done;	#}
	printf "${itab2}END LOOP;\n\n"
	printf "${itab1}<== function Move_OS_and_HDFS_SplitFiles_To_Archive\t\t$(GetCurrTime)\n\n"
} #function Move_OS_and_HDFS_SplitFiles_To_Archive


function ExtractSTDOUTfromYARNlogs
{
	local l_SubmitMode=$1
	local l_SplitFile_Log=$2
	local l_YARN_LogDir=$3
	local l_INVOKE_SPARK_LOG="$4"

	local itab1="$5";   if [[ -z "$itab1" ]]; then itab1=""; fi; local itab2="${itab1}\t"; 
	local itab3="${itab2}\t"; local itab4="${itab3}\t"; local itab5="${itab4}\t"; local itab6="${itab5}\t"; 
	local RC   l_YarnAppID

	# NOTE: Sometimes the job Might not get on the QUEUE yet

	#printf "${itab1}==> function ExtractSTDOUTfromYARNlogs\t\t$(GetCurrTime)\n"	|tee -a $l_INVOKE_SPARK_LOG

	printf "\n${itab1}==> function ExtractSTDOUTfromYARNlogs\t\t$(GetCurrTime)\n"	>> $l_INVOKE_SPARK_LOG
	printf "${itab2}l_SubmitMode		= $l_SubmitMode\n"							>> $l_INVOKE_SPARK_LOG
	printf "${itab2}l_YARN_LogDir		= cd $l_YARN_LogDir\n"						>> $l_INVOKE_SPARK_LOG
	printf "${itab2}l_SplitFile_Log		= view $l_SplitFile_Log\n"					>> $l_INVOKE_SPARK_LOG
	printf "${itab2}l_INVOKE_SPARK_LOG	= view $l_INVOKE_SPARK_LOG\n\n"				>> $l_INVOKE_SPARK_LOG

	if [[ "$l_SubmitMode" == "YARN" ]] || [[ "$l_SubmitMode" == "yarn" ]]	
	then #--{
		if [[ ! -s $l_SplitFile_Log ]]
		then	printf "\n\n\n\nERROR:  Cannot find SplitFile_Log:  $l_SplitFile_Log\n\n\n\n" >> $l_INVOKE_SPARK_LOG
		else
			l_YarnAppID=$(grep "INFO Client: Submitting application .* to ResourceManager" $l_SplitFile_Log | head -1 |awk 'END {print $7}')
			printf "${itab2}===== YarnAppID=\"$l_YarnAppID\"\n"						>> $l_INVOKE_SPARK_LOG

			if [[ -z "$l_YarnAppID" ]] 
			then
				printf "\n\n\nERROR: Cannot get YarnAppID from YARN LOG $l_SplitFile_YARN_LOG\n\n\n"	>> $l_INVOKE_SPARK_LOG
			else
				sleep 10;	#sleep a little in case YARN not complete wrappring up yet
				yarn logs -applicationId  $l_YarnAppID   -out $l_YARN_LogDir   > ${l_YARN_LogDir}/yarn_ScreenMessage_1st_TRY.txt  2>&1
					# It creates many subdirs under l_YARN_LogDir. In each subdir there is usually 1 text file
					# One of these text files contain string: "Python Job starts at"
					# (as all Python program outputs this line as the 1st line, per our protocol)

				RC=$?
				if [[ $RC -eq 0 ]]	#--{
				then	
					printf "\n${itab2}RC=$RC;  Successfully extracted log from YARN (1st Try), saved at: $l_YARN_LogDir\n\n"	>> $l_INVOKE_SPARK_LOG
					printf "This is the log containing Python Output:\n"														>> $l_INVOKE_SPARK_LOG
					printf "=====================================================================================================================\n"	>> $l_INVOKE_SPARK_LOG
							#Add token "YARN_LOG_DIR   view"  to this log
							#The caller (function Execute) greps for this token 
							#So if change, must change at 2 places.
							#Note:  The grep specifies */*:  1st = subdir, 2nd=text file;
							#So this combo is a list of textfiles, for grep to search for string "Python Job starts at"

							# Add the token   "YARN_LOG_DIR   view" to begin of output string
					grep -i -w -l "Python Job starts at"  ${l_YARN_LogDir}/*/* | sed 's:^:YARN_LOG_DIR   view  :'										>> $l_INVOKE_SPARK_LOG
					printf "\n=====================================================================================================================\n"	>> $l_INVOKE_SPARK_LOG
					printf "\n"											>> $l_INVOKE_SPARK_LOG

					printf "\n\nA. Content at l_YARN_LogDir: $l_YARN_LogDir\n"	>> $l_INVOKE_SPARK_LOG
					ls -ltr $l_YARN_LogDir										>> $l_INVOKE_SPARK_LOG
					printf "\n\n"												>> $l_INVOKE_SPARK_LOG

				else	#--}{
					printf "\n\n\nWARNING: Cannot extract Yarn Log on 1st Try;  l_YarnAppID = \"$l_YarnAppID\" ; RC=$RC\n"	>> $l_INVOKE_SPARK_LOG
					printf "\tUsually this means: Spark Job has failed, or it's not ready yet!\n\n\n"						>> $l_INVOKE_SPARK_LOG
					printf "\tSleep for a 10 seconds and Try Again: \n"														>> $l_INVOKE_SPARK_LOG

					sleep 10;	#sleep a little in case YARN not complete wrappring up yet
					yarn logs -applicationId  $l_YarnAppID   -out $l_YARN_LogDir   > ${l_YARN_LogDir}/yarn_ScreenMessage_2nd_TRY.txt  2>&1
					RC=$?

					if [[ $RC -eq 0 ]]	#--{
					then	
						printf "\n${itab2}RC=$RC;  Successfully extracted log from YARN (2nd Try), saved at: $l_YARN_LogDir\n\n"	>> $l_INVOKE_SPARK_LOG
						printf "This is the log containing Python Output:\n"																				>> $l_INVOKE_SPARK_LOG
							# Add the token   "YARN_LOG_DIR   view" to begin of output string
						printf "=====================================================================================================================\n"	>> $l_INVOKE_SPARK_LOG
						grep -i -w -l "Python Job starts at"  ${l_YARN_LogDir}/*/* | sed 's:^:YARN_LOG_DIR   view  :'										>> $l_INVOKE_SPARK_LOG
						printf "\n=====================================================================================================================\n"	>> $l_INVOKE_SPARK_LOG
						printf "\n"													>> $l_INVOKE_SPARK_LOG

						printf "\n\nB. Content at l_YARN_LogDir: $l_YARN_LogDir\n"	>> $l_INVOKE_SPARK_LOG
						ls -ltr $l_YARN_LogDir										>> $l_INVOKE_SPARK_LOG
						printf "\n\n"												>> $l_INVOKE_SPARK_LOG
					else	#--}{
						#This is the token that another procedure search for.  So do not change it unless both places are changed
						printf "\n\n\nWARNING: Cannot extract Yarn Log on 2nd Try;  l_YarnAppID = \"$l_YarnAppID\" ; RC=$RC\n"	>> $l_INVOKE_SPARK_LOG
					fi;	#--}
				fi;	#--}
			fi;
		fi;
	else	#--}{
		printf "\n${itab2}NOT Extract YARN log due to LOCAL MODE: (SUBMIT mode: \"$l_SubmitMode\")\n"	>> $l_INVOKE_SPARK_LOG
		printf "${itab2}Output file is: $l_SplitFile_Log\n"						>> $l_INVOKE_SPARK_LOG
	fi;	#--}
	printf "${itab1}<== function ExtractSTDOUTfromYARNlogs\t\t$(GetCurrTime)\n\n" >> $l_INVOKE_SPARK_LOG
}


function func_invoke_PySpark  
{
		local l_origInputFile=$1;	    # Ex: mdb_list.txt
		local l_toRunDir="$2";
		local l_splitFile=$3;	    
		# Ex: /data/commonScripts/util/CHEVELLE/STAT/MDB/tbl_list100.txt/TORUN/20180510_095532__100__1__tbl_list100.txt
		#                                            ^          ^                    ^
		#                                            |          |                    |l_SplitFile with Date
		#                                            |          |
		#                                            |          | Original File                    
		#                                            | 
		#                                            |Project (-P)

		local l_numExecutors=$4;		# Ex: 10 or 20 ....
		local l_sparkName=$5;			# 
		local l_hdfsINPUTDir="$6";		#
		local l_hdfsOUTPUTDir="$7";		#
		local l_hdfsCHECKPOINTdir=$8;	#
		local l_hdfsARCHIVEDir=$9;		#
shift;	local l_hdfsHIVEDATATYPESDir=$9;#
shift;	local l_YARN_LOG_DIR=$9;		#
shift;	local l_sessionDate="$9";		# Ex: YYYYMMDD_HHMMSS
shift;	local l_submitMaster="$9";		#
shift;  local l_deployMode="$9";		#
shift;  local l_maxRetryThreshold="$9";	#
shift;  local l_must_have_HIVE_datatypes="$9";
shift;  local l_SplitFile_SUCCESS_FLAG="$9"
shift;  local l_SplitFile_FAILED_FLAG="$9"
shift;  local l_SplitFile_YARN_LOG="$9"
shift;  local l_INCLUDE_SKIP_DATA_IN_OUTPUT="$9"
shift;  local l_NthSplitFile="$9"
shift;	local l_totalSplitFiles="$9"
shift;  local l_BIG_TABLE_ROWS_TO_SWITCH="$9"
shift;	local l_BIG_TABLE_TABLECOLS_BATCH_SIZE="$9"
shift;  local l_BIG_TABLE_STATCOLS_BATCH_SIZE="$9"
shift;  local l_INVOKE_SPARK_LOG="$9"
shift;	local l_PROGRAM_TYPE="$9"
shift;	local l_BATCH_ID="$9"

shift;	local itab1="$9";
	if [[ -z "$itab1" ]]; then itab1=""; fi; 
	local itab2="${itab1}\t"; local itab3="${itab2}\t"; local itab4="${itab3}\t"; local itab5="${itab4}\t"; local itab6="${itab5}\t";
	local l_fullSplitFile="${l_toRunDir}/${l_splitFile}"
	local l_totalTables=$(wc    -l $l_fullSplitFile | cut -d" " -f1);
	local l_totalTablesSTR="table"
	if [[ $l_totalTables -gt 1 ]]; then l_totalTablesSTR="tables"; fi;

	local l_startTime="$( date +"%Y%m%d %H:%M:%S")"
	local l_startTime_EPOC="$(date +%s)"
	local RC    l_stopTime    endTime   endTime_EPOC   seconds    l_BKG_Job_Complete  l_BKG_Process_Count   l_YarnAppID

	local l_splitFileHDFSCheckPointDir="${l_hdfsCHECKPOINTdir}/${l_splitFile}"
		# Ex: l_splitFileHDFSCheckPointDir = /DEV/.../Checkpoint/tbl_list100.txt/20180608_121711__040__4__tbl_list100.txt
		# It has the date in front: "20180608_121711___" 
		# This date gives the idea when it was created.
		# Also, the files from TORUN has exact same date.  That's why it can find data from previous checkpoints

	local l_splitFileHDFSCheckPoint_SUCCESS_Dir="${l_splitFileHDFSCheckPointDir}/SUCCESS"
	local l_splitFileHDFSCheckPoint_ATTEMPTED_Dir="${l_splitFileHDFSCheckPointDir}/ATTEMPTED"
	local l_splitFileHDFSCheckPoint_ROWSDATA_Dir="${l_splitFileHDFSCheckPointDir}/ROWSDATA"

		### local l_splitFileHDFS_HIVE_ColumnTypes_Dir="${l_splitFileHDFSCheckPointDir}/HIVE_COLUMN_TYPES"
	local l_splitFileHDFS_HIVE_ColumnTypes_Dir="${l_hdfsHIVEDATATYPESDir}";	#Unlike other dir, this dir is NOT to be removed after this session
										#The file will be replaced by OP people
	#### local l_newSparkName="${l_sparkName}__e${l_numExecutors}__t${l_totalTables}"
	local l_newSparkName="${l_sparkName}__e${l_numExecutors}__t${l_totalTables}__b${l_BATCH_ID}"
		# ex: MDB___j1__e20__t100__b123:     j=JobNum #1;  e=20 Executors;  t=100 Tables;  b=Batch_id
		#  This batch_id on YARN job is useful to compre with BATCH_ID in the QUEUE table
		#  Each YARN job has 1 BATCH_ID (similar to: each job =  1 EXECUTOR)

	local l_This_Process_PID=$$	#Get PID as this could in BKG job

	rm -f $l_SplitFile_SUCCESS_FLAG >/dev/null 2>&1;	#Remove SUCCESS flag just in case
	printf "\n\n${itab1}==> function func_invoke_PySpark;\t\t\t$(GetCurrTime)

whoami = $(whoami)

${itab2}l_This_Process_PID  = $l_This_Process_PID 

${itab2}l_origInputFile     = $l_origInputFile
${itab2}l_toRunDir          = $l_toRunDir
${itab2}l_splitFile         = $l_splitFile
${itab2}l_numExecutors      = $l_numExecutors
${itab2}l_sparkName         = $l_sparkName
${itab2}l_hdfsINPUTDir      = $l_hdfsINPUTDir
${itab2}l_hdfsOUTPUTDir     = $l_hdfsOUTPUTDir
${itab2}l_hdfsCHECKPOINTdir = $l_hdfsCHECKPOINTdir
${itab2}l_hdfsARCHIVEDir    = $l_hdfsARCHIVEDir
${itab2}l_YARN_LOG_DIR      = $l_YARN_LOG_DIR
${itab2}l_sessionDate       = $l_sessionDate
${itab2}l_submitMaster	    = $l_submitMaster
${itab2}l_deployMode	    = $l_deployMode
${itab2}l_maxRetryThreshold = $l_maxRetryThreshold
${itab2}l_must_have_HIVE_datatypes	= $l_must_have_HIVE_datatypes
${itab2}l_SplitFile_SUCCESS_FLAG	= $l_SplitFile_SUCCESS_FLAG
${itab2}l_SplitFile_FAILED_FLAG		= $l_SplitFile_FAILED_FLAG
${itab2}l_SplitFile_YARN_LOG		= $l_SplitFile_YARN_LOG
${itab2}l_INCLUDE_SKIP_DATA_IN_OUTPUT	= $l_INCLUDE_SKIP_DATA_IN_OUTPUT
${itab2}l_BIG_TABLE_ROWS_TO_SWITCH	= $l_BIG_TABLE_ROWS_TO_SWITCH
${itab2}l_BIG_TABLE_TABLECOLS_BATCH_SIZE= $l_BIG_TABLE_TABLECOLS_BATCH_SIZE
${itab2}l_BIG_TABLE_STATCOLS_BATCH_SIZE	= $l_BIG_TABLE_STATCOLS_BATCH_SIZE

${itab2}l_INVOKE_SPARK_LOG	= $l_INVOKE_SPARK_LOG
${itab2}l_PROGRAM_TYPE		= $l_PROGRAM_TYPE
${itab2}l_BATCH_ID			= $l_BATCH_ID

${itab2}l_splitFileHDFSCheckPoint_SUCCESS_Dir   = $l_splitFileHDFSCheckPoint_SUCCESS_Dir
${itab2}l_splitFileHDFSCheckPoint_ATTEMPTED_Dir = $l_splitFileHDFSCheckPoint_ATTEMPTED_Dir
${itab2}l_splitFileHDFSCheckPoint_ROWSDATA_Dir  = $l_splitFileHDFSCheckPoint_ROWSDATA_Dir
${itab2}l_splitFileHDFS_HIVE_ColumnTypes_Dir    = $l_splitFileHDFS_HIVE_ColumnTypes_Dir

${itab2}l_splitFile         = $l_splitFile
${itab2}l_totalTables       = $l_totalTables
${itab2}l_startTime         = $l_startTime
${itab2}l_startTime_EPOC    = $l_startTime_EPOC
${itab2}l_newSparkName	    = $l_newSparkName

${itab2}Program  = ${G_MAIN_PROG_DIR}/${G_PYTHON_PROGRAM} 

\n" >> $l_INVOKE_SPARK_LOG

	#-------------------------------------------------------------------------------------------------------------------------------------------------
	#	${itab2}Make 4 HDFS CheckpointDirs for this l_SplitFile = $l_splitFileHDFSCheckPointDir
	#	${itab3}(HDFS Checkpoint dirs remain until the ALL input l_SplitFiles complete successfully so that 
	#	${itab3}(every run it has data (from HDFS ROWSDATA dir) to display a SAME, COMPLETE set of output rows)
	#
	# Make HDSF Directory with the l_SplitFile as part of the name, for 2 reasons:
	# 1. HDFS filename is system-generated
	# 2. Keep each checkpoint file of each l_SplitFile separate so it's easier to remove 
	#    entire Checkpoint directory in 1 command
	# 3. HDFS checkpoint direcotry has runDate in the name.  This is OK as the checkpoint process
	#    check for existence of any directory name
	# Ex: l_splitFileHDFSCheckPointDir = /DEV/EDW/SALES_MKTG/LZ/temp/qzpftv2/Checkpoint/tbl_list100.txt/20180608_121711__040__4__tbl_list100.txt
	# It has the date in front: "20180608_121711___" 
	# This gives the idea when it was created

	# No longer need HDFS CHECKPOINT dirs (SUCCESS/ATTEMPTED/ROWSDATA) as we now use Oracle QUEUE as a way of controling
	#
	#	Make_1_HDFS_Dir "$l_splitFileHDFSCheckPointDir"            "CHECKPOINT_DIR"           "${itab3}"    "$l_INVOKE_SPARK_LOG"
	#	Make_1_HDFS_Dir "$l_splitFileHDFSCheckPoint_SUCCESS_Dir"   "CHECKPOINT_SUCCESS_DIR"   "${itab3}"    "$l_INVOKE_SPARK_LOG"
	#				#Ex:  /DEV/.../Checkpoint/MDB/tbl_list1.txt/20180708_220309__j1__e10__t1__tbl_list1.txt/SUCCESS
	#	Make_1_HDFS_Dir "$l_splitFileHDFSCheckPoint_ATTEMPTED_Dir" "CHECKPOINT_ATTEMPTED_DIR" "${itab3}"     "$l_INVOKE_SPARK_LOG"
	#				#Ex:  /DEV/.../Checkpoint/MDB/tbl_list1.txt/20180708_220309__j1__e10__t1__tbl_list1.txt/ATTEMPTED
	#	Make_1_HDFS_Dir "$l_splitFileHDFSCheckPoint_ROWSDATA_Dir"  "CHECKPOINT_ROWSDATA_DIR"  "${itab3}"     "$l_INVOKE_SPARK_LOG"
	#				#Ex:  /DEV/.../Checkpoint/MDB/tbl_list1.txt/20180708_220309__j1__e10__t1__tbl_list1.txt/ROWSDATA
	#-------------------------------------------------------------------------------------------------------------------------------------------------


# To use Python package:    pytz    (to get local timezomne)
# Method 1: Use this export PYSPARK="...." 
#	export PYSPARK_PYTHON=/opt/oss/anaconda/envs/py3.5/bin/python;
# Method 2: Use this PYSPARK="...."   (no export)  and the 2 --conf   in the spark-submit call

	PYSPARK_PYTHON=/opt/oss/anaconda/envs/py3.5/bin/python;	# Use this to get package  pytz  for timezone 

	# Use the same value G_SESSION_DATE_STR for ALL Split files
	
	URL_GetTableByID_MS="";
	URL_GetWorkUnit_MS="";
	URL_PostToOutput_MS="";
	URL_PostToQueue_MS="";

	l_startTime_EPOC="$(date +%s)"





# DEBUG
#endTime="$(date +"%Y%m%d %H:%M:%S")";
#endTime_EPOC="$(date +%s)";
#seconds=$((endTime_EPOC - l_startTime_EPOC));
#printf "HI I AM HERE ....Split File: $l_splitFile:\n\n" >> $l_INVOKE_SPARK_LOG
#sleep $((1 + RANDOM % 50))
#
#printf "Split File: $l_splitFile:\tSUCCESS\t;\tTook: $seconds secs\t\t(StartTime: $l_startTime - EndTime: $endTime)\t\t$(GetCurrTime)\n" >  $l_SplitFile_SUCCESS_FLAG
#printf "Split File: $l_splitFile:\tFAILED\t;\tTook: $seconds secs\t\t(StartTime: $l_startTime - EndTime: $endTime)\t\t$(GetCurrTime)\n" >  $l_SplitFile_FAILED_FLAG
#
#return 0;



	case "$l_PROGRAM_TYPE" in
	"BSTAT")	
		printf "\n${itab2}BSTAT: Submitting Spark Program To Spark Cluster for PROGRAM_TYPE = BSTAT ... $(GetCurrTime)\n\n" >> $l_INVOKE_SPARK_LOG
		/usr/bin/spark-submit						\
			--name            $l_newSparkName		\
			--master          $l_submitMaster		\
			--deploy-mode     $l_deployMode			\
			--driver-memory   5g					\
			--executor-memory 5g					\
			--executor-cores  5						\
			--num-executors   $l_numExecutors		\
			--files /usr/hdp/current/hive-client/conf/hive-site.xml	\
			--conf spark.yarn.appMasterEnv.HADOOP_USER_NAME=$USERID	\
			--conf spark.yarn.maxAppAttempts=1						\
			--conf "spark.pyspark.python=$PYSPARK_PYTHON"			\
			--conf "spark.pyspark.driver.python=$PYSPARK_PYTHON"	\
			${G_MAIN_PROG_DIR}/${G_PYTHON_PROGRAM}					\
				"$l_splitFile"						\
				"$G_MS_GET_TABLEBYID_URL"			\
				"$G_MS_GET_WORK_UNIT_URL"			\
				"$G_MS_SAVE_BATCH_OUTPUT_URL"		\
				"$G_MS_UPDATE_QUEUE_URL"			\
				"$G_MS_AUTHENTICATE_URL"			\
				"$G_MS_OUTBOUND_URL"				\
				"$G_MS_MESSAGING_SECRET_KEY"		\
				"$G_GMT_TS_FOR_MICROSERVICE"		\
				"$l_origInputFile"					\
				"$l_hdfsINPUTDir"   				\
				"$l_hdfsOUTPUTDir"   				\
				"$l_splitFileHDFSCheckPointDir"				\
				"$l_splitFileHDFSCheckPoint_SUCCESS_Dir" 	\
				"$l_splitFileHDFSCheckPoint_ATTEMPTED_Dir" 	\
				"$l_splitFileHDFSCheckPoint_ROWSDATA_Dir"	\
				"$l_splitFileHDFS_HIVE_ColumnTypes_Dir"		\
				"$l_numExecutors"						\
				"$l_maxRetryThreshold"					\
				"$G_DF_COLUMNS_BATCH_SIZE"				\
				"$l_must_have_HIVE_datatypes"			\
				"$l_INCLUDE_SKIP_DATA_IN_OUTPUT"		\
				"$l_BIG_TABLE_ROWS_TO_SWITCH"			\
				"$l_BIG_TABLE_TABLECOLS_BATCH_SIZE"		\
				"$l_BIG_TABLE_STATCOLS_BATCH_SIZE"		\
				"$l_BATCH_ID"							\
				"$G_SRVR_NM"							\
				"$G_MS_AUTHORIZATION_KEY"				\
				"$G_DEBUG"      >$l_SplitFile_YARN_LOG       2>&1
		;;
	"DC")	
		printf "\n${itab2}DC: Submitting Spark Program To Spark Cluster for PROGRAM_TYPE = DC ... $(GetCurrTime)\n\n" >> $l_INVOKE_SPARK_LOG
		/usr/bin/spark-submit							\
			--name            $l_newSparkName			\
			--master          $l_submitMaster			\
			--deploy-mode     $l_deployMode				\
			--driver-memory   5g						\
			--executor-memory 5g						\
			--executor-cores  5							\
			--num-executors   $l_numExecutors			\
			--files /usr/hdp/current/hive-client/conf/hive-site.xml	\
			--conf spark.yarn.appMasterEnv.HADOOP_USER_NAME=$USERID	\
			--conf spark.yarn.maxAppAttempts=1						\
			--conf "spark.pyspark.python=$PYSPARK_PYTHON"			\
			--conf "spark.pyspark.driver.python=$PYSPARK_PYTHON"	\
			--py-files /data/commonScripts/util/__init__.py,/data/commonScripts/util/hyper_profiling.py	\
			${G_MAIN_PROG_DIR}/${G_PYTHON_PROGRAM}    "$l_splitFile"	\
				"$l_BATCH_ID"      >$l_SplitFile_YARN_LOG       2>&1

			#Without the 2>&1   YARN will write the "state: ACCEPTED" ... onto the screen, sharing the caller's screen
			# By using 2>&1  we force the stderr to to into  stdout also.
		;;
	esac;

	#--------------------------------------------------------------------------------------------------------------------------------
	# For Cluster execution:
	#    1- Lines line this: 18/08/27 09:21:11 INFO Client: Application report for application_1534775120910_9632 (state: RUNNING)
	#       are written to stdERR, not stdOUT.  
	#--------------------------------------------------------------------------------------------------------------------------------
	#	This YARN Log captures these info, which comes from spark-submit command: 
	#18/07/12 21:45:12 INFO Client: Application report for application_1530216133965_25977 (state: ACCEPTED)	
	#18/07/12 21:47:30 INFO Client: Application report for application_1530216133965_25977 (state: RUNNING)
	#18/07/12 21:47:34 INFO Client: Application report for application_1530216133965_25977 (state: FINISHED)
	#--------------------------------------------------------------------------------------------------------------------------------

	SPARKJOB_RC=$?
	endTime="$(date +"%Y%m%d %H:%M:%S")";
	endTime_EPOC="$(date +%s)";
	seconds=$((endTime_EPOC - l_startTime_EPOC));
	printf "${itab2}Spark Job Completed with RC=$SPARKJOB_RC;  Took: $seconds secs\t($l_startTime -> $endTime);  (l_This_Process_PID=$l_This_Process_PID)\n\n"  >> $l_INVOKE_SPARK_LOG



	# NEW: DO these step regardless whether the Spark job succeeded or failed (as we no lnoger rely on OS for Checkpoint.  
	#	We are using Oracle Queue instead.
	# ==> Remove these files regardless of SUCCESS of FAILED (because we now just use the Oracle QUEUE to control checkpoints, not HDFS anymore:

#----------------------------------------------------------------------------------------------------------------------------------------------------------
# No longer need this as we no longer create HDFS Checkpoint Dirs:
#	# Remove OS split files and HDFS CHECKPOINT files immediately if that split file is completely successful
#	printf "\n${itab2}Step 1/5) Removing HDFS CHECKPOINT Dirs of this l_Splitfile (-R): \"$l_splitFileHDFSCheckPointDir\": \n" >> $l_INVOKE_SPARK_LOG
#	# Ex: Remove this dir: /DEV/EDW/SALES_MKTG/LZ/temp/qzpftv2/Checkpoint/MDB/tbl_list1.txt/20180709_192508__j1__e10__t1__tbl_list1.txt
#	hadoop fs -rm -R ${l_splitFileHDFSCheckPointDir} >> $l_INVOKE_SPARK_LOG  2>&1
#	RC=$?
#	if [[ $RC -ne 0 ]]
#	then
#		printf "\n\n\n***** ERROR: Cannot remove HDFS CHECKPOINT dir: \"${l_splitFileHDFSCheckPointDir}\";  RC=$RC;\t\t$(GetCurrTime)\n\n\n" >> $l_INVOKE_SPARK_LOG
#		printf "${itab1}<== function func_invoke_PySpark\n\n\n" >> $l_INVOKE_SPARK_LOG
#		printf "It's OK for this error as we no longer performing HDFS CheckPoint.  We are usig Oracle QUEUE table, column: FAIL_CNT ==> DO NOT EXIT\n\n" >> $l_INVOKE_SPARK_LOG
#		# DONOT Exit if failure
#	fi;
#----------------------------------------------------------------------------------------------------------------------------------------------------------
	

	printf "\n${itab2}Step 1/4) Archiving HDFS Data Split_File \"$l_splitFile\", from HDFS INPUT Dir to HDFS Archive Dir:\n" >> $l_INVOKE_SPARK_LOG
		# Ex: Move  /DEV/EDW/.../Input/20180709_192508__j1__e10__t1__tbl_list1.txt   
		#     to    /DEV/EDW/.../Archive/MDB/SPLITFILES_OF_tbl_list1.txt/20180709_192508__j1__e10__t1__tbl_list1_20180709_192508.txt:
	MoveHDFSFileToHDFSArchive  "$l_splitFile"  "$hdfsINPUTDir"  "$hdfsARCHIVEDir"  "${l_sessionDate}"  "${itab3}"  "$l_INVOKE_SPARK_LOG"
	

	printf "\n${itab2}Step 2/4) Archiving OS Data Split_File, \"$l_splitFile\"\n" >> $l_INVOKE_SPARK_LOG
	printf "${itab3}From:\t$l_toRunDir\n${itab3}To:\t($archiveLINUXDir)\n"        >> $l_INVOKE_SPARK_LOG
		# Ex: Move OS Split_File:   /data/commonScripts/util/CHEVELLE/STAT/MDB/tbl_list1.txt/TORUN/20180709_192508__j1__e10__t1__tbl_list1.txt
		#     to archive_LINUX_Dir: /data/commonScripts/util/CHEVELLE/STAT/MDB/tbl_list1.txt/ARCHIVE
	mv ${l_toRunDir}/${l_splitFile} $archiveLINUXDir
	RC=$?
	if [[ $RC -ne 0 ]]
	then
		printf "\n\n\nERROR: Cannot move OS splitFile \"${l_toRunDir}/${l_splitFile}\" to ArchiveDir \"$archiveLINUXDir\" ; RC=$RC\n\n\n" >> $l_INVOKE_SPARK_LOG
		printf "${itab1}<== function func_invoke_PySpark\t\t$(GetCurrTime)\n\n" >> $l_INVOKE_SPARK_LOG
	fi;
	printf "\n";
	
	printf "\n\n${itab2}Step 3/4) Extract output from Python from YARN log: (SUBMIT choice: \"$G_SUBMIT_MASTER\")\n" >> $l_INVOKE_SPARK_LOG
	ExtractSTDOUTfromYARNlogs   "$G_SUBMIT_MASTER" $l_SplitFile_YARN_LOG   $l_YARN_LOG_DIR  "$l_INVOKE_SPARK_LOG"   "$itab3"

	rm -f $l_SplitFile_SUCCESS_FLAG $l_SplitFile_FAILED_FLAG  2>/dev/null

	if [[ $SPARKJOB_RC -eq 0 ]]
	then	#--{
		printf "\n\n${itab2}===== SplitFile $l_splitFile: SUCCESS;  SPARKJOB_RC=$SPARKJOB_RC;  Took: $seconds secs\t\t($l_startTime -> $endTime);  (l_This_Process_PID=$l_This_Process_PID)\n\n"  >> $l_INVOKE_SPARK_LOG

		# SUCCESS/FAILED Flag are used to communicate back to caller, as this function can execute in BKG.
		# So only this function should create this flag
		printf "\n${itab2}$l_splitFile:\tSUCCESS;\tTook: $seconds secs\t\t($l_startTime - $endTime)\n"   >> $G_EMAIL_TEMP_FILE
		printf "${itab2}Step 4/4) Creating OS SUCCESS flag to communicate back to Caller:\n${itab3}$l_SplitFile_SUCCESS_FLAG\n\n" >> $l_INVOKE_SPARK_LOG
		printf "Split File: $l_splitFile:\tSUCCESS\t;\tTook: $seconds secs\t\t(StartTime: $l_startTime - EndTime: $endTime)\t\t$(GetCurrTime)\n" >  $l_SplitFile_SUCCESS_FLAG
	
	else #--}{ if [[ $SPARKJOB_RC -eq 0 ]]

		printf "\n\n${itab2}===== SplitFile $l_splitFile: FAILED;  SPARKJOB_RC=$SPARKJOB_RC;  Took: $seconds secs\t\t($l_startTime -> $endTime);  (l_This_Process_PID=$l_This_Process_PID)\n\n"  >> $l_INVOKE_SPARK_LOG
		printf "\n${itab2}SPARKJOB_RC=$SPARKJOB_RC;\t\t$seconds secs\t\t($l_startTime - $endTime)\n\n"				>> $l_INVOKE_SPARK_LOG
		printf "\n${itab2}YARN Application ID: \"$l_YarnAppID\"\n\n"								>> $l_INVOKE_SPARK_LOG
		printf "\n${itab2}View this YARN log:  view $l_SplitFile_YARN_LOG \n"							>> $l_INVOKE_SPARK_LOG
		printf "\n${itab2}You can do either of these 2 steps to find the Error:\n"						>> $l_INVOKE_SPARK_LOG
		printf "\n${itab2}1) Use YARN UI to display the 2 logs (stdout and stderr)  for this ApplicationID \"$l_YarnAppID\":\n"	>> $l_INVOKE_SPARK_LOG
		printf "\n${itab2}2) Go into Linux directory  \"$l_YARN_LOG_DIR\" to look for these   stderr  and  stdout  logs by:\n"	>> $l_INVOKE_SPARK_LOG
		printf "\n${itab3}grep -i -w -l \"Python Job starts at\"  ${l_YARN_LOG_DIR}/*/*\n"					>> $l_INVOKE_SPARK_LOG
		printf "\n${itab2}This command returns the name of the file containing the actual Python logs\n"			>> $l_INVOKE_SPARK_LOG
		printf "\n${itab2}Edit this file to see actual content\n\n"								>> $l_INVOKE_SPARK_LOG

		# SUCCESS/FAILED Flag are used to communicate back to caller, as this function can execute in BKG:
		# So only this function should create this flag
		printf "${itab2}Step 4/4) Creating OS FAILED flag to communicate back to Caller:\n${itab3}$l_SplitFile_FAILED_FLAG\n\n" >> $l_INVOKE_SPARK_LOG
		printf "Split File: $l_splitFile:\tFAILED\t;\tTook: $seconds secs\t\t(StartTime: $l_startTime - EndTime: $endTime)\t\t$(GetCurrTime)\n" >  $l_SplitFile_FAILED_FLAG
	fi;	#--}

	printf "${itab1}<== function func_invoke_PySpark;  RC=$RC;  Took: $seconds secs\t\t$(GetCurrTime)\n\n\n" >> $l_INVOKE_SPARK_LOG
	return $RC
} #function func_invoke_PySpark  


function GetYARNLog
{
	local l_submitMaster=$1;
	local l_INVOKE_SPARK_LOG=$2;
	local l_SplitFile_YARN_Log=$3;
	local l_YARN_LogDir=$4;
	local itab1=$5
	if [[ -z "$itab1" ]]; then itab1=""; fi; 
	local itab2="${itab1}\t";  local itab3="${itab2}\t";   local itab4="${itab3}\t";   local itab5="${itab4}\t"; 

	G_YARN_LOGFILE="";
	if [[ "$submitMaster" == "yarn" ]] || [[ "$submitMaster" == "YARN" ]] #--{
	then	
		if [[ -s $l_INVOKE_SPARK_LOG ]]
		then	#--{
			# The BKG process should have completed (as the SUCCESS flag is the last step it # created_)
			yarnlog=$(grep "^YARN_LOG_DIR *view"   $l_INVOKE_SPARK_LOG | sed "s:^YARN_LOG_DIR *::" )
			if [[ -z "$yarnlog" ]]; then yarnlog="YARN Log NOT FOUND YET!"; fi;

		else	#--}{
			# This else clause should never orccurm but just in ncase.
			printf "\n\n\tERROR: Cannot find this BKG log: l_INVOKE_SPARK_LOG = $l_INVOKE_SPARK_LOG\n\n\n"

		fi;	#--}
	else	#--}{
			#yarnlog="No Log available as this is LOCAL mode\n"
			yarnlog="$l_SplitFile_YARN_Log";
	fi;	#--}
	#return "$yarnlog"	;	#Return can only return Numeric; hence must use printf
	#printf "$yarnlog"

	#printf "${itab5}(If YARN Log is in-complete, issue this command at Linux to re-generate YARN LOG:\n"
	#printf "${itab6}yarn logs -applicationId  $l_YarnAppID   -out $l_YARN_LogDir   > ${l_YARN_LogDir}/MANNUAL_yarn.log 2>&1\n"

	G_YARN_LOGFILE="$yarnlog";	#Now, assign to variable instead of return, so i can print log lines 
}


function Execute
{
		local l_origInputFile="$1";
		local l_toRunDir="$2";		# Linux Directory containing split files to execute
		local l_LOG_DIR="$3";		# Linux Directory containing SUCCESS flag (for success spark execution)
		local l_FLAG_DIR="$4";
		local archiveLINUXDir="$5";
		local sparkName="$6";
		local hdfsINPUTDir="$7";
		local hdfsOUTPUTDir="$8";
		local hdfsCHECKPOINTDir="$9";
shift;	local hdfsARCHIVEDir="$9";
shift;	local hdfsHIVEDATATYPESDir="$9"
shift;	local sessionDate="$9";
shift;	local submitMaster="$9";
shift;	local deployMode="$9";
shift;	local maxRetryThreshold="$9";
shift;	local l_must_have_HIVE_datatypes="$9"
shift;	local l_INCLUDE_SKIP_DATA_IN_OUTPUT="$9"
shift;	local l_BIG_TABLE_ROWS_TO_SWITCH="$9"
shift;	local l_BIG_TABLE_TABLECOLS_BATCH_SIZE="$9"
shift;	local l_BIG_TABLE_STATCOLS_BATCH_SIZE="$9"
shift;	local l_PROGRAM_TYPE="$9"
shift;	local l_BATCH_ID="$9"

shift;	local itab1="$9";   

	#Because the lines in this function is LONG, set itab1="" to display at beginning of line:
	itab1="";

	if [[ -z "$itab1" ]]; then itab1=""; fi; 
	local itab2="${itab1}\t";  local itab3="${itab2}\t";   local itab4="${itab3}\t";   local itab5="${itab4}\t"; 
	local itab6="${itab5}\t";  local itab7="${itab6}\t";   local itab8="${itab7}\t";   local itab9="${itab8}\t"; 
	local itab10="${itab9}\t"; local itab11="${itab10}\t"; local itab12="${itab11}\t"; 

	local l_YARN_LOG_DIR       n   m   RC     bn
	local num_executors        maxLen         l_startTime_EPOC            l_endTime      l_endTime_EPOC     l_MainStartTime_EPOC  l_secondsElapsed
	local l_BKG_PID            l_YarnAppID    l_SplitFile_SUCCESS_FLAG    l_SplitFile_YARN_LOG       l_SplitFile_TIME_LOG;
	local l_BKG_Job_Complete   l_Loop_Counter   l_INVOKE_SPARK_LOG;

	local l_BKG_PROCESSES_LIST="${l_LOG_DIR}/All_BKG_Processes.list"; # Contains list of BKG Processes info of ALL splitfiles

	printf "\n${itab1}==> function Execute;\t\t\t$(GetCurrTime)
${itab2}l_origInputFile		= $l_origInputFile
${itab2}l_toRunDir		= $l_toRunDir
${itab2}l_LOG_DIR		= $l_LOG_DIR
${itab2}l_FLAG_DIR		= $l_FLAG_DIR
${itab2}archiveLINUXDir		= $archiveLINUXDir
${itab2}sparkName		= $sparkName
${itab2}hdfsINPUTDir		= $hdfsINPUTDir
${itab2}hdfsOUTPUTDir		= $hdfsOUTPUTDir
${itab2}hdfsCHECKPOINTDir	= $hdfsCHECKPOINTDir
${itab2}hdfsARCHIVEDir		= $hdfsARCHIVEDir
${itab2}hdfsHIVEDATATYPESDir	= $hdfsHIVEDATATYPESDir
${itab2}sessionDate		= $sessionDate
${itab2}submitMaster		= $submitMaster
${itab2}deployMode		= $deployMode
${itab2}maxRetryThreshold	= $maxRetryThreshold
${itab2}l_must_have_HIVE_datatypes	= $l_must_have_HIVE_datatypes
${itab2}l_INCLUDE_SKIP_DATA_IN_OUTPUT	= $l_INCLUDE_SKIP_DATA_IN_OUTPUT
${itab2}l_BIG_TABLE_ROWS_TO_SWITCH	= $l_BIG_TABLE_ROWS_TO_SWITCH
${itab2}l_BIG_TABLE_TABLECOLS_BATCH_SIZE= $l_BIG_TABLE_TABLECOLS_BATCH_SIZE
${itab2}l_BIG_TABLE_STATCOLS_BATCH_SIZE	= $l_BIG_TABLE_STATCOLS_BATCH_SIZE

${itab2}l_PROGRAM_TYPE		= $l_PROGRAM_TYPE
${itab2}l_BATCH_ID		= $l_BATCH_ID
${itab2}l_BKG_PROCESSES_LIST	= $l_BKG_PROCESSES_LIST
\n\n";

	let total_SUCCESS_SplitFiles=0;
	let total_FAIL_SplitFiles=0;
	let totalSplitFiles=$(ls -1 $l_toRunDir | wc -l|cut -d" " -f1)
	let remainingSplitFiles=$totalSplitFiles

	if [[ $totalSplitFiles -eq 0 ]]
	then	printf "\n\n\nWARNING:  There is no files at TORUN directory to execute ($l_toRunDir)\n\n\n"
		return 0;
	fi;


	# Add some randomness to execution order:
	#	If even then SortOption="":     means sort filename normally (alphabetical)
	#	If odd  then SortOption="-1":   means sort filename reverse order 
	# The Splitfilename is like:  20180630_132100__011__1__tbl_list200.txt
	#                                              ^^^
	#                                              |||
	# These 3 chars (=numberOfExecutors) will sort correctly
	local modulo=$((G_JOB_NUM%2)); 
	if [[ $modulo -eq 1 ]]; then SortOption="-r"; else SortOption=""; fi;  
	if [[ -z "$modulo"  ]]; then SortOption=""; fi;
	printf "\n${itab2}====== G_JOB_NUM=\"$G_JOB_NUM\";  modulo\"=$modulo\"; SortOption=\"$SortOption\";\n"
	if [[ "$SortOption" == "-r" ]]; then sortOrderStr="REVERSE (Large to Small Executors)"; else sortOrderStr="NORMAL (Small to Large Executors)"; fi;
	printf "\n${itab2}======> Final list of SplitFiles to execute. SortOrder = ***** $sortOrderStr *****\n"
	ls -1 $l_toRunDir | sort  $SortOption -k9 |nl|sed 's:^:\t\t:'
	printf "\n"
	
	printf "# ===================================================================================\n" >  $l_BKG_PROCESSES_LIST
	printf "# BKG_PID,SplitFile_SUCCESS_FLAG,SplitFile_FAILED_FLAG,l_SplitFile_YARN_LOG\n" >> $l_BKG_PROCESSES_LIST
	printf "# ===================================================================================\n" >> $l_BKG_PROCESSES_LIST

	l_MainStartTime_EPOC="$(date +%s)";	# Since all scripts are spawned practically at same time, so they are have same start time

	local l_BKG_PID      l_success_Flag    l_failed_Flag     l_BKG_Job_Complete    seconds
	local l_PARA_GROUP   l_PARA_SPLITFILES_COUNT_PER_GROUP
	local l_YarnAppID    l_yarnlog


	### let G_MAX_PARA_SPLITFILES_PER_GROUP=3;
	let l_PARA_GROUP=1;
	let l_PARA_SPLITFILES_COUNT_PER_GROUP=0;
	l_BKG_PROCESSES_LIST="${l_LOG_DIR}/BKG_PROCESSES___ParaGroup_${l_PARA_GROUP}.list"; # Contains list of BKG Processes info of ALL splitfiles

	printf "\n${itab1}LOOP thru directory TORUN to execute all these SplitFiles in Parallel in BKG, per GROUP of   $G_MAX_PARA_SPLITFILES_PER_GROUP   SplitFiles each:\n"
	let n=0;
	for splitFile in $(ls -1 $l_toRunDir | sort  $SortOption -k9)
	do	#{
		n=$((n+1))
		printf "\n${itab2}===== PART 1/PARA Group #$l_PARA_GROUP:  Submit SplitFiles to BKG for PARA_GROUP #$l_PARA_GROUP:\t(Num of BKG processes=G_MAX_PARA_SPLITFILES_PER_GROUP=$G_MAX_PARA_SPLITFILES_PER_GROUP)\n";
		printf "${itab3}SPLIT File $n/$totalSplitFiles) \"${l_toRunDir}/${splitFile}\";\n";

		bn=$(basename $splitFile .txt);			# Keep entire filename, including extension
		l_YARN_LOG_DIR="${l_LOG_DIR}/YARN_LOG_DIR/${bn}"

		if [[ "$G_DEBUG"=="YES" ]]; then printf "\n${itab4}A) Create YARN LOG DIR (Linux) : $l_YARN_LOG_DIR\n"; fi;
		MakeLinuxDir $l_YARN_LOG_DIR   "${itab5}"

		exeFileNameOnly=$(printf "$splitFile" |sed 's:^...............::');	#Remove first 15 chars

		#                            splitFile="20180908_230959__j800__e012a__t2__dc_list800.txt" (<== this pattern not yet used; maybe in future)
		#
		#                            splitFile="20180908_230959__j800__e012__t2__dc_list800.txt"
		#                                                               ^ This is field #3 out of 4 fields
		#                                                               |
		#num_executors=$(echo $splitFile     | sed -e "s:^\(.*\)__\(.*\)__\(.*\)__\(.*\)__\(.*\)$:\3:"); 

		#printf "\n${itab3}exeFileNameOnly=\"$exeFileNameOnly\""
		#                            splitFile="20181005_002023__j800__e010__t1__b1452__tbl_list800.txt"
		#                                                               ^ This is field #3 out of 5 fields
		#                                                               |
		num_executors=$(echo $splitFile     | sed -e "s:^\(.*\)__\(.*\)__\(.*\)__\(.*\)__\(.*\)__\(.*\)$:\3:");
		# This gives:  e010ab
		num_executors=$(echo $num_executors | sed -e "s:[a-zA-Z]::g" -e "s:^E::"  -e "s:^0*::"); 	#remove all Aphabets and leading 0 digits
		if [[ "$num_executors" == "" ]] || [[ "$num_executors" == "000" ]]
		then	#This condition should ideally never happens, but just in case, it serves as a "catch all" case
			num_executors="10"
		fi;

		numexe_NoLeadingZeros=$(printf $num_executors|sed 's:^0*::'); 		#Remove leading 0s (it was prepended with 0s previously)


		if [[ "$G_DEBUG"=="YES" ]]; then printf "${itab4}B) Remove HDFS file in case it's already there (OK if not exists): \"$splitFile\"\n"; fi;
		hadoop    fs -rm  ${hdfsINPUTDir}/${splitFile} 2>/dev/null;
		# No need to check for RC;  OK if not exists, so don't care about RC 

		if [[ "$G_DEBUG"=="YES" ]]; then printf "${itab4}C) Put OS SplitFile into HDFS InputDir: $hdfsINPUTDir, for PySpark program to use:\n"; fi;
		# Must be able to put this file into HDFS as that is the current place where Python reads input tables
		hadoop    fs -put ${l_toRunDir}/${splitFile}   ${hdfsINPUTDir}
		RC=$?
		if [[ $RC -ne 0 ]]
		then	printf "\n\n\n***** ERROR: Cannot put OS SplitFile into HDFS: ($splitFile); RC=$RC;\t\t$(GetCurrTime)\n\n\n"
			printf "${itab1}<== function Execute\n\n"
			exit -1;
		fi;

		if [[ $l_PARA_SPLITFILES_COUNT_PER_GROUP -gt $G_MAX_PARA_SPLITFILES_PER_GROUP ]]
		then
			l_PARA_SPLITFILES_COUNT_PER_GROUP=$((0));
			l_PARA_GROUP=$((l_PARA_GROUP+1));
		fi;

		l_startTime_EPOC="$(date +%s)"

		l_SplitFile_SUCCESS_FLAG="${l_FLAG_DIR}/${splitFile}___ParaGroup_${l_PARA_GROUP}.success";
		l_SplitFile_FAILED_FLAG="${l_FLAG_DIR}/${splitFile}___ParaGroup_${l_PARA_GROUP}.failed";
		l_SplitFile_YARN_LOG="${l_LOG_DIR}/${splitFile}___ParaGroup_${l_PARA_GROUP}.yarnQUEUE";
		l_SplitFile_TIME_LOG="${l_LOG_DIR}/${splitFile}___ParaGroup_${l_PARA_GROUP}.timelog";
		l_INVOKE_SPARK_LOG="${l_YARN_LOG_DIR}/InvokeSpark___ParaGroup_${l_PARA_GROUP}.log"

		printf "${splitFile},${l_startTime_EPOC}\n" >> $l_SplitFile_TIME_LOG
		rm $l_SplitFile_SUCCESS_FLAG  >/dev/null 2>/dev/null
		> $l_INVOKE_SPARK_LOG
		l_YarnAppID="";
		l_PARA_SPLITFILES_COUNT_PER_GROUP=$((l_PARA_SPLITFILES_COUNT_PER_GROUP+1));

		printf "${itab4}D) Submit SplitFile into BKG ($splitFile) to Spark-Cluster with $numexe_NoLeadingZeros executors;  PARA_GROUP=$l_PARA_GROUP;  PARA_SPLITFILES_COUNT_PER_GROUP=$l_PARA_SPLITFILES_COUNT_PER_GROUP
${itab5}l_SplitFile_SUCCESS_FLAG= $l_SplitFile_SUCCESS_FLAG
${itab5}l_SplitFile_FAILED_FLAG	= $l_SplitFile_FAILED_FLAG
${itab5}l_SplitFile_YARN_LOG	= view $l_SplitFile_YARN_LOG
${itab5}l_INVOKE_SPARK_LOG	= view $l_INVOKE_SPARK_LOG\n"
		#### ${itab5}l_SplitFile_TIME_LOG     = $l_SplitFile_TIME_LOG


		#Execute this script in BKG so we can terminate it if necessary
		func_invoke_PySpark	"$l_origInputFile"     "$l_toRunDir"    "$splitFile"   "$numexe_NoLeadingZeros"  "$sparkName"	\
					"$hdfsINPUTDir"                "$hdfsOUTPUTDir"         "$hdfsCHECKPOINTDir"   "$hdfsARCHIVEDir"		\
					"$hdfsHIVEDATATYPESDir"        "$l_YARN_LOG_DIR"        "$sessionDate"         "$submitMaster"			\
					"$deployMode"                  "$maxRetryThreshold"     "$l_must_have_HIVE_datatypes"					\
					"$l_SplitFile_SUCCESS_FLAG"    "$l_SplitFile_FAILED_FLAG"												\
					"$l_SplitFile_YARN_LOG"        "$l_INCLUDE_SKIP_DATA_IN_OUTPUT"											\
					"$n"                           "$totalSplitFiles"														\
					"$l_BIG_TABLE_ROWS_TO_SWITCH"  "$l_BIG_TABLE_TABLECOLS_BATCH_SIZE" "$l_BIG_TABLE_STATCOLS_BATCH_SIZE"	\
					"$l_INVOKE_SPARK_LOG"          "$l_PROGRAM_TYPE"   "$l_BATCH_ID"    "$itab1"     &

				#NOTE: Pass in itab1  not itab5  because this  "func_invoke_PySpark" executes in a BKG process
				#	and has its own log.  So it can start at  itab1

		l_BKG_PID=$!
		remainingSplitFiles=$((remainingSplitFiles-1));
		printf "${itab4}BKG_PID				= $l_BKG_PID\n";
		printf "${itab4}CURRENT PARA_GROUP		= $l_PARA_GROUP\n";
		printf "${itab4}Count of SplitFiles so far	= $l_PARA_SPLITFILES_COUNT_PER_GROUP\n";
		printf "${itab4}Remaining Split Files		= $remainingSplitFiles (out of $totalSplitFiles);\n\n";

		# Save all BKG pids of this PARA GROUP  in a Comma-delimited file so we can loop over to to check them later
		printf "$l_BKG_PID,$l_PARA_GROUP,$l_SplitFile_SUCCESS_FLAG,$l_SplitFile_FAILED_FLAG,$l_SplitFile_YARN_LOG,$l_INVOKE_SPARK_LOG,$l_YARN_LOG_DIR,\n" >> $l_BKG_PROCESSES_LIST

		if [[ $l_PARA_SPLITFILES_COUNT_PER_GROUP -ge $G_MAX_PARA_SPLITFILES_PER_GROUP ]]  || [[ $remainingSplitFiles -eq 0 ]]
		then	#--{ # This IF statement checks if this new SplitFile should belong to same PARA_GROUP or new PARA_GROUP

			printf "\n\n${itab2}===== PART 2/PARA Group #$l_PARA_GROUP:  Wait for all above BKG processses to finish (PARA_GROUP=$l_PARA_GROUP)  (up to MaxWaitTime=$G_SECS_TO_WAIT_FOR_SPARK_JOB_BEFORE_ABORT secs; After that, any still-running BKG jobs will be terminated)\n"
			printf "${itab3}Total SplitFiles in this PARA_GROUP #$l_PARA_GROUP = $l_PARA_SPLITFILES_COUNT_PER_GROUP\t(l_BKG_PROCESSES_LIST=$l_BKG_PROCESSES_LIST)\n\n"

			l_MainStartTime_EPOC="$(date +%s)";	# Since all scripts are spawned practically at same time, so they are have same start time
			l_endTime="$(date +"%Y%m%d %H:%M:%S")";
			l_endTime_EPOC="$(date +%s)"; 
			l_secondsElapsed=$((l_endTime_EPOC - l_MainStartTime_EPOC)); 

			let l_TotalJobsCompleted=0;
			let l_Loop_Counter=0;
			# Loop to wait until ALL  BKG jobs completed or MaxWaitTime is reached, whichever comes first:
			while [[ $l_TotalJobsCompleted -lt $l_PARA_SPLITFILES_COUNT_PER_GROUP ]] && [[ $l_secondsElapsed -le $G_SECS_TO_WAIT_FOR_SPARK_JOB_BEFORE_ABORT  ]]
			do	#--{
				l_Loop_Counter=$((l_Loop_Counter+1))

				# start at left at line is too long to use tabs
				printf "\n${itab3}WAIT LoopCounter #$l_Loop_Counter of PARA Group #$l_PARA_GROUP (has $l_PARA_SPLITFILES_COUNT_PER_GROUP BKG processes; 1 per SplitFile):"
				printf "\t(Total Elapsed so far: $l_secondsElapsed seconds);  (MaxWaitTime=$G_SECS_TO_WAIT_FOR_SPARK_JOB_BEFORE_ABORT seconds)\t\t$(GetCurrTime)\n"

				let l_TotalJobsCompleted=0;	#Because the for loop goes thru the SAME entire list, then must reset this counter to 0 everytime
				let N=0;
				#Check every BKG process of this PARA_GROUP listed in this file:
				for line in $(grep -v "^#" $l_BKG_PROCESSES_LIST)		#--{	# Display in Same order as was created
				do	
					N=$((N+1));
					l_BKG_PID=$(           echo $line | cut -d"," -f 1);
					l_success_Flag=$(      echo $line | cut -d"," -f 3);
					l_failed_Flag=$(       echo $line | cut -d"," -f 4);
					l_SplitFile_YARN_LOG=$(echo $line | cut -d"," -f 5);
					l_INVOKE_SPARK_LOG=$(  echo $line | cut -d"," -f 6);
					l_YARN_LOG_DIR=$(      echo $line | cut -d"," -f 7);

					if [[ "$submitMaster" == "YARN" ]] || [[ "$submitMaster" == "yarn" ]]	
					then	#--{
						if [[ "$l_YarnAppID" == "NOT_ON_YARN_QUEUE_YET" ]]
						then
							if [[ -f $l_SplitFile_YARN_LOG ]]
							then
								l_YarnAppID=$(grep "INFO Client: Submitting application .* to ResourceManager" $l_SplitFile_YARN_LOG|head -1|awk 'END{print $7}');
							fi;
						fi;
					fi;	#--}
					l_YarnAppID="${l_YarnAppID:-"NOT_ON_YARN_QUEUE_YET"}"; 	# DO NOT CHANGE THIS TOKEN as another IF stmnt below uses it

					#Display COMPLETE useful messages every 20 loops; Otherwise it scrolss off screen for too far off
					MODULUS=$((l_Loop_Counter%10));

					# if [[ $MODULUS -eq 0 ]] # Every 10 times because in production there are many scriptFiles for every run
					if [[ $MODULUS -eq 0 ]] || [[ $l_Loop_Counter -eq 1 ]]	
					then	#--{
						printf "${itab4}(Same info as before, just in full for debugging if necessary)\n"
						# Display at itab1 not itab5 to save output space
						printf "${itab4}#$N) BKGPID = \"$l_BKG_PID\"\n";
						printf "${itab5}Success Flag     = view \"$l_success_Flag\"\n";
						printf "${itab5}Failed  Flag     = view \"$l_failed_Flag\"\n";
						printf "${itab5}INVOKE_SPARK_LOG = view \"$l_INVOKE_SPARK_LOG\"\n";
						printf "${itab5}YARN App ID      = \"$l_YarnAppID\"\n";
						printf "${itab5}YARN_LOG_DIR     = \"$l_YARN_LOG_DIR\"\n";
						printf "${itab5}YARN QUEUE Log   = view \"$l_SplitFile_YARN_LOG\"\n";
						# printf "\n"
						printf "${itab5}YARN BKG Shell Job:	ps -aef |grep -E \"^[0-9]*\s*$l_BKG_PID \"\n"
						#printf "${itab2}Main Shell Script:      ps -aef |grep -E \"^[a-zA-Z0-9]*\s*$G_THIS_JOB_PID \"  \n"
						printf "${itab6}1) To abort this YARN process:	yarn  application -kill $l_YarnAppID\n"
						printf "${itab6}2) To abort Shell BKG process:	kill -9 $l_BKG_PID\t\t"
						printf "(The \"yarn -kill\" command MIGHT have already terminated this BKG shell process, so this step might not be necessary!)\n\n"
						continue;
					fi;	#--}

					FLAG_STATUS="???";
					if [[   -f $l_success_Flag ]] && [[ ! -f $l_failed_Flag ]]; then FLAG_STATUS="SUCCESS flag found";			fi;
					if [[ ! -f $l_success_Flag ]] && [[   -f $l_failed_Flag ]]; then FLAG_STATUS="FAILED flag found";			fi;
					if [[ ! -f $l_success_Flag ]] && [[ ! -f $l_failed_Flag ]]; then FLAG_STATUS="Both SUCCESS and FAILED Flags NOT FOUND";   fi;
					if [[   -f $l_success_Flag ]] && [[   -f $l_failed_Flag ]]; then FLAG_STATUS="Both SUCCESS and FAILED Flags FOUND";		fi; #Should never happen


					# Get Count of ps-aef|grep this PID.  
					#	If a process still running                   ==> It must have COUNT=1
					#	If a process stops (natually or terminated)  ==> It must have COUNT=0.  It might have a SUCCESS flag
					l_BKG_Process_Count=$(ps -aef |grep -E "^[a-zA-Z0-9]*\s*$l_BKG_PID "  |grep -v -w grep|wc -l); 
					if [[ $l_BKG_Process_Count -eq 0 ]]
					then	
						RUNNING_STATUS="COMPLETED";     	# COMPLETE=Job could Either be: SUCCESS (has SUCCESS flag) or FAILED (has FAILED flag)
						l_TotalJobsCompleted=$((l_TotalJobsCompleted+1));	# Completed is when the pS count=0,  which could mean SUCCESS or FAILED

						#l_yarnlog=$(GetYARNLog   "$submitMaster"    "$l_INVOKE_SPARK_LOG"   "$l_SplitFile_YARN_LOG"   "$itab1");
						GetYARNLog   "$submitMaster"    "$l_INVOKE_SPARK_LOG"   "$l_SplitFile_YARN_LOG" "$l_YARN_LOG_DIR" "$itab1"
						printf "${itab4}#$N) BKGPID #%6d: %9s;  BKG_Count=%s;  %s;   YarnAppID=%s\n"     "$l_BKG_PID" "$RUNNING_STATUS"   "$l_BKG_Process_Count"   "$FLAG_STATUS"  "$l_YarnAppID" 
						printf "${itab5}YARN log = %s  \n\n"     "$G_YARN_LOGFILE"

							#Each SplitFile is a BKG process; hence each SplitFile has its own YARN log
						l_yarnlog=$(echo $l_yarnlog | sed -e "s: *view::");	#Remove the word view and spaces so there is only filename on line
						printf "$l_yarnlog\n">> $G_ALL_YARN_LOGS;			#Keep accumulated list of all Yarn logs
					elif [[ $l_BKG_Process_Count -eq 1 ]]
					then
						RUNNING_STATUS="STILL RUNNING";	
						# There are NOT that many SplitFiles per PARA_GROUP, so this printf won't take up much space
						printf "${itab4}#$N) BKGPID #%6d: %13s; (%s);   To check:  ps -aef | grep -E \"^[a-zA-Z0-9]*\s*$l_BKG_PID\";   YarnAppID=%s;\n"      "$l_BKG_PID"  "$RUNNING_STATUS" "$FLAG_STATUS"  "$l_YarnAppID"
					else
						RUNNING_STATUS="**UNKNOWN**";
						printf "    ${itab5}#$N) BKGPID #%6d: %11s; (%s);   To check:  ps -aef | grep -E \"^[a-zA-Z0-9]*\s*$l_BKG_PID\";   YarnAppID=%s;\n"  "$l_BKG_PID"  "$RUNNING_STATUS"   "$FLAG_STATUS"  "$l_YarnAppID"
					fi;
				done;	#--}  for loop

				printf "${itab3}Total SplitFiles Completed so far for this Group #$l_PARA_GROUP: $l_TotalJobsCompleted\n";
				if [[ $l_TotalJobsCompleted -eq $l_PARA_SPLITFILES_COUNT_PER_GROUP ]] 
				then
					printf "${itab3}No more running jobs to wait for ... TotalJobsCompleted=$l_TotalJobsCompleted; PARA_SPLITFILES_COUNT_PER_GROUP=$l_PARA_SPLITFILES_COUNT_PER_GROUP\t(These 2 counts should equal)\n\n";
				else
					printf "${itab3}Sleeping for $G_SLEEP_SECS_FOR_SPARK_JOB secs ... \n";
					sleep $G_SLEEP_SECS_FOR_SPARK_JOB
				fi;

				l_endTime="$(date +"%Y%m%d %H:%M:%S")";
				l_endTime_EPOC="$(date +%s)"; 
				l_secondsElapsed=$((l_endTime_EPOC - l_MainStartTime_EPOC)); 

				#printf "${itab3}Total Seconds Elapsed so far: $l_secondsElapsed;\t\tTotal SplitFiles Completed so far for this PARA Group #$l_PARA_GROUP: $l_TotalJobsCompleted\n\n";
			done;	#--}	while loop: Wait for ALL bkg process to complete, but only  up till  MaxWaitTime


			# Now, Auto-Terminate all BKG processes that do not have SUCCESS Flag (because by now, either all have completed, or some have not and exceeded MaxWaitTime:
			l_JOBS_STILL_RUNNING=$((l_PARA_SPLITFILES_COUNT_PER_GROUP-l_TotalJobsCompleted));
			printf "\n\n${itab3}===== Here, WAIT LOOP completes (PARA_GROUP=$l_PARA_GROUP), due to one or both of these 2 conditions:\n" 
			printf "${itab4}1) BKG jobs completed: ${l_TotalJobsCompleted} out of ${l_PARA_SPLITFILES_COUNT_PER_GROUP} total;\t\tJobs Still running = $l_JOBS_STILL_RUNNING\n"
			printf "${itab4}2) SecondsElapsed=$l_secondsElapsed;  MaxWaitTime=$G_SECS_TO_WAIT_FOR_SPARK_JOB_BEFORE_ABORT\n" 


			printf "\n\n${itab2}===== PART 3/PARA Group #$l_PARA_GROUP:  Terminate ANY STILL-RUNNING jobs as they have now exceeded MaxWaitTime: Elapsed Time=$l_secondsElapsed; MaxWaitTime=$G_SECS_TO_WAIT_FOR_SPARK_JOB_BEFORE_ABORT\n"
			if [[ $l_JOBS_STILL_RUNNING -le 0 ]]
			then	#--{
				printf "\n${itab3}There are NO jobs still running to terminate (JOBS_STILL_RUNNING=$l_JOBS_STILL_RUNNING).  All have completed!\n\n"
			else	#--}{
				let N=0
				for line in $(grep -v "^#" $l_BKG_PROCESSES_LIST)
				do	#--{
					N=$((N+1));
					l_BKG_PID=$(           echo $line | cut -d"," -f 1);
					l_success_Flag=$(      echo $line | cut -d"," -f 3);
					l_failed_Flag=$(       echo $line | cut -d"," -f 4);
					l_SplitFile_YARN_LOG=$(echo $line | cut -d"," -f 5);
					l_INVOKE_SPARK_LOG=$(  echo $line | cut -d"," -f 6);
					l_YARN_LOG_DIR=$(      echo $line | cut -d"," -f 7);


					if [[   -f $l_success_Flag ]] && [[ ! -f $l_failed_Flag ]]; then FLAG_STATUS="SUCCESS flag found, FAILED flag NOT found"; fi;
					if [[ ! -f $l_success_Flag ]] && [[   -f $l_failed_Flag ]]; then FLAG_STATUS="SUCCESS flag NOT found, FAILED flag found"; fi;
					if [[ ! -f $l_success_Flag ]] && [[ ! -f $l_failed_Flag ]]; then FLAG_STATUS="Both SUCCESS and FAILED Flags NOT FOUND";   fi;
					if [[   -f $l_success_Flag ]] && [[   -f $l_failed_Flag ]]; then FLAG_STATUS="Both SUCCESS and FAILED Flags FOUND";		  fi; #Should never happen


					l_BKG_Process_Count=$(ps -aef |grep -E "^[a-zA-Z0-9]*\s*$l_BKG_PID "  |grep -v -w grep|wc -l); 
					#printf "\n${itab3}#$N) BKGPID %6d;  BKG_Process_Count=%d;   $FLAG_STATUS\n" $l_BKG_PID  $l_BKG_Process_Count

					printf "\n${itab3}#$N)\tBKGPID       = $l_BKG_PID\n"
					printf "${itab4}BKG_Count    = $l_BKG_Process_Count\n" 
					printf "${itab4}Flags Status = $FLAG_STATUS\n\n" 

					# Second chance:  Some SplitFiles near the bottom of the PARA GROUP could have completed by the time it gets to here
					# So just accept them as good.  There is no bkg id (completed) to terminate anyway
					if [[ -f $l_success_Flag ]] || [[ -f $l_failed_Flag ]]
					then	#--{
						# If we have SUCCESS flag:	The job completed successfully.
						#							We should have the YARN log
						# If we have FAIL    flag:	The job completed may have terminated on its own OR was terminated manually by some user. 
						#							Which means we might NOT have the YARN log

						#At this time, this BKGPID might have already been displayed in the PART A (Success) arleady.
						# SO just repeat it here, no harm

						printf "\n${itab5}This means this BKG process completed by this time, its YARN log would have generated, (hence nothing to terminate)\n" $l_BKG_PID

						#l_yarnlog=$(GetYARNLog   "$submitMaster"    "$l_INVOKE_SPARK_LOG"   "$l_SplitFile_YARN_LOG"   "$itab1");
						GetYARNLog   "$submitMaster"    "$l_INVOKE_SPARK_LOG"   "$l_SplitFile_YARN_LOG"  "$l_YARN_LOG_DIR"  "$itab1"

						printf "    ${itab1}SplitFile #$N of PARA_GROUP #%d)  BKGPID #%6d: %10s; %24s; YARN log=%s\n" $l_PARA_GROUP   "$l_BKG_PID"  "$RUNNING_STATUS" "$FLAG_STATUS"   "$G_YARN_LOGFILE"

					else	#--}{
	
						#Here MaxWaitTime is exceeded and NO Success or Fail flag ==> Need to Terminate YARN app
						# If FAIL flag exists
						if [[ $l_BKG_Process_Count -ge 1 ]]
						then
							printf "${itab4}This BKG process STILL RUNNING, but no SUCCESS/FAIL flag found, and its ElapsedTime (%d secs) has EXCEEDED MaxWaitTime (%d secs)\n" "$l_secondsElapsed"  "$G_SECS_TO_WAIT_FOR_SPARK_JOB_BEFORE_ABORT"
						else
							printf "${itab4}This BKG process COMPLETED, but no SUCCESS/FAILED flag found (that means it was MANUALLY Terminated by some user), and its ElapsedTime (%d secs) has EXCEEDED MaxWaitTime (%d secs)\n" "$l_secondsElapsed"  "$G_SECS_TO_WAIT_FOR_SPARK_JOB_BEFORE_ABORT"
						fi;

						printf "${itab4}Its 2 processes will now be terminated:\n"
						printf "${itab4}Step 1/2: Terminate YARN app:\t\tYarnAppID=\"$l_YarnAppID\";   yarn  application -kill  $l_YarnAppID\n"
	
						if [[ "$submitMaster" == "YARN" ]] || [[ "$submitMaster" == "yarn" ]]	
						then	#--{
							if [[ "$l_YarnAppID" == "NOT_ON_YARN_QUEUE_YET" ]]
							then #--{
								printf "${itab6}========== Re-Extracting Yarn_Application_ID again as it currently is: \"$l_YarnAppID\"\n"
								# Get ApplicationID again, as the job might get on the YARN queue at this time:
								if [[ -f $l_SplitFile_YARN_LOG ]]
								then
									l_YarnAppID=$(grep "INFO Client: Submitting application .* to ResourceManager" $l_SplitFile_YARN_LOG|head -1|awk 'END{print $7}');
								fi;
								l_YarnAppID="${l_YarnAppID:-"NOT_ON_YARN_QUEUE_YET"}"; 	# DO NOT CHANGE THIS TOKEN as other IF stmnts uses it
								printf "${itab6}========== New Value:  \"$l_YarnAppID\"\n"
							fi; #--}
		
							if [[ "$l_YarnAppID" == "NOT_ON_YARN_QUEUE_YET" ]]
							then	#--{
								printf "${itab6}Cannot terminate this YARN app as its YARN_APP_ID still is = \"$l_YarnAppID\"\n\n"
							else	#--}{
								printf "${itab5}A. Try to generate YARN Log if possible by Calling ExtractSTDOUTfromYARNlogs() before terminating YARN app\n"

									# Normally, func_invoke_PySpark() calls ExtractSTDOUTfromYARNlogs() to generate YARN log
									# But at this step, we are terminating func_invoke_PySpark(), which means it might or might not have called ExtractSTDOUTfromYARNlogs() yet.
									# So we just call it here just in case.  Does not matter if it executes multiple times
								ExtractSTDOUTfromYARNlogs   "$G_SUBMIT_MASTER"   $l_SplitFile_YARN_LOG   $l_YARN_LOG_DIR   "$l_INVOKE_SPARK_LOG"  "$itab5"

								#l_yarnlog=$(GetYARNLog   "$submitMaster"      "$l_INVOKE_SPARK_LOG"   "$l_SplitFile_YARN_LOG"  "$itab1");
								GetYARNLog   "$submitMaster"      "$l_INVOKE_SPARK_LOG"   "$l_SplitFile_YARN_LOG" "$l_YARN_LOG_DIR"   "$itab1"
								printf "\n${itab6}YARN log: $G_YARN_LOGFILE\n"
	
								printf "\n${itab6}For review:\n"
								printf "${itab7}PARA_GROUP:		$l_PARA_GROUP\n"
								printf "${itab7}YARN Application ID:	$l_YarnAppID\n"
								printf "${itab7}YARN_LOG_DIR:		$l_YARN_LOG_DIR\n"
								printf "${itab7}YARN Queue:		view $l_SplitFile_YARN_LOG\n"
								printf "${itab7}INVOKE_SPARK_LOG:	view $l_INVOKE_SPARK_LOG\n\n"
								printf "${itab7}You can also do either of these 2 steps to find the Error:\n"
								printf "${itab7}1)     Use YARN UI to display the 2 logs (stdout and stderr)  for this ApplicationID \"$l_YarnAppID\":\n"
								printf "${itab7}2) Or, issue this command as Linux (might be able to find the same Yarn log, depending on the state of the BKG process)\n\n" 
								printf "${itab8}grep -i -w -l \"Python Job starts at\"  ${l_YARN_LOG_DIR}/*/*\n\n"
								printf "${itab7}This command returns the name of the file containing actual Python logs\n"
								printf "${itab7}Edit this file to see actual content\n\n"

								printf "${itab5}B. Terminate BKG YARN process:     yarn  application -kill  $l_YarnAppID\n\n"
								yarn  application -kill $l_YarnAppID
								RC=$?
								if [[ $RC -ne 0 ]]
								then 
									printf "\n\n\n********** ERROR: Cannot kill YARN app: $l_YarnAppID (it might have been MANUALY killed);  RC=$RC;\n\n"
								fi;
								sleep 15; 	# Sleep a little as YARN also displays on STDERR some error message, which interferes with this job's output
								printf "\n\n"
							fi;	#--}
	
						else	#--}{
							printf "${itab6}No YARN process to terminate due to executing in LOCAL mode\n\n"
						fi;	#--}
	
						printf "${itab4}Step 2/2) Terminate BKG Shell script:  ${l_BKG_PID}\t\t(NOTE: this BKG process MIGHT have completed on its own (SUCCESS or FAILED within MaxWaitTime))\n"
						printf "${itab5}A) Check this PID from OS again: \"ps -aef\" for verification: ps -aef |grep -E \"^[a-zA-Z0-9]*\s*$l_BKG_PID \"\n" 
	
						FLAG_STATUS="Both S/F Flags NOT FOUND";
						if [[   -f $l_success_Flag ]] && [[ ! -f $l_failed_Flag ]]; then FLAG_STATUS="SUCCESS flag found, FAILED flag NOT found"; fi;
						if [[ ! -f $l_success_Flag ]] && [[   -f $l_failed_Flag ]]; then FLAG_STATUS="SUCCESS flag NOT found, FAILED flag found"; fi;
						if [[ ! -f $l_success_Flag ]] && [[ ! -f $l_failed_Flag ]]; then FLAG_STATUS="Both S/F Flags NOT FOUND";                  fi;
	
						l_BKG_Process_Count=$(ps -aef |grep -E "^[a-zA-Z0-9]*\s*$l_BKG_PID "  |grep -v -w grep|wc -l); 
							#ps -aef |grep -E "^[a-zA-Z0-9]*\s*$l_BKG_PID "  |grep -v -w grep | sed "s:^:${itab7}:"
						printf "${itab6}BKG_Count   = ${l_BKG_Process_Count}\n"
						printf "${itab6}FLAG_STATUS = \"${FLAG_STATUS}\"\n"
	
						if [[ $l_BKG_Process_Count -ge 1 ]]
						then
							printf "${itab5}B) Terminate this PID : kill -9 $l_BKG_PID\n"
							kill -9 $l_BKG_PID
							RC=$?
							printf "${itab7}RC=$RC\n"
							if [[ $RC -ne 0 ]]
							then 
								printf "\n\n\n${itab6}********** ERROR: Cannot kill BKG_PID $l_BKG_PID;  RC=$RC;\n\n"; 
								printf "${itab6}This could be due to: someone MANUALLY killed the YARN process, or the Auto-Terminate feature\n"
								printf "${itab6}That Yarn -kill will also terminate this shell BKG process ($l_BKG_PID) immediately\n\n\n"
								printf "${itab6}Anyway it's OK as this BKG ($l_BKG_PID) has stopped running (as expected to follow the yarn kill)\n\n\n"
							fi;
						else
							printf "${itab7}BKG_PID $l_BKG_PID already completed (BKG_Process_Count=$l_BKG_Process_Count) ==>  No process PID to terminate\n"; 
						fi;
					fi;	#--}
					printf "\n"
				done;	#--}  for loop: Terminate BKG process of job that do NOT have SUCCESS flag

				sleep 10;	#Sleep for a little bit so that all messages from terminated process print out, so not to intermingling with the output of this shell

				printf "\n${itab2}===== Part3 Completed for all BKG process of this PARA_GROUP: $l_PARA_GROUP\n"
				printf "${itab2}===== RemainingSplitFiles = $remainingSplitFiles files"
				if [[ $remainingSplitFiles -gt 0 ]]
				then printf "\t==> Back to set up next PARA_GROUP\n\n"; 
				else printf "\t==> All SplitFiles have completed processing\n\n"; fi;
			fi;	#--}

			printf "\n${itab2}Check how many SUCCESS Flags (1 per SplitFile) so far (of all PARA_GROUPs up till now):\n"
			let N=0
			for line in $(grep -v "^#" $l_BKG_PROCESSES_LIST)
			do	#--{
				N=$((N+1));
				l_BKG_PID=$(           echo $line | cut -d"," -f 1);
				l_success_Flag=$(      echo $line | cut -d"," -f 3);
				l_failed_Flag=$(       echo $line | cut -d"," -f 4);
				if [[ -f $l_success_Flag ]]
				then total_SUCCESS_SplitFiles=$((total_SUCCESS_SplitFiles + 1 ));
				else 
					total_FAIL_SplitFiles=$((   total_FAIL_SplitFiles    + 1 ));
						# A job can: 1) terminate with FAIL state, or 2) was manually terminated.
						# Either way, it will not have a SUCCESS flag.
						# By checking only for SUCCESS flag this total_FAIL_SplitFile represents both of these conditions
				fi;
			done;	#--}

			printf "${itab3}Total SplitFiles SUCCESS so far = $total_SUCCESS_SplitFiles
${itab3}Total SplitFiles FAIL    so far = $total_FAIL_SplitFiles\n\n"


			if [[ $remainingSplitFiles -gt 0 ]]
			then
				printf "\n${itab2}=====\n"
				printf "${itab2}===== Create new PARA_GROUP: $l_PARA_GROUP; remainingSplitFiles=$remainingSplitFiles\n"
				printf "${itab2}=====\n\n"

				l_PARA_GROUP=$((l_PARA_GROUP+1));
				let l_PARA_SPLITFILES_COUNT_PER_GROUP=0
				l_BKG_PROCESSES_LIST="${l_LOG_DIR}/BKG_PROCESSES___ParaGroup_${l_PARA_GROUP}.list"; # Contains list of BKG Processes info of ALL splitfiles
			fi;

		fi;	#--} #-- IF statement checks if this new SplitFile should belong to same PARA_GROUP or new PARA_GROUP
	done;	#--}  for loop

	printf "END LOOP;\n"

	printf "
============================================================================\t$(GetCurrTime)
SUMMARY:
	BATCH_ID:	$l_BATCH_ID
	SESSION_DATE:	$G_SESSION_DATE
	Input File:	$G_INPUT_LIST
		Total PARA Groups:	$l_PARA_GROUP
		Total Split Files Submitted	= $totalSplitFiles	files
			SplitFiles SUCCESS	= $total_SUCCESS_SplitFiles	files
			SplitFiles FAIL		= $total_FAIL_SplitFiles	files
============================================================================\n"

	#Assign to Global variable so other function can access these variables
	export         G_TOTAL_SPLITFILES=$totalSplitFiles
	export G_TOTAL_SUCCESS_SPLITFILES=$total_SUCCESS_SplitFiles
	export    G_TOTAL_FAIL_SPLITFILES=$total_FAIL_SplitFiles

	if [[ $total_FAIL_SplitFiles -gt 0 ]]
	then
		printf  "\n${itab2}Total_FAIL_SplitFiles=$total_FAIL_SplitFiles  ==> Exiting -1\n\n"
		printf "${itab1}<== function Execute\t\t$(GetCurrTime)\n\n\n"
		exit -1
	fi;
	printf "${itab1}<== function Execute\t\t$(GetCurrTime)\n\n\n"
} #function Execute


function MoveHDFSFileToHDFSArchive
{
	local inputFile="$1";
	local hdfsDir="$2";
	local hdfsARCHIVEDir="$3";
	local sessionDate="$4";
	local itab1="$5";   if [[ -z "$itab1" ]]; then itab1=""; fi; local itab2="${itab1}\t"; local itab3="${itab2}\t";

	local log="$6";	#This is reverse  as it appears AFTER itab1
	local RC;

	if [[ -s $log ]]
	then
		printf "${itab1}==> function MoveHDFSFileToHDFSArchive\t\t\t$(GetCurrTime)
${itab2}inputFile	= $inputFile
${itab2}hdfsDir		= $hdfsDir
${itab2}hdfsARCHIVEDir	= $hdfsARCHIVEDir
${itab2}sessionDate	= $sessionDate\n\n" >> $log;
	else
		printf "${itab1}==> function MoveHDFSFileToHDFSArchive\t\t\t$(GetCurrTime)
${itab2}inputFile	= $inputFile
${itab2}hdfsDir		= $hdfsDir
${itab2}hdfsARCHIVEDir	= $hdfsARCHIVEDir
${itab2}sessionDate	= $sessionDate\n\n";
	fi;

	local dt=$(date +%Y%m%d_%H%M%S);
	local bn=$(basename $inputFile);			# Keep entire filename, including extension
	local archiveFileName="${bn}___${sessionDate}.txt";	# Add date to filename, so it will be distinct in ARCHIVE_DIR
								# as the same splitfile might be repeated many times
	local hdfsInputFile="${hdfsDir}/${inputFile}"
	local hdfsArchiveFile="${hdfsARCHIVEDir}/${archiveFileName}"

	if [[ -s $log ]]
	then 
		printf "${itab2}Move:\t$hdfsInputFile\n${itab2}to:\t$hdfsArchiveFile:\n" >> $log
		hadoop fs -mv	$hdfsInputFile  $hdfsArchiveFile >>$log 2>&1
		RC=$?
		printf "${itab2}RC=$RC\n" >> $log; 
	else 
		printf "${itab2}Move:\t$hdfsInputFile\n${itab2}to:\t$hdfsArchiveFile:\n"
		hadoop fs -mv	$hdfsInputFile  $hdfsArchiveFile
		RC=$?
		printf "${itab2}RC=$RC\n"; 
	fi;

	if [[ $RC -ne 0 ]]
	then
		if [[ -s $log ]]
		then	printf "\n\n\n***** ERROR: Cannot move input HDFS file ($bn) to Archive ($archiveFileName); RC=$RC\n\n\n" >> $log;
			printf "${itab1}<== function MoveHDFSFileToHDFSArchive\n" >> $log;
		else	printf "\n\n\n***** ERROR: Cannot move input HDFS file ($bn) to Archive ($archiveFileName); RC=$RC\n\n\n"
			printf "${itab1}<== function MoveHDFSFileToHDFSArchive\n";
		fi;
		exit -1;
	fi;
	if [[ -s $log ]]
	then printf "${itab1}<== function MoveHDFSFileToHDFSArchive\t\t$(GetCurrTime)\n\n" >> $log;
	else printf "${itab1}<== function MoveHDFSFileToHDFSArchive\t\t$(GetCurrTime)\n\n";
	fi;
} #function MoveHDFSFileToHDFSArchive


function ConvertGenericSplitFile_To_AppSplitFile    
{
	local l_PROGRAM_TYPE="$1"
	local l_fromDir="$2";
	local l_destDir="$3";
	local l_INPUT_LIST_PER_PROGRAM_TYPE="$4";
	local l_logDir="$5";

	local itab1="$6";   if [[ -z "$itab1" ]]; then itab1=""; fi; local itab2="${itab1}\t"; local itab3="${itab2}\t"; local itab4="${itab3}\t";
	local N  K  bn  l_destSplitFile   l_schemaName   l_tableName
	local l_totalFiles=0;
	local l_totalLines=0;


	printf "\n${itab1}==> function ConvertGenericSplitFile_To_AppSplitFile;\t\t\t$(GetCurrTime)\n"
	printf "${itab2}(AppSplitFile =  Specific File for BSTAT, or DC, ...)"
	printf "${itab2}(in other words, create files that are specific to the different applications:  BSTAT or DC)"
	printf "${itab2}l_PROGRAM_TYPE	= $l_PROGRAM_TYPE\n"
	printf "${itab2}l_fromDir	= $l_fromDir\n"
	printf "${itab2}l_destDir	= $l_destDir\n"
	printf "${itab2}l_INPUT_LIST_PER_PROGRAM_TYPE	= $l_INPUT_LIST_PER_PROGRAM_TYPE;\n"
	printf "${itab2}l_logDir	= $l_logDir\n\n"

	let N=0;
	for splitFile in $(ls ${l_fromDir}/*.txt)		# SplitFile contains absolute path
	do	#--{
		N=$((N+1));
		if [[ ! -s $splitFile ]]; then	printf "\n\n\nERROR:  Input SplitFile cannot be zero-size. \"$splitFile\" "; exit -1; fi;

		bn=$(basename $splitFile);
		l_destSplitFile="${l_destDir}/${bn}"
		rm $l_destSplitFile >/dev/null 2>&1

		printf "${itab2}SplitFile #$N) $splitFile;\n${itab3}Save in this OutputFile: \"$l_destSplitFile\"\n\n"

		l_totalLines=$(wc -l $splitFile | cut -d " " -f1)
		printf "${itab3}Sample 2 lines of splitFile:  out of TotalLines=$l_totalLines)\n"
		head -2 $splitFile |cat -v
		printf "\n"

		l_totalLines=$(wc -l $l_INPUT_LIST_PER_PROGRAM_TYPE | cut -d " " -f1)
		printf "${itab3}Sample 2 lines of file INPUT_LIST_PER_PROGRAM_TYPE;   out of (TotalLines=$l_totalLines);  ($l_INPUT_LIST_PER_PROGRAM_TYPE)\n"
		head -2 $l_INPUT_LIST_PER_PROGRAM_TYPE |cat -v
		printf "\n"

		printf "${itab3}Create NEW SplitFile by Extracting Contents of INPUT_LIST_PER_PROGRAM_TYPE:\n"
		l_totalFiles=$((l_totalFiles+1));
		case "$l_PROGRAM_TYPE" in
		"BSTAT")	
			let K=0;
			for line in $(sed "s:\t:.:" $splitFile)
			do

				#PURPOSE: make each SplitFile has same content as file l_INPUT_LIST_PER_PROGRAM_TYPE, seperate by  TAB
				#         l_INPUT_LIST_PER_PROGRAM_TYPE fields are separated by: ^\
				#         SplitFile                     fields are separated by: TAB
				# Unlike DC, this BSTAT output file is using TAB as the delimiter between fields
				# (because it does not have RegExp in the field)
				#
				# Now SplitFile has additional column at the end: FileSize (Could be 0 or more bytes)

				K=$((K+1));
				l_schemaName=$(echo $line | cut -d"." -f1);
				l_tableName=$( echo $line | cut -d"." -f2| cut -d"," -f1);
				l_fileSize=$(  echo $line | cut -d"." -f2| cut -d"," -f2);
				printf "${itab4}Line $K) \"$line\";\tl_schemaName=\"$l_schemaName\"; l_tableName=\"$l_tableName\";   l_fileSize=\"$l_fileSize\"\n"

				printf "\n${itab5}===== Check 1/3:    Grep returns for this schemaName=\"$l_schemaName\"\tin file l_INPUT_LIST_PER_PROGRAM_TYPE\t==>  Should find at least 1 row (due to possible same Schema for multiple tables)\n";
				printf "${itab6}===== Only display top 2\n";
				grep -i -w "$l_schemaName" $l_INPUT_LIST_PER_PROGRAM_TYPE  | sed  "s:^:${itab6}:" | cat -v | head -2| nl
				
				printf "${itab5}===== Check 2/3:    Grep returns for this tableName=\"$l_tableName\" (from above Schema)\t\t\t\t==> Should find 1\n";
					grep -i -w "$l_schemaName" $l_INPUT_LIST_PER_PROGRAM_TYPE |grep -i -w "$l_tableName"  | sed  "s:^:${itab6}:" | cat -v |nl
			
				l_COUNT=$(	grep -i -w "$l_schemaName" $l_INPUT_LIST_PER_PROGRAM_TYPE |grep -i -w "$l_tableName" |\
						awk -F"$G_INPUT_FIELD_DELIMITER" '{printf ("%s%s%s%s%s\n", $1,"\t",$2,"\t",$3)}'  | wc -l |cut -d" " -f1)

				printf "${itab5}===== Check 3/3:    Grep/Awk returns for this Schema/TableName=\"$l_tableName\"\t\t\t\t==> Count=$l_COUNT\t\tShould find 1\n";
				grep -i -w "$l_schemaName" $l_INPUT_LIST_PER_PROGRAM_TYPE |grep -i -w "$l_tableName" |\
				awk -F"$G_INPUT_FIELD_DELIMITER" '{printf ("%s%s%s%s%s\n", $1,"\t",$2,"\t",$3)}'  | sed  "s:^:${itab6}:" | cat -v |nl

				if [[ $l_COUNT -lt 1 ]]
				then
					printf "\n\n\nWARNING:   This count should NOT be  0.  This means it could not find Schema/Table in the INPUT FILE.\n\n\n"
				else
					grep -i -w "$l_schemaName" $l_INPUT_LIST_PER_PROGRAM_TYPE |grep -i -w "$l_tableName" |\
					awk -F"$G_INPUT_FIELD_DELIMITER" -v FileSize=$l_fileSize '{printf ("%s%s%s%s%s%s%s\n", $1,"\t",$2,"\t",$3, "\t",FileSize)}' >> $l_destSplitFile;
							# Pass FileSize to Python program also, so it can make intelligent choices quickly

					#awk -F"$G_INPUT_FIELD_DELIMITER" '{printf ("%s%s%s%s%s\n", $1,"\t",$2,"\t",$3)}' >> $l_destSplitFile;
						#Unlike DC, this BSTAT output file is using TAB as the delimiter between fields
						#(because it does not have RegExp in the field)

					printf "\n\n${itab3}Sample content of output File;  ($l_destSplitFile):\n"
					head -20 $l_destSplitFile |cat -v|nl
					printf "\n\n"

				fi;
				printf "\n"
			done;
			rm $splitFile;;
		"DC")	
			let K=0;
			for line in $(sed "s:\t:.:" $splitFile)
			do
				K=$((K+1));
#### l_tableName=$( echo $line | cut -d"." -f2)
#### printf "${itab4}Line $K) \"$line\";\tl_schemaName=\"$l_schemaName\"; l_tableName=\"$l_tableName\"\n"

				l_schemaName=$(echo $line | cut -d"." -f1)
				l_tableName=$( echo $line | cut -d"." -f2| cut -d"," -f1);
				l_fileSize=$(  echo $line | cut -d"." -f2| cut -d"," -f2);
				printf "${itab4}Line $K) \"$line\";\tl_schemaName=\"$l_schemaName\"; l_tableName=\"$l_tableName\";   l_fileSize=\"$l_fileSize\"\n"

				printf "\n${itab5}===== Check 1/2:    Grep returns for this schemaName=\"$l_schemaName\" in file l_INPUT_LIST_PER_PROGRAM_TYPE\t\t==>  Should find at least 1 row (due to having same Schema)\n";
				# This is long line, so display at BeginOfLine, no itab
				grep -i -w "$l_schemaName" $l_INPUT_LIST_PER_PROGRAM_TYPE  | cat -v

				l_COUNT=$(	grep -i -w "$l_schemaName" $l_INPUT_LIST_PER_PROGRAM_TYPE |grep -i -w "$l_tableName" | wc -l |cut -d" " -f1);
				printf "${itab5}===== Check 2/2:    Grep returns for this tableName=\"$l_tableName\" (from above Schema)\t\t\t==> Count=$l_COUNT\t==> Should find 1\n";
				# This is long line, so display at BeginOfLine, no itab
				grep -i -w "$l_schemaName" $l_INPUT_LIST_PER_PROGRAM_TYPE |grep -i -w "$l_tableName"  | cat -v

				if [[ $l_COUNT -lt 1 ]]
				then
					printf "\n\n\nWARNING:   This count should NOT be  0.  This means it could not find Schema/Table in the INPUT FILE.\n\n\n"
				else
					grep -i -w "$l_schemaName" $l_INPUT_LIST_PER_PROGRAM_TYPE  |grep -i -w "$l_tableName" >> $l_destSplitFile;
				fi;

			done;

			if [[ ! -s $l_destSplitFile ]]
			then	printf "\n\n\nERROR:  Output File cannot be zero-size. \"$l_destSplitFile\" "
				printf "\n${itab1}<== function ConvertGenericSplitFile_To_AppSplitFile;\t\t\t$(GetCurrTime)\n"
				exit -1;
			fi;
			rm $splitFile;;
		esac;
 
		l_totalLines=$(wc -l $l_destSplitFile | cut -d " " -f1)
		printf "\n${itab3}Sample content of output File;  l_totalLines=$l_totalLines;  ($l_destSplitFile):\n"
		head -5 $l_destSplitFile |cat -v
		printf "\n\n\n"


	done;	#--}

	printf "${itab3}Total Files processed:  $l_totalFiles\n"
	printf "\n${itab3}====================================================================================================================\n"
	printf "${itab3}Content of fromDir: $l_fromDir\n"
	ls -ltr $l_fromDir | sed "s:^:${itab3}:";
	printf "${itab4}(Should be 0 file as every file should have move to DestDir) ($l_fromDir)\n"
	printf "${itab3}====================================================================================================================\n"
	printf "\n${itab3}Content of destDir: $l_destDir\n"
	ls -ltr $l_destDir | sed "s:^:${itab3}:";
	printf "\n${itab4}(Should have as many files as in previous STEP) ($l_fromDir)\n"
	printf "${itab3}====================================================================================================================\n"
	printf "\n${itab1}<== function ConvertGenericSplitFile_To_AppSplitFile;\t\t\t$(GetCurrTime)\n"
}



function SubSplitPerTableBatchSize
{
	#Reason for this Sub-Split:  Assume original input file has 200 tables
	# When calcualte filesize (for bucketing num_executors) they all belong to 10 executor bucket.
	# Hence, there would be only 1 split file  with 200 tables.
	# ==> We want to make this the number of tables per Splitfile to be small, to keep Spark memory requirement small
	#     (more tables require more dataframes ...)
	# Hence we want to SubSplit this Splitfile into multiple files, based on variable:    $tableBatchSize


	#NOTE: 2 batchsize:	- Table BatchSize:  Used in this schel script to keep small number tables in each SplitSize 
	#			- Column BatchSize: Used in Python to formulate SELECT statement

	local tableBatchSize="$1";	# This BatchSize is for INITIAL estimation how many tables SHOULD be in the file
					# that get submitted to Spark.  After it calculates the DIVISOR and apply MAX value,
					# the final table count would be equal or smaller than this batchsize
	local fromDir="$2";
	local destDir="$3";
	local logDir="$4";

	local itab1="$5";   if [[ -z "$itab1" ]]; then itab1=""; fi; local itab2="${itab1}\t"; local itab3="${itab2}\t"; local itab4="${itab3}\t";

	local currDir=$(pwd)
	local l_sessionDate        l_jobNum      l_num_executors  l_origInputFile   l_divisor
	local N    bn    l_lines   l_tempFile1   l_upperLimit     l_divisor         l_tablesCount    l_tempFileName
	local l_newNumExecutors    l_num_executors_PADDED         l_newTotalTables  l_newFileName

	printf "\n${itab1}==> function SubSplitPerTableBatchSize;\t\t\t$(GetCurrTime)\n"
	printf "${itab2}tableBatchSize	= $tableBatchSize\n"
	printf "${itab2}fromDir		= $fromDir\n"
	printf "${itab2}destDir		= $destDir\n\n"

	#Example: splitFile=/tmp/MDB/TEST/20180702_165630__j1__e10__t100__tbl_list1000.txt #cd $fromDir
	local N=0;
	for splitFile in $(ls ${fromDir}/*.txt)		# File contains absolute path
	do	#--{
		N=$((N+1))
		bn=$(basename $splitFile);
		l_tablesCount=$(wc -l $splitFile | cut -d" " -f1)
		printf "${itab2}File #$N: $bn;  TablesCount=$l_tablesCount;  BatchSize=$tableBatchSize\n"

		if [[ $l_tablesCount  -le $tableBatchSize ]]
		then
			printf "${itab3}TablesCounts -le tableBatchSize  ==> No subsplit this file.  Just move this file to Output Dir\n\n"
			mv $splitFile  $destDir
			continue;
		fi;

		###l_divisor=$((l_tablesCount/${tableBatchSize}+1));	# Add 1 to ensure that all files would be EQUAL or less thatn BATCHSIZE

		l_mod=$((l_tablesCount%$tableBatchSize));
		l_divisor=$((l_tablesCount/tableBatchSize));
		if [[ $l_mod -ne 0 ]]
		then
			l_divisor=$((l_divisor+1));	# Add 1 to ensure that all files would be EQUAL or less thatn BATCHSIZE
		fi;								# Withoud adding 1, for some combination a file will have more lines than BATCHSIZE
										# Ex 1: l_tablesCount=8; BatchSize=3;
										#       8/3 = 2
										#       lines=8/2 = 4 <== this number indicates maximun lines per subsplit file
										#       which is more than what BATCHSIZE says should be
										#
										# Ex 2: l_tablesCount=8; BatchSize=3;
										#       (8/3)+1 = 3
										#       lines=8/3 = 2 <== this number indicates maximun lines per subsplit file
										#       which is same as BATCHSIZE 


		if   [[ $l_divisor -lt 1 ]]
		then	l_divisor=1; 	
		elif [[ $l_divisor -gt 10 ]]
		then	l_divisor=10; 	# Only keep with 1..10 because another split file might be directly after this input SplitFile
					# (such as  executor_20 follows executor_10. That means we can only have max 10 sub-split files) 
		fi;

			#This Divisor determines how many subsplit files.
			# if Divisor=5  then there are  5 subsplit files
			# if Divisor=10 then there are 10 subsplit files
			# if Divisor=20 then there are 20 subsplit files
			# So by setting it to 10, there are 10 subsplit files: e010, e011, e012, ... e019


			#---------------------------------------------------------------------------------------------------------------
			# This current SubSplit approach is OK,
			# but a better approach is to have infinite number of subsplits like:
			#      e10a, e10b, ..., e10z, and    even   e10aa, a10ab ....
			# Then when calculate num_executors later on, just remove all alphabets, leaving numbers for Executors only
			# This current approach is ok, as we are now keeping these files small:  <=1000 tables per input file .
			# So, for worse case (all 1000 tables fall into 1 bucket), then with tableSize=100
			# this current logic would sublist into 10 files: e10 .. e19
			# So it would work fine.
			#---------------------------------------------------------------------------------------------------------------
		
		l_upperLimit=$((l_divisor-1))
		printf "${itab3}==> Divisor=$l_divisor; l_upperLimit=$l_upperLimit\n"
		if [[ $l_divisor -le 1 ]]
		then
			printf "${itab3}Since Divisor=1 ==> No need Further Subplist ==> Move file this Splitfile to $destDir:\n\n"
			mv $splitFile  $destDir
		else
			l_lines=$((l_tablesCount/l_divisor));  #Integer Division; This is the total lines for the new file
			l_tempFile1="${fromDir}/${bn}___withExecutors.temp";  #Must have different extension from .txt in above for loop
			printf "${itab3}1. Need to split this file into sub-files based on value of DIVISOR=$l_divisor;  l_lines=$l_lines\n"
			printf "${itab3}2. Create TEMP file:  Adding new executors 0..$l_upperLimit (=$l_divisor DIVISOR)  to begin of each line: $l_tempFile1\n"
				# Start with 0 because that's the 1st executor:  like 10, 20, 30 ....
			awk -v ul=$l_upperLimit 'BEGIN {i=0} {printf ("%d,%s\n",i,$0); if (i>= ul) {i=0} else {i++};}' $splitFile > $l_tempFile1

			### new: "${destDir}/${sessionDate}__j${G_JOB_NUM}__e${num_executors_PADDED}__t${totalTables}__${l_BATCH_ID}__${originalInputFile}";
			l_sessionDate=$(   echo $bn | sed -e "s:^\(.*\)__\(.*\)__\(.*\)__\(.*\)__\(.*\)__\(.*\)$:\1:");   
			l_jobNum=$(        echo $bn | sed -e "s:^\(.*\)__\(.*\)__\(.*\)__\(.*\)__\(.*\)__\(.*\)$:\2:"); 
			l_num_executors=$( echo $bn | sed -e "s:^\(.*\)__\(.*\)__\(.*\)__\(.*\)__\(.*\)__\(.*\)$:\3:"); 
			l_num_executors=$( echo $l_num_executors | sed -e "s:^e::" -e "s:^E::"  -e "s:^0*::"); 	#remove "e" and leading 0 digits
			l_batch_id=$(      echo $bn | sed -e "s:^\(.*\)__\(.*\)__\(.*\)__\(.*\)__\(.*\)__\(.*\)$:\5:"); 
			l_origInputFile=$( echo $bn | sed -e "s:^\(.*\)__\(.*\)__\(.*\)__\(.*\)__\(.*\)__\(.*\)$:\6:"); 

			printf "${itab3}3. Extract fields from filename:\n"
			printf "${itab4}l_sessionDate	= \"$l_sessionDate\"\n"
			printf "${itab4}l_jobNum	= \"$l_jobNum\"\n"
			printf "${itab4}l_num_executors	= \"$l_num_executors\"\t\t(After remove \"e\" and leading 0 digits)\n"
			printf "${itab4}l_batch_id	= \"$l_batch_id\"\n"
			printf "${itab4}l_origInputFile	= \"$l_origInputFile\"\n"

			printf "${itab3}4. Create $l_divisor new OUTPUT files (based on divisor=$l_divisor); Placing them at dir:  $destDir\n"
			for num in $(seq 0 $l_upperLimit)
			do	#--{
				l_tempFileName=${splitFile}__$$.temp
				#printf "l_tempFileName=$l_tempFileName\n"
				grep "^${num}," $l_tempFile1 | sed 's:^[^,]*,::' > $l_tempFileName
				l_newNumExecutors=$((l_num_executors + num))
				l_num_executors_PADDED=$(printf "%03d" $l_newNumExecutors);	
					#Pad up to 3 digits, just like input file
				l_newTotalTables=$(wc -l $l_tempFileName | cut -d" " -f1 );
				l_newFileName="${destDir}/${l_sessionDate}__${l_jobNum}__e${l_num_executors_PADDED}__t${l_newTotalTables}__${l_batch_id}__${l_origInputFile}";
				if [[ $l_newTotalTables -gt 0 ]]
				then
					mv $l_tempFileName  $l_newFileName;
					printf "${itab4}$num/$l_upperLimit) $l_newFileName\t\tSize=$l_newTotalTables tables\n";
				else
					printf "${itab4}File(s) has $(wc -l $l_tempFileName |cut -d" " -f1) tables:\n";
					ls -ltr $l_tempFileName;
					printf "\n${itab5}Remove this file #$num because it's 0-tables size ($l_tempFileName):\n\n";
					rm $l_tempFileName;
				fi;
			done;	#--}

			#Each line has 3 fields, like:
			#	0,DL_EDGE_BASE_PPM_40568_BASE_PPMBO_RPTBO1	CBA_HEADER_AUDIT
			#	1,DL_EDGE_BASE_PPM_40568_BASE_PPMBO_RPTBO1	KCRT_REQUESTS_CDC_CM_W
			#	2,DL_EDGE_BASE_PPM_40568_BASE_PPMBO_RPTBO1	RPT_FCT_RM_RES_DISTRIBUTION

			printf "\n\n${itab3}Sample content of output file: $l_tempFile1\n"
			head -5 $l_tempFile1  | sed "s:^:${itab4}:"

			printf "\n${itab3}5.Move l_tempFile1 and splitFile  files to $logDir:\n\n";
			mv  $l_tempFile1  $splitFile  $logDir;
		fi;
	done;	#--}
	printf "\n${itab2}FINAL Content of DESTDIR:  $destDir\n";
	ls -ltr $destDir | sed "s:^:${itab3}:";
	printf "\n${itab1}<== function SubSplitPerTableBatchSize;\t\t\t$(GetCurrTime)\n\n";
}


function GenerateSplitFilesInTORUNDir
{
	# TORUNScripts are scripts after split based on numExecutors.
	# Each output script will be submitted to YARN one after another
	local originalInputFile="$1";
	local tablesWithExecutors="$2";
	local distinctExecutors="$3";
	local logDir="$4";
	local fnStr="$5";
	local destDir="$6";		#This is a temp location.  not final yet
	local sessionDate="$7"
	local l_BATCH_ID="$8";		# Also avaible from G_BATCH_ID

	local itab1="$9";   if [[ -z "$itab1" ]]; then itab1=""; fi; local itab2="${itab1}\t"; 
	local itab3="${itab2}\t"; local itab4="${itab3}\t";
	local tablesWithSameExecutors="${logDir}/${fnStr}_tablesWithSameExecutors_$$.txt";
	local n    num_executorsPADDED   outFile;

	printf "\n${itab1}==> function GenerateSplitFilesInTORUNDir\t\t$(GetCurrTime)
${itab2}originalInputFile       = $originalInputFile
${itab2}tablesWithExecutors     = $tablesWithExecutors
${itab2}distinctExecutors       = $distinctExecutors
${itab2}tablesWithSameExecutors = $tablesWithSameExecutors
${itab2}logDir      = $logDir
${itab2}fnStr       = $fnStr
${itab2}destDir     = $destDir
${itab2}sessionDate = $sessionDate
\n";

	totalGroups=$(wc -l $distinctExecutors |  cut -d" " -f1);
	let n=0;
	for num_executors in $(cat $distinctExecutors)	#{
	do
		n=$((n+1))
		grep "^${num_executors},"  $tablesWithExecutors  | sort > $tablesWithSameExecutors
		totalTables=$(wc -l $tablesWithSameExecutors |cut -d" " -f1) 
		printf "\n${itab2}Group #${n}/${totalGroups}) Num-Executors = $num_executors\n"
		printf "\n${itab3}A) List of tables for this Group:\t\tTotalTables=$totalTables\n";		#Display for debug

		sed -e "s:^:${itab4}:" $tablesWithSameExecutors |nl

		num_executors_PADDED=$(printf "%03d" $num_executors);	
			# Pad up to 3 digits, leading 0, for numeric Sorting in next function:
			#	To sort according lowest to highest:  Start with lowest
			# 3 digits because of maximum Executors=200  (3 digits max=999)

		# outFile="${destDir}/${sessionDate}__e${num_executors_PADDED}__t${totalFiles}__b${batch_id}__${originalInputFile}";
		#	# 2 fields: 1) SessionDate:   to identify a group
		#	#           2) num_executors:
		if [[ -z "$G_JOB_NUM" ]]
		then outFile="${destDir}/${sessionDate}__e${num_executors_PADDED}__t${totalTables}__b${l_BATCH_ID}__${originalInputFile}";
		else outFile="${destDir}/${sessionDate}__j${G_JOB_NUM}__e${num_executors_PADDED}__t${totalTables}__b${l_BATCH_ID}__${originalInputFile}";
		fi;


		printf "\n${itab3}B) ReFormat output file: Add tab between SchemaName and TableName: \"SchemaName<tab>TableName\" (outFile=$outFile)\n"
		printf "\n${itab3}Sample content of input file: tablesWithSameExecutors $tablesWithSameExecutorstablesWithSameExecutors\n"
		head -3 $tablesWithSameExecutors 
		###### cut -d"," -f2 $tablesWithSameExecutors | sed "s:\.:\t:" > $outFile;	#
		cut -d"," -f2,3 $tablesWithSameExecutors | sed "s:\.:\t:" > $outFile;	#

		printf "\n${itab2}Sample content of output File:\n"
		head -5 $outFile|sed "s:^:${itab3}:"
		printf "\n\n";
	done;	#} 
	printf "\n";

	if [[ $n -eq 1 ]]
	then printf "\n\n${itab2}There is 1 script to submit to Spark:\n";
	else printf "\n\n${itab2}There are $n scripts to submit to Spark:\n";
	fi;
	DisplayDirectory $destDir    "PRIOR_APP"     ""    "${itab2}";

	#These files have 2 fields each:  SchemaName<TAB>TableName
	printf "${itab1}<== function GenerateSplitFilesInTORUNDir\t\t$(GetCurrTime)\n\n";
} #function GenerateSplitFilesInTORUNDir


#-------------------------------------------------------------------------------------------------------------------------
#---------------------------------------- MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN ----------------------------------------
#-------------------------------------------------------------------------------------------------------------------------
#Initialize:
export G_THIS_JOB_PID=$$
export G_MAIN_PROG_DIR="/data/commonScripts/util";			# 

		#DEBUG  DEBUG DEBUG
		#export G_MAIN_PROG_DIR="/home/tqzpftv/CHEVELLEL/SINGLETHREAD2"
		#export G_MAIN_PROG_DIR="/home/tqzpftv/CHEVELLEL/PARALLEL"

itab1="\t";
itab2="${itab1}\t";
itab3="${itab2}\t";
itab4="${itab3}\t";
itab5="${itab4}\t";
itab6="${itab5}\t";
itab7="${itab6}\t";

	# These commas will be removed later, so you can add it here for easy viewing
export G_CHUNK_SIZE="100,000,000";		# Default; 
export G_CHUNK_SIZE_MINIMUM="50,000,000";	#
export G_CHUNK_SIZE_MAXIMUM="100,000,000,000";	#
export G_CHUNK_SIZE_DEFAULT="50,000,000";	#  

#export G_BIG_TABLE_ROWS_TO_SWITCH=100,000,000;		# 100 mil;  Anytable having this many rows or bigger will use the BRANCH 2 in Python code
export G_BIG_TABLE_ROWS_TO_SWITCH=100,000,000,000;	# 100 bil;  Anytable having this many rows or bigger will use the BRANCH 2 in Python code
		# Set it this big so it doest NOT go into Approach 2
						# Override with -C


export G_BIG_TABLE_TABLECOLS_BATCH_SIZE="1";	# Number of TABLE Cols per loop for BIG TABLE only 
export G_BIG_TABLE_STATCOLS_BATCH_SIZE="8";	# Number of STAT  Cols per loop (usuall entire set=8 cols; but for very large table this will set upper limit)
						# (usuall entire set=8 cols; but for very large table this will set upper limit)
						# Override with -b

export G_DF_COLUMNS_BATCH_SIZE="200";		# Default; Override with -c
		# This variable solves the problem where table has too many columns (>250 columns) to send to Spark DataFrame
		# Each column has about 10 BASIC STAT columns, so for table having >= 250cols, then Spark DataFrame will have 2500 columns
		# which hit the limit of Spark DataFrame.  So we have to keep it less than this number when building SQL dynnamically in Python
		#
		# This column is actually useless, as the Python program will dynamically re-determine the actual
		# TableCol BatchSize as it has the actual Table RowsCount.  Here we only have FileSize, not RowsCount
		#

export G_ASMS_EXCLUDE_FILE="ASMS_exclude.txt";	# Default; Override with -a

export G_DEBUG="NO";							# Default; Override with -d
export G_MAIL_LIST="";							# Default; Override with -e
export G_HDFS_ROOT_DIR="/EDW/DSODB/OPS/CHEVELLE/HYPER_PROF_RSLT";	# Default; Override with -H;  No specify $ENV
		# This single HDFS RootDir is the root for these 4 directories:
		#	1. G_HDFS_INPUT_DIR		-- Currently do NOT have PROJECT built in; Should have
		#	2. G_HDFS_OUTPUT_DIR		-- Currently do NOT have PROJECT built in; Should have
		#	3. G_HDFS_ARCHIVE_DIR		-- Currently do NOT have PROJECT built in; Should have
		#	4. G_HDFS_CHECKPOINT_ROOT_DIR	-- Has PROJECT built in

export G_ROOT_PROGDIR="/data/commonScripts/util/CHEVELLE/STAT";	# Default; Override with -L
export G_MIN_NUM_EXECUTORS="10";				# Default; Override with -m
export G_NUM_EXECUTORS_DEFAULT="10";				# (no option to override)
export G_MAX_NUM_EXECUTORS="200";				# Default; Override with -M
export G_YARN_SPARK_NAME="CHEVELLE_STAT";				# Default; Override with -N; To build YARN SPARK NAME
export G_MUST_HAVE_HIVE_DATATYPES="YES";			# Default; Override with -o

export G_PROJECT_DIRNAME="CHEVELLE_STAT";			# Default; Override with -P  (Linux Root Dir for this input file): 

export G_PROGRAM_TYPE="BSTAT";
export G_PYTHON_PROGRAM="hyper_profile_desc_stats.py";		# Default; Override with -P

export G_RESET_NORUN="NO";					# Default; Override with -r
export G_RESET="NO";						# Default; Override with -R

export G_SUBMIT_MASTER="yarn";					# Default; Override with -l.  Just -l, not additional value
export G_DEPLOY_MODE="cluster";					# Default; 
	# Only allow 2 choices: 	YARN	Cluster
	#				LOCAL	Client

export G_INCLUDE_SKIP_DATA_IN_OUTPUT="YES";			# Default; Override with -I
export G_MAX_RETRY_THRESHOLD=3;					# Default; Override with -t

export G_NEW_SPARK_JOB_NAME="";	#Use to build YARN spark job name

	#TZ="US/Central" date	# Austin
	#TZ="UTC" date		# GMT
	#TZ="US/Pacific" date	# Cali
	#TZ=Asia/Bangkok date +"%FI%H:%M"
export                                  G_GMT_TS="$(date +%Y%m%d%H%M%S)";	# This is used for Python Column, to match previous Date format
export                                  G_GMT_TS="$(date --utc +%Y%m%d%H%M%SZ)"; # GMT time to match JSON Output column name "INS_GMT_TS"
export                                  G_GMT_TS="$(date --utc +%Y%m%d%H%M%S)";  # GMT time to match JSON Output column name "INS_GMT_TS"
export                 G_GMT_TS_FOR_MICROSERVICE="$(date +"%Y-%m-%d %H:%M:%S")";	#Same timestamp, but slightly different format
		                							# to send into MicroService calls
export                                 G_SRVR_NM="$(hostname)";
export                                       env="";
export                                       ENV="";
export                              G_JOB_STATUS="FAIL";	
export              G_JOB_STEPS_COMPLETED_SO_FAR="0";
export                            G_SESSION_DATE="$(date +%Y%m%d_%H%M%S)";
export                        G_SESSION_DATE_STR="$(date +"%Y/%m/%d %H:%M:%S")";
export                        G_THIS_SCRIPT_NAME="$(basename $0)";
export                                   G_FNSTR="CP";	# FNST=FileNameString;  CP=Chevelle Profiling;  Used to make filenames
export                              G_INPUT_LIST="";
export                          G_DAYS_TO_REMOVE=60;
export                   G_SPLIT_FILE_BATCH_SIZE="101";	# 100 files per split file .  Option -S
export                        G_TOTAL_SPLITFILES=0;
export                G_TOTAL_SUCCESS_SPLITFILES=0;
export                   G_TOTAL_FAIL_SPLITFILES=0;
export                G_SLEEP_SECS_FOR_SPARK_JOB=60;	# 1 min
export G_SECS_TO_WAIT_FOR_SPARK_JOB_BEFORE_ABORT=86401;	# 1 day (1 sec more)
export                                G_BATCH_ID="";		#Intialize to Empty.  It will be set in the GetInputFile function()
export                          G_BATCH_ID_COUNT=0;
export                   G_INPUT_FIELD_DELIMITER='\x1c';
export                G_TABLES_TO_GET_FROM_QUEUE="500";
export              p_G_TABLES_TO_GET_FROM_QUEUE="";
export           G_MAX_PARA_SPLITFILES_PER_GROUP=1;		#How many bkg processes to run in parallel (1 SplitFile per BKG process)
														#Set to 1 for DC, by default.  In BSTAT it will be set to a bigger number
														#Override with option -p
export           p_MAX_PARA_SPLITFILES_PER_GROUP_str="";	
export	G_YARN_LOGFILE="";
export      G_PRIORITY="";	# Priority for GetWorkUnit.  Normally , this string is "", which means use the default method (return lowest priority first)
							# User can specify rows belonging to a specific priority, usually some low value (like 10, 20, or 100).
							# This way it does not interfere with the default approaceh


trap "Func_Trap_INT"    INT  EXIT

printf "\n\n\n"
while getopts "dhIlora:b:C:e:g:H:N:L:p:P:S:t:T:w:W:Y:"  OPTION
do
        case $OPTION in
        a)                        G_ASMS_EXCLUDE_FILE="$OPTARG";;	# File containing list of ASMS to be ignored (NOT to be processed). 
																	# Useful when table is > 250 columns (as DataFrame has max around 2500 columns)
																	# Just filename, not including directory
        b)           G_BIG_TABLE_STATCOLS_BATCH_SIZE="$OPTARG";;	# Number of STATSCols per run (usuall entire set=8 cols; but for very large table	
																	# This variable will set upper limit)
																	# Only takes effect if "LARGE TABLE PROCESSEING (Approach 2)" is chosen in Python program
		C)                G_BIG_TABLE_ROWS_TO_SWITCH="$OPTARG";;	# Number of Rows to switch from Regular Processing to Large Table Processing
		c)                   G_DF_COLUMNS_BATCH_SIZE="$OPTARG";;	# 100;  batch TableColumns for Python Spark SQL to execute (1:10 ratio in DataFame)
        d)                                   G_DEBUG="YES";;		# YES/NO			Default=NO
        e)                               G_MAIL_LIST="$OPTARG";;	# List of email address		Default=nobody
        h)      Usage; exit;;						#
        H)                           p_HDFS_ROOT_DIR="$OPTARG";;	# Default:   /$ENV/EDW/DSODB/OPS/CHEVELLE/HYPER_PROF_RSLT/
        I)             G_INCLUDE_SKIP_DATA_IN_OUTPUT="NO";;			# Default=YES (include SKIP data in output (JSON/Oracle/HDFS) when rerun a script)
        l)                           G_SUBMIT_MASTER="local";;		# l=Local; Default: YARN.  (Used to be  "-s local", now:  "-l")
        L)                            G_ROOT_PROGDIR="$OPTARG";;	# Linux LogDir (for logging)	    Default="/data/commonScripts/util/CHEVELLE/STAT";
        N)              p_G_TABLES_TO_GET_FROM_QUEUE="$OPTARG";;	# Default: 200, or whatver the microservice returns
        o)                G_MUST_HAVE_HIVE_DATATYPES="NO";;			# Default =YES: meaning: MUST HAVE table definition in ROADMASTER file
									#      -o = NO: meaning: Do NOT check against RoadMaster.
									# This -o option should only be used for debugging (such as testing 
									# the batching mechanism of table with large number of columns, ...)
									# because we need to test against ROADMASTER to perform MIN/MAX correctly 
									# on the correct datatype

		g)       p_MAX_PARA_SPLITFILES_PER_GROUP_str="$OPTARG";;	# How many SplitFiles per PARALLEL Group.  Set to 1 to simulate SERIAL processing
        p)                                G_PRIORITY="$OPTARG";;	# Used as Directory Name;	Default="CHEVELLE_STAT" for G_PROJECT_TYPE=BSTAT
        P)                         p_PROJECT_DIRNAME="$OPTARG";;	# Used as Directory Name;	Default="CHEVELLE_STAT" for G_PROJECT_TYPE=BSTAT
        r)                             G_RESET_NORUN="YES";;		# 

#       R)                                   G_RESET="YES";;		# Clear out HDFS Checkpoint Dir;	Deafult=NO
									# No longer used due to the new logic using Oracle QUEUE table
									# Cannot specify RESET anymore.

        S)                   G_SPLIT_FILE_BATCH_SIZE="$OPTARG";;	# How many tables (lines) per SplitFile.  Default 101 tables
        t)                     G_MAX_RETRY_THRESHOLD="$OPTARG";;	# 
		T)                            G_PROGRAM_TYPE="$OPTARG";;	# BSTAT, DC;		Default=BSTAT  for BASIC_STAT
        w)                G_SLEEP_SECS_FOR_SPARK_JOB="$OPTARG";;	# 
        W) G_SECS_TO_WAIT_FOR_SPARK_JOB_BEFORE_ABORT="$OPTARG";;	# 
        Y)                         p_YARN_SPARK_NAME="$OPTARG";;	# YARN JobName;		Default=CHEVELLE_STAT
        esac;
done;
shift $((OPTIND - 1));


# Remove commas from numbers
export G_CHUNK_SIZE=$(              echo $G_CHUNK_SIZE               | sed 's:,::g');
export G_CHUNK_SIZE_MINIMUM=$(      echo $G_CHUNK_SIZE_MINIMUM       | sed 's:,::g');
export G_CHUNK_SIZE_MAXIMUM=$(      echo $G_CHUNK_SIZE_MAXIMUM       | sed 's:,::g');
export G_CHUNK_SIZE_DEFAULT=$(      echo $G_CHUNK_SIZE_DEFAULT       | sed 's:,::g');
export G_BIG_TABLE_ROWS_TO_SWITCH=$(echo $G_BIG_TABLE_ROWS_TO_SWITCH | sed 's:,::g');

export G_SUBMIT_MASTER=$(           echo $G_SUBMIT_MASTER            | tr A-Z a-z  );
export G_DEPLOY_MODE=$(             echo $G_DEPLOY_MODE              | tr A-Z a-z  );

export G_MS_GET_TABLEBYID_URL="";
export G_MS_GET_WORK_UNIT_URL="";
export G_MS_WRITE_1TABLE_TO_OUTPUT_URL="";
export G_MS_SAVE_BATCH_OUTPUT_URL="";
export G_MS_UPDATE_QUEUE_URL="";

ValidateInputParameters	$G_INPUT_LIST  "" ;

GetENVvariable;				#ex: DEV,TST,CRT,PROD

case "$G_PROGRAM_TYPE" in	#--{
	"BSTAT")	
		case "$ENV" in
			"DEV")	
				export	G_MS_AUTHORIZATION_KEY="Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJhY2N0VHlwZSI6InNydkFjY3QiLCJzdWIiOiJub21hZC1jb25maWctMjJjNzM1NmQtZmJhMy00OWU3LWEwZWUtOTU5YWQ5NWM0OTlmIiwianRpIjoiZjM3NmU3MWEtYTdjMC00MGNmLWFmNjEtMjhjMDMxM2QzNTIyIiwiaXNzIjoiZ2RhYXMuZ20iLCJuYmYiOjE1MzAzODA0OTIsImlhdCI6MTUzMDM4MDQ5MiwiZXhwIjoxNTkzNDUyNDkyLCJnbWlkIjoibm9tYWQtY29uZmlnLTIyYzczNTZkLWZiYTMtNDllNy1hMGVlLTk1OWFkOTVjNDk5ZiIsImdtaW4iOiJub21hZC1jb25maWctMjJjNzM1NmQtZmJhMy00OWU3LWEwZWUtOTU5YWQ5NWM0OTlmIiwicm9sZXMiOlsic2VydmljZSIsImNvbmZpZyJdfQ.-5Bkwe7Z6QSt6DuStpye0sYSgwEonBzy-UHXMCzpVcs"

				export          G_MS_GET_TABLEBYID_URL="https://edge-gateway.cp-epg2i.domain.com/roadmaster/tables";
				export          G_MS_GET_WORK_UNIT_URL="https://chevelle-micro-dev.cp-epg2i.domain.com/basicstats/que/getworkunit"
				export G_MS_WRITE_1TABLE_TO_OUTPUT_URL="https://chevelle-micro-dev.cp-epg2i.domain.com/basicstats/out/save";
				export      G_MS_SAVE_BATCH_OUTPUT_URL="https://chevelle-micro-dev.cp-epg2i.domain.com/basicstats/out/savebatch";
				export           G_MS_UPDATE_QUEUE_URL="https://chevelle-micro-dev.cp-epg2i.domain.com/basicstats/que/update";

				export           G_MS_AUTHENTICATE_URL="https://chevelle-messaging-dev.cp-epg2i.domain.com/authenticate";	# BSTAT to pass a message for each completed Table
				export               G_MS_OUTBOUND_URL="https://chevelle-messaging-dev.cp-epg2i.domain.com/events/basic_stats/outbound/updateQueue";	# BSTAT to pass a message for each completed Table
				export       G_MS_MESSAGING_SECRET_KEY="3YmCdvHBKX;UwXrn"
				;;
			"TST")	
				export	G_MS_AUTHORIZATION_KEY="Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJhY2N0VHlwZSI6InNydkFjY3QiLCJzdWIiOiJub21hZC1jb25maWctMjJjNzM1NmQtZmJhMy00OWU3LWEwZWUtOTU5YWQ5NWM0OTlmIiwianRpIjoiZjM3NmU3MWEtYTdjMC00MGNmLWFmNjEtMjhjMDMxM2QzNTIyIiwiaXNzIjoiZ2RhYXMuZ20iLCJuYmYiOjE1MzAzODA0OTIsImlhdCI6MTUzMDM4MDQ5MiwiZXhwIjoxNTkzNDUyNDkyLCJnbWlkIjoibm9tYWQtY29uZmlnLTIyYzczNTZkLWZiYTMtNDllNy1hMGVlLTk1OWFkOTVjNDk5ZiIsImdtaW4iOiJub21hZC1jb25maWctMjJjNzM1NmQtZmJhMy00OWU3LWEwZWUtOTU5YWQ5NWM0OTlmIiwicm9sZXMiOlsic2VydmljZSIsImNvbmZpZyJdfQ.-5Bkwe7Z6QSt6DuStpye0sYSgwEonBzy-UHXMCzpVcs"

				export          G_MS_GET_TABLEBYID_URL="https://edge-gateway-test.cp-epg2i.domain.com/roadmaster/tables";
				export          G_MS_GET_WORK_UNIT_URL="https://chevelle-micro-test.cp-epg2i.domain.com/basicstats/que/getworkunit"
				export G_MS_WRITE_1TABLE_TO_OUTPUT_URL="https://chevelle-micro-test.cp-epg2i.domain.com/basicstats/out/save";
				export      G_MS_SAVE_BATCH_OUTPUT_URL="https://chevelle-micro-test.cp-epg2i.domain.com/basicstats/out/savebatch";
				export           G_MS_UPDATE_QUEUE_URL="https://chevelle-micro-test.cp-epg2i.domain.com/basicstats/que/update";

				export           G_MS_AUTHENTICATE_URL="https://chevelle-messaging-test.cp-epg2i.domain.com/authenticate";	# BSTAT to pass a message for each completed Table
				export               G_MS_OUTBOUND_URL="https://chevelle-messaging-test.cp-epg2i.domain.com/events/basic_stats/outbound/updateQueue";	# BSTAT to pass a message for each completed Table
				export       G_MS_MESSAGING_SECRET_KEY="3YmCdvHBKX;UwXrn"
				;;
			"CRT")	
				export	G_MS_AUTHORIZATION_KEY="Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJhY2N0VHlwZSI6InNydkFjY3QiLCJzdWIiOiJub21hZC1jb25maWctMjJjNzM1NmQtZmJhMy00OWU3LWEwZWUtOTU5YWQ5NWM0OTlmIiwianRpIjoiZjM3NmU3MWEtYTdjMC00MGNmLWFmNjEtMjhjMDMxM2QzNTIyIiwiaXNzIjoiZ2RhYXMuZ20iLCJuYmYiOjE1MzAzODA0OTIsImlhdCI6MTUzMDM4MDQ5MiwiZXhwIjoxNTkzNDUyNDkyLCJnbWlkIjoibm9tYWQtY29uZmlnLTIyYzczNTZkLWZiYTMtNDllNy1hMGVlLTk1OWFkOTVjNDk5ZiIsImdtaW4iOiJub21hZC1jb25maWctMjJjNzM1NmQtZmJhMy00OWU3LWEwZWUtOTU5YWQ5NWM0OTlmIiwicm9sZXMiOlsic2VydmljZSIsImNvbmZpZyJdfQ.-5Bkwe7Z6QSt6DuStpye0sYSgwEonBzy-UHXMCzpVcs"
				export          G_MS_GET_TABLEBYID_URL="https://edge-gateway-qa.cp-epg2i.domain.com/roadmaster/tables";
				export          G_MS_GET_WORK_UNIT_URL="https://chevelle-micro-qa.cp-epg2i.domain.com/basicstats/que/getworkunit"
				export G_MS_WRITE_1TABLE_TO_OUTPUT_URL="https://chevelle-micro-qa.cp-epg2i.domain.com/basicstats/out/save";
				export      G_MS_SAVE_BATCH_OUTPUT_URL="https://chevelle-micro-qa.cp-epg2i.domain.com/basicstats/out/savebatch";
				export           G_MS_UPDATE_QUEUE_URL="https://chevelle-micro-qa.cp-epg2i.domain.com/basicstats/que/update";

				export           G_MS_AUTHENTICATE_URL="https://chevelle-messaging-qa.cp-epg2i.domain.com/authenticate";	# BSTAT to pass a message for each completed Table
				export               G_MS_OUTBOUND_URL="https://chevelle-messaging-qa.cp-epg2i.domain.com/events/basic_stats/outbound/updateQueue";	# BSTAT to pass a message for each completed Table
				export       G_MS_MESSAGING_SECRET_KEY="3YmCdvHBKX;UwXrn"
				;;
			"PRD")	
				export	G_MS_AUTHORIZATION_KEY="Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJhY2N0VHlwZSI6InNydkFjY3QiLCJzdWIiOiJub21hZC1jb25maWctMjJjNzM1NmQtZmJhMy00OWU3LWEwZWUtOTU5YWQ5NWM0OTlmIiwianRpIjoiZjM3NmU3MWEtYTdjMC00MGNmLWFmNjEtMjhjMDMxM2QzNTIyIiwiaXNzIjoiZ2RhYXMuZ20iLCJuYmYiOjE1MzAzODA0OTIsImlhdCI6MTUzMDM4MDQ5MiwiZXhwIjoxNTkzNDUyNDkyLCJnbWlkIjoibm9tYWQtY29uZmlnLTIyYzczNTZkLWZiYTMtNDllNy1hMGVlLTk1OWFkOTVjNDk5ZiIsImdtaW4iOiJub21hZC1jb25maWctMjJjNzM1NmQtZmJhMy00OWU3LWEwZWUtOTU5YWQ5NWM0OTlmIiwicm9sZXMiOlsic2VydmljZSIsImNvbmZpZyJdfQ.-5Bkwe7Z6QSt6DuStpye0sYSgwEonBzy-UHXMCzpVcs"

				export          G_MS_GET_TABLEBYID_URL="https://edge-gateway.cpi.domain.com/roadmaster/tables";
				export          G_MS_GET_WORK_UNIT_URL="https://chevelle-micro.cpi.domain.com/basicstats/que/getworkunit"
				export G_MS_WRITE_1TABLE_TO_OUTPUT_URL="https://chevelle-micro.cpi.domain.com/basicstats/out/save";
				export      G_MS_SAVE_BATCH_OUTPUT_URL="https://chevelle-micro.cpi.domain.com/basicstats/out/savebatch";
				export           G_MS_UPDATE_QUEUE_URL="https://chevelle-micro.cpi.domain.com/basicstats/que/update";

				#export           G_MS_AUTHENTICATE_URL="https://chevelle-messaging.cp-epg2i.domain.com/authenticate";	# BSTAT to pass a message for each completed Table
				#export               G_MS_OUTBOUND_URL="https://chevelle-messaging.cp-epg2i.domain.com/events/basic_stats/outbound/updateQueue";	# BSTAT to pass a message for each completed Table
				export            G_MS_AUTHENTICATE_URL="https://chevelle-messaging.cpi.domain.com/authenticate";									# BSTAT to pass a message for each completed Table
				export                G_MS_OUTBOUND_URL="https://chevelle-messaging.cpi.domain.com/events/basic_stats/outbound/updateQueue";		# BSTAT to pass a message for each completed Table
				
				export       G_MS_MESSAGING_SECRET_KEY="3YmCdvHBKX;UwXrn"

				;;
			*)	;;
		esac ;

		export G_PYTHON_PROGRAM="hyper_profile_desc_stats.py";		# Default; Override with -P
		export G_PROJECT_DIRNAME="CHEVELLE_STAT";			# Default; Override with -P  (Linux Root Dir for this input file): 
		if [[ ! -z "$p_PROJECT_DIRNAME" ]]; then export G_PROJECT_DIRNAME="$p_PROJECT_DIRNAME"; fi;

		export G_YARN_SPARK_NAME="CHEVELLE_STAT"; 
		if [[ ! -z "$p_YARN_SPARK_NAME" ]]; then export G_YARN_SPARK_NAME="$p_YARN_SPARK_NAME"; fi;

		if [[ ! -z "$p_HDFS_ROOT_DIR"   ]]; then export G_HDFS_ROOT_DIR="$p_HDFS_ROOT_DIR";     fi;
		G_HDFS_ROOT_DIR=$(echo $G_HDFS_ROOT_DIR| sed -e 's:^/::' -e 's:/$::' );	#Remove leading and trailing '/', if exists
		G_HDFS_ROOT_DIR="/${ENV}/${G_HDFS_ROOT_DIR}";			# Add env to front (/DEV, /TST, ....)
		export   G_HDFS_INPUT_DIR="${G_HDFS_ROOT_DIR}/Input";		# Contain HDFS input file for PySpark to tun
		export  G_HDFS_OUTPUT_DIR="${G_HDFS_ROOT_DIR}/Output_Desc";	# Contain HDFS output file (1 for each table)
		export G_HDFS_ARCHIVE_DIR="${G_HDFS_ROOT_DIR}/Archive";		# After each HDFS Split_File completes successfully it will be moved into HDFS_ARCHIVE

		export     G_HDFS_OUTPUT_DIR="${G_HDFS_ROOT_DIR}/Output_Desc";	# Contain HDFS output file (1 for each table)

		if [[ -z "$p_MAX_PARA_SPLITFILES_PER_GROUP_str" ]]
		then	export	G_MAX_PARA_SPLITFILES_PER_GROUP=100;	#Up to 100 bkg processes to execute in parallel at a time
		else	export	G_MAX_PARA_SPLITFILES_PER_GROUP="$p_MAX_PARA_SPLITFILES_PER_GROUP_str";  #Override
		fi;

		;;
	"DC")		
		case "$ENV" in
			"DEV")	
				export	G_MS_AUTHORIZATION_KEY="Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJhY2N0VHlwZSI6InNydkFjY3QiLCJzdWIiOiJub21hZC1jb25maWctMjJjNzM1NmQtZmJhMy00OWU3LWEwZWUtOTU5YWQ5NWM0OTlmIiwianRpIjoiZjM3NmU3MWEtYTdjMC00MGNmLWFmNjEtMjhjMDMxM2QzNTIyIiwiaXNzIjoiZ2RhYXMuZ20iLCJuYmYiOjE1MzAzODA0OTIsImlhdCI6MTUzMDM4MDQ5MiwiZXhwIjoxNTkzNDUyNDkyLCJnbWlkIjoibm9tYWQtY29uZmlnLTIyYzczNTZkLWZiYTMtNDllNy1hMGVlLTk1OWFkOTVjNDk5ZiIsImdtaW4iOiJub21hZC1jb25maWctMjJjNzM1NmQtZmJhMy00OWU3LWEwZWUtOTU5YWQ5NWM0OTlmIiwicm9sZXMiOlsic2VydmljZSIsImNvbmZpZyJdfQ.-5Bkwe7Z6QSt6DuStpye0sYSgwEonBzy-UHXMCzpVcs"
				export          G_MS_GET_TABLEBYID_URL="https://edge-gateway.cp-epg2i.domain.com/roadmaster/tables";
				export          G_MS_GET_WORK_UNIT_URL="https://chevelle-micro-dev.cp-epg2i.domain.com/dataclass/que/getworkunit"
				export G_MS_WRITE_1TABLE_TO_OUTPUT_URL="https://chevelle-micro-dev.cp-epg2i.domain.com/dataclass/out/save";
				export      G_MS_SAVE_BATCH_OUTPUT_URL="https://chevelle-micro-dev.cp-epg2i.domain.com/dataclass/out/savebatch";
				export           G_MS_UPDATE_QUEUE_URL="https://chevelle-micro-dev.cp-epg2i.domain.com/dataclass/que/update";
				;;
			"TST")	
				export	G_MS_AUTHORIZATION_KEY="Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJhY2N0VHlwZSI6InNydkFjY3QiLCJzdWIiOiJub21hZC1jb25maWctMjJjNzM1NmQtZmJhMy00OWU3LWEwZWUtOTU5YWQ5NWM0OTlmIiwianRpIjoiZjM3NmU3MWEtYTdjMC00MGNmLWFmNjEtMjhjMDMxM2QzNTIyIiwiaXNzIjoiZ2RhYXMuZ20iLCJuYmYiOjE1MzAzODA0OTIsImlhdCI6MTUzMDM4MDQ5MiwiZXhwIjoxNTkzNDUyNDkyLCJnbWlkIjoibm9tYWQtY29uZmlnLTIyYzczNTZkLWZiYTMtNDllNy1hMGVlLTk1OWFkOTVjNDk5ZiIsImdtaW4iOiJub21hZC1jb25maWctMjJjNzM1NmQtZmJhMy00OWU3LWEwZWUtOTU5YWQ5NWM0OTlmIiwicm9sZXMiOlsic2VydmljZSIsImNvbmZpZyJdfQ.-5Bkwe7Z6QSt6DuStpye0sYSgwEonBzy-UHXMCzpVcs"
				export          G_MS_GET_TABLEBYID_URL="https://edge-gateway-test.cp-epg2i.domain.com/roadmaster/tables";
				export          G_MS_GET_WORK_UNIT_URL="https://chevelle-micro-test.cp-epg2i.domain.com/dataclass/que/getworkunit"
				export G_MS_WRITE_1TABLE_TO_OUTPUT_URL="https://chevelle-micro-test.cp-epg2i.domain.com/dataclass/out/save";
				export      G_MS_SAVE_BATCH_OUTPUT_URL="https://chevelle-micro-test.cp-epg2i.domain.com/dataclass/out/savebatch";
				export           G_MS_UPDATE_QUEUE_URL="https://chevelle-micro-test.cp-epg2i.domain.com/dataclass/que/update";
				;;
			"CRT")	
				export	G_MS_AUTHORIZATION_KEY="Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJhY2N0VHlwZSI6InNydkFjY3QiLCJzdWIiOiJub21hZC1jb25maWctMjJjNzM1NmQtZmJhMy00OWU3LWEwZWUtOTU5YWQ5NWM0OTlmIiwianRpIjoiZjM3NmU3MWEtYTdjMC00MGNmLWFmNjEtMjhjMDMxM2QzNTIyIiwiaXNzIjoiZ2RhYXMuZ20iLCJuYmYiOjE1MzAzODA0OTIsImlhdCI6MTUzMDM4MDQ5MiwiZXhwIjoxNTkzNDUyNDkyLCJnbWlkIjoibm9tYWQtY29uZmlnLTIyYzczNTZkLWZiYTMtNDllNy1hMGVlLTk1OWFkOTVjNDk5ZiIsImdtaW4iOiJub21hZC1jb25maWctMjJjNzM1NmQtZmJhMy00OWU3LWEwZWUtOTU5YWQ5NWM0OTlmIiwicm9sZXMiOlsic2VydmljZSIsImNvbmZpZyJdfQ.-5Bkwe7Z6QSt6DuStpye0sYSgwEonBzy-UHXMCzpVcs"
				export          G_MS_GET_TABLEBYID_URL="https://edge-gateway-qa.cp-epg2i.domain.com/roadmaster/tables";
				export          G_MS_GET_WORK_UNIT_URL="https://chevelle-micro-qa.cp-epg2i.domain.com/dataclass/que/getworkunit"
				export G_MS_WRITE_1TABLE_TO_OUTPUT_URL="https://chevelle-micro-qa.cp-epg2i.domain.com/dataclass/out/save";
				export      G_MS_SAVE_BATCH_OUTPUT_URL="https://chevelle-micro-qa.cp-epg2i.domain.com/dataclass/out/savebatch";
				export           G_MS_UPDATE_QUEUE_URL="https://chevelle-micro-qa.cp-epg2i.domain.com/dataclass/que/update";

				;;
			"PRD")	
				export	G_MS_AUTHORIZATION_KEY="Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJhY2N0VHlwZSI6InNydkFjY3QiLCJzdWIiOiJub21hZC1jb25maWctMjJjNzM1NmQtZmJhMy00OWU3LWEwZWUtOTU5YWQ5NWM0OTlmIiwianRpIjoiZjM3NmU3MWEtYTdjMC00MGNmLWFmNjEtMjhjMDMxM2QzNTIyIiwiaXNzIjoiZ2RhYXMuZ20iLCJuYmYiOjE1MzAzODA0OTIsImlhdCI6MTUzMDM4MDQ5MiwiZXhwIjoxNTkzNDUyNDkyLCJnbWlkIjoibm9tYWQtY29uZmlnLTIyYzczNTZkLWZiYTMtNDllNy1hMGVlLTk1OWFkOTVjNDk5ZiIsImdtaW4iOiJub21hZC1jb25maWctMjJjNzM1NmQtZmJhMy00OWU3LWEwZWUtOTU5YWQ5NWM0OTlmIiwicm9sZXMiOlsic2VydmljZSIsImNvbmZpZyJdfQ.-5Bkwe7Z6QSt6DuStpye0sYSgwEonBzy-UHXMCzpVcs"
				export          G_MS_GET_TABLEBYID_URL="https://edge-gateway.cpi.domain.com/roadmaster/tables";
				export          G_MS_GET_WORK_UNIT_URL="https://chevelle-micro.cpi.domain.com/dataclass/que/getworkunit"
				export G_MS_WRITE_1TABLE_TO_OUTPUT_URL="https://chevelle-micro.cpi.domain.com/dataclass/out/save";
				export      G_MS_SAVE_BATCH_OUTPUT_URL="https://chevelle-micro.cpi.domain.com/dataclass/out/savebatch";
				export           G_MS_UPDATE_QUEUE_URL="https://chevelle-micro.cpi.domain.com/dataclass/que/update";
				;;
			*)	;;
		esac ;

		export G_PYTHON_PROGRAM="data_classification.py";	# Default; Override with -P
#### DEBUG
#### export G_PYTHON_PROGRAM="mdb.py";					# Default; Override with -P



		export G_PROJECT_DIRNAME="DATA_CLASSIFICATION";		# Default; Override with -p  (Linux Root Dir for this input file)
		if [[ ! -z "$p_PROJECT_DIRNAME" ]]; then export G_PROJECT_DIRNAME="$p_PROJECT_DIRNAME"; fi;

		export G_YARN_SPARK_NAME="DATA_CLASSIFICATION"; 
		if [[ ! -z "$p_YARN_SPARK_NAME" ]]; then export G_YARN_SPARK_NAME="$p_YARN_SPARK_NAME"; fi;

		if [[ ! -z "$p_HDFS_ROOT_DIR"   ]]; then export G_HDFS_ROOT_DIR="$p_HDFS_ROOT_DIR";     fi;
		G_HDFS_ROOT_DIR=$(echo $G_HDFS_ROOT_DIR| sed -e 's:^/::' -e 's:/$::' );	#Remove leading and trailing '/', if exists
		G_HDFS_ROOT_DIR="/${ENV}/${G_HDFS_ROOT_DIR}";		# Add env to front (/DEV, /TST, ....)
		export   G_HDFS_INPUT_DIR="${G_HDFS_ROOT_DIR}/Input";	# Contain HDFS input file for PySpark to tun
		export  G_HDFS_OUTPUT_DIR="${G_HDFS_ROOT_DIR}/Output";	# Contain HDFS output file (1 for each table)
		export G_HDFS_ARCHIVE_DIR="${G_HDFS_ROOT_DIR}/Archive";	# After each HDFS Split_File completes successfully it will be moved into HDFS_ARCHIVE

		if [[ -z "$p_MAX_PARA_SPLITFILES_PER_GROUP_str" ]]
		then	export	G_MAX_PARA_SPLITFILES_PER_GROUP=1;	#Only 1 bkg to execute at a time
		else	export	G_MAX_PARA_SPLITFILES_PER_GROUP="$p_MAX_PARA_SPLITFILES_PER_GROUP_str";  #Override
		fi;
		;;

esac;	#--}


if [[ -z "$1" ]]
then
	# Must enter a Number (even if empty) so it can set up Checkpoint directories 
	# If it sees the SAME name it will use same   Checkpoint info.
	# If it sees the NEW name  it will set up new Checkpoint directories
	# At this time:  $1  is just simple integer   1,2,3 ...
	# which will be translated into:  tbl_list1.txt, or tbl_list2.txt, or tbl_list3.txt ....
	printf "\n\n\n***** ERROR:  Must enter a number representing filename\t\t$(GetCurrTime)\n\n"
	exit -1;
else
	export G_JOB_NUM="${1}";		# This is a number the user specifies at command line

	case "$G_PROGRAM_TYPE" in
	"BSTAT")	export G_FILENAME_PREFIX="tbl_list";;	# tbl_list   for BSTAT
	"DC")		export G_FILENAME_PREFIX="dc_list";;	#  dc_list   for DataClassification
	esac;

	export G_INPUT_LIST="${G_FILENAME_PREFIX}${1}.txt";		# This is a number the user specifies at command line
	export G_NEW_SPARK_JOB_NAME="${G_YARN_SPARK_NAME}___j${1}";	# There will be more appended to this variable
					# ___j = jobnum
fi;

if [[ ! -x "${G_MAIN_PROG_DIR}/${G_PYTHON_PROGRAM}" ]]
then	printf "\n\n\n***** ERROR: Python Program not found:  \"${G_MAIN_PROG_DIR}/${G_PYTHON_PROGRAM}\"\t\t$(GetCurrTime)\n\n\n";
	exit -1;
fi;

export                G_ASMS_DIR="${G_ROOT_PROGDIR}/${G_PROJECT_DIRNAME}";			# (dir IS NOT dependent on Input File)
export                G_ROOT_DIR="${G_ROOT_PROGDIR}/${G_PROJECT_DIRNAME}/${G_INPUT_LIST}";	# Difference between: G_ROOT_DIR   and G_ROOT_PROG_DIR
export        G_PROJECT_ROOT_DIR="${G_ROOT_PROGDIR}/${G_PROJECT_DIRNAME}";
export          G_CHECKPOINT_DIR="${G_ROOT_DIR}/CHECKPOINT";  
export  G_CHECKPOINT_ARCHIVE_DIR="${G_ROOT_DIR}/CHECKPOINT/ARCHIVE";   

export           G_PRIOR_APP_DIR="${G_ROOT_DIR}/PRIOR_APP";
export         G_PRIOR_TORUN_DIR="${G_ROOT_DIR}/PRIOR_TORUN";
export               G_TORUN_DIR="${G_ROOT_DIR}/TORUN";
export             G_BATCHID_DIR="${G_ROOT_DIR}/BATCHID";
export            G_BATCHID_FILE="${G_BATCHID_DIR}/BATCH_ID.txt"

export       G_ARCHIVE_LINUX_DIR="${G_ROOT_DIR}/ARCHIVE/${G_SESSION_DATE}";
export                 G_LOG_DIR="${G_ROOT_DIR}/LOGS/${G_SESSION_DATE}";
export               G_FLAGS_DIR="${G_ROOT_DIR}/FLAGS/${G_SESSION_DATE}";

export            G_ALL_YARN_LOGS="${G_LOG_DIR}/All_YARN_Logs.txt";
export   G_ALL_FINAL_SUMMARY_LOGS="${G_LOG_DIR}/All_FINAL_SUMMARY.txt";

	# Now add $ENV to front of path to these 3 HDFS dirs
export    G_HDFS_ARCHIVE_DIR="${G_HDFS_ROOT_DIR}/Archive";	# After each HDFS Split_File completes successfully it will be moved into HDFS_ARCHIVE

export G_HDFS_HIVE_DATATYPES_DIR="${G_HDFS_ROOT_DIR}/HiveColumnsTypes";	#Contain HDFS input file for MIN/MAX basic stat function
export G_HDFS_CHECKPOINT_ROOT_DIR="${G_HDFS_ROOT_DIR}/Checkpoint/${G_PROJECT_DIRNAME}/${G_INPUT_LIST}";	
	# As PySpark completes calculating Col_Stats for EACHtable it will create a SUCCESS JSON row in HDFS_CHECKPOINT_DIR.
	# NOTE:	This HDFS_CheckpointDir NAME must be based on G_INPUT_LIST
	#	This way all tables within this G_INPUT_LIST will have a flag in this HDFS_CHECKPOINT_DIR,
	#	regardless of which Split_File.
	#	PySpark checks CHECKPOINT of every table of every split file, and skips as appropriate

export G_LAST_STEP_TO_COMPLETE="FINAL"




G_EMAIL_TEMP_FILE="${G_LOG_DIR}/email_temp.txt"
G_EMAIL_FILE="${G_LOG_DIR}/email.txt"

MakeLinuxDir   $G_LOG_DIR;
MakeLinuxDir   $G_ARCHIVE_LINUX_DIR;
MakeLinuxDir   $G_PRIOR_APP_DIR;
MakeLinuxDir   $G_PRIOR_TORUN_DIR;
MakeLinuxDir   $G_TORUN_DIR;
MakeLinuxDir   $G_BATCHID_DIR;
MakeLinuxDir   $G_FLAGS_DIR
CreateInternalFileNames   $G_LOG_DIR   $G_FNSTR;

DisplayGlobalVariables;


>$G_EMAIL_FILE	#Always generate email file regardless if user requested.

if [[ "$G_RESET_NORUN" == "YES" ]]
then
	ResetToStartFresh   $G_TORUN_DIR   $G_PRIOR_TORUN_DIR   $G_PRIOR_APP_DIR   $G_FLAGS_DIR   $G_HDFS_CHECKPOINT_ROOT_DIR  "\t"  ; 

	G_JOB_STEPS_COMPLETED_SO_FAR="$G_LAST_STEP_TO_COMPLETE" 
	G_JOB_STATUS="SUCCESS"
	exit 0;
fi;

if [[ "$G_RESET" == "YES" ]]
then 
	printf "\n\nUser requested RESET option\n" >> $G_EMAIL_FILE
	ResetToStartFresh  $G_TORUN_DIR  $G_PRIOR_TORUN_DIR $G_FLAGS_DIR $G_HDFS_CHECKPOINT_ROOT_DIR  "\t"  ; 
fi;

#Check to make sure if there are any Split_Files in TORUN Dir.
#If there are some, that means the previous run failed.
# (If a run was successful, all Split scripts will be removed!)

G_JOB_STEPS_COMPLETED_SO_FAR="STEP0";	


# NEW: Ignore all previous checkpoint files (as we are now relying on Oracle QUEUE for checkpoints)
printf "Remove all files from TORUN_DIR (ignore all previous runs):\n"
rm -f -r ${G_PRIOR_APP_DIR}/*   2>/dev/null
rm -f -r ${G_PRIOR_TORUN_DIR}/* 2>/dev/null
rm -f -r ${G_TORUN_DIR}/*       2>/dev/null

	#printf "\n\nCHECKING if any files left over from previous run (ie, files LEFTOVER from previous failed run)\n";



printf "\t(TORUN_DIR=$G_TORUN_DIR)\n";
totalTORUNscripts=$(ls -1 ${G_TORUN_DIR}/*txt 2>/dev/null | wc -l);
filesCountStr="file"
if [[ $totalTORUNscripts -gt 1 ]]; then filesCountStr="files"; fi;
printf "\n\tFound:  $totalTORUNscripts  Checkpoint ${filesCountStr}  at TORUN dir ($G_TORUN_DIR)\n";

printf "INPUT:\nFound: $totalTORUNscripts Checkpoint $filesCountStr  at TORUN dir($G_TORUN_DIR):\n"	>> $G_EMAIL_FILE
ls -1 ${G_TORUN_DIR}/*txt  2>/dev/null |nl| sed 's:^:\t:'						>> $G_EMAIL_FILE
printf "\n"												>> $G_EMAIL_FILE

if [[ $totalTORUNscripts -gt 0 ]]
then	#--{
	printf "\n\tThis means the previous run failed, and left these checkpoint files for re-processing.\n\n";
	printf "\n\nThese are the checkpoint files found:\n";
	ls -1 ${G_TORUN_DIR}/*txt 2>/dev/null |nl|sed 's:^:\t\t:'

	printf "\n\n\t==> Skip Re-Calculating Table Filesizes steps;\n";
	printf "\n\t==> You can issue -R to reset these checkpoints to start at begininng (recalulating Filesizes)\n";

	G_JOB_STEPS_COMPLETED_SO_FAR="STEP9";	

else	#--}{

	##### printf "\t\nThis means the previous run ran successfully.  All checkpointed files were cleared out\n\n";

	>$G_TABLES_LIST_1B;
	chmod 755 $G_TABLES_LIST_1B;

	#BEFORE: printf "\n\nSTEP 1: Get HDFS input file ($G_INPUT_LIST) from HDFS ($G_HDFS_INPUT_DIR)\n\tinto Linux file:  ${G_TABLES_LIST_1}\n"
	printf "\n\nSTEP 1: Get input file ($G_INPUT_LIST) from MicroService into Linux file:  ${G_TABLES_LIST_1}\n"
	GetInputFile  $G_INPUT_LIST  $G_HDFS_INPUT_DIR   $G_PROGRAM_TYPE   $G_LOG_DIR   $G_TABLES_LIST_1   $G_INPUT_LIST_PER_PROGRAM_TYPE  "\t"
		# 1st outputFile:  G_TABLES_LIST_1:               consists of only 2 fields:	SchemaName<TAB>TableName    -- used for SplitFile  
		# 2nd outputFile:  G_INPUT_LIST_PER_PROGRAM_TYPE: consists of whatever format needed by the app. such as BSTAT, or DC, ...

	# G_BATCH_ID is available here, exported from above function GetInputFile 


	G_JOB_STEPS_COMPLETED_SO_FAR="STEP1";	


	#----------------------------------------------------------------------------------------------------------------------
	# No longer needed to do this step "excludeSecretASMS" as it will be ALREADY EXCLUDED from the QUEUE table
	#	printf "\n\nSTEP 2: Exclude secret ASMS from InputList:   (ASMS Exclude File=${G_ASMS_EXCLUDE_FILE})\n"
	#	G_ASMS_EXCLUDE_FILE="${G_PROJECT_ROOT_DIR}/${G_ASMS_EXCLUDE_FILE}";
	#	G_INPUT_LIST_PREASMS="${G_LOG_DIR}/${G_INPUT_LIST}_pre_ASMS_exclude"
	#	cp  $G_TABLES_LIST_1  $G_INPUT_LIST_PREASMS
	#	ExcludeSecretASMS     $G_ASMS_EXCLUDE_FILE   $G_INPUT_LIST_PREASMS   $G_TABLES_LIST_1  $G_LOG_DIR
	#----------------------------------------------------------------------------------------------------------------------


	printf "\n\nSTEP 2: Make Checkpoint Directories\n";
	Make_1_HDFS_Dir	$G_HDFS_CHECKPOINT_ROOT_DIR  "HDFS CheckpointDir"  "\t"
		# G_INPUT_LIST is part of this HDFS_CHECKPOINT_DIR name
		# G_SPLIT_FILENAME will also be added to this ROOT_CHECKPOINT, and SUCCESS and FAIL sub_HDFS_dirs
	G_JOB_STEPS_COMPLETED_SO_FAR="STEP2";	
	printf "\n\n";
	>$G_TABLE_SIZE_1; 
	chmod 755 $G_TABLE_SIZE_1;


	#----------------------------------------------------------------------------------------------------------------------
	# No longer needed to do this step "ExecuteHIVECommands" (to get the HDFS Dir of each table) as it is provided by the MicroService 
	#	printf "\nSTEP 2: Generate Hive SHOW CREATE TABLE commands for input tables:\n";
	#	GenerateSHOWCREATETABLECommands   $G_TABLES_LIST_1     $G_SHOW_CREATE_TABLE_1_HQL   $G_TABLES_LIST_1B  "\t";
	#	G_JOB_STEPS_COMPLETED_SO_FAR="STEP2";	
	#	
	#	printf "\n\nSTEP 3: Execute Hive SHOW CREATE TABLE commands:\n";
	#	ExecuteHIVECommands     $G_SHOW_CREATE_TABLE_1_HQL    $G_SHOW_CREATE_TABLE_1_TXT  "CONTINUE"   "\t";
	#	G_JOB_STEPS_COMPLETED_SO_FAR="STEP3";	
	#----------------------------------------------------------------------------------------------------------------------

	printf "\n\nSTEP 3: Get HDFS Dirs for all tables\n";
	cp $G_TABLES_LIST_1  $G_TABLES_LIST_1B


	printf "\n\nSTEP 4: Extract HDFS filesize for each table:\n";
	ExtractFileSizes_FromHDFS_FilesListing	$G_TABLES_LIST_1B     $G_SHOW_CREATE_TABLE_1_TXT			\
						$G_CHUNK_SIZE         $G_TABLE_SIZE_1    $G_NUM_EXECUTORS_DEFAULT	\
						$G_MIN_NUM_EXECUTORS  $G_LOG_DIR         "\t" 
	G_JOB_STEPS_COMPLETED_SO_FAR="STEP4";	

	printf "\n\t===========================================================================================================================\n";
	printf "\tResults of NUM_EXECUTORS derived from HDSF filesizes: ($G_TABLE_SIZE_1):\n";
	if [[ -f $G_TABLE_SIZE_1 ]]
	then	
		nl  $G_TABLE_SIZE_1  | sed 's:^:\t\t:' 
		cat $G_TABLE_SIZE_1  >> $G_ALL_FILES_SIZES_BOTH_TRIES
	else
		printf "\n\n\n***** ERROR: Cannot find file:  G_TABLE_SIZE: $G_TABLE_SIZE_1;\t\t$(GetCurrTime)\n\n\n"
		exit -1;
	fi;
	printf "\t===========================================================================================================================\n";
	cat $G_TABLE_SIZE_1 > $G_ALL_FILES_SIZES_BOTH_TRIES  


	printf "\n\nSTEP 5: Sort Tables By NumExecutors:\n";
	SortTablesByNumExecutors     $G_ALL_FILES_SIZES_BOTH_TRIES  $G_ALL_FILES_SIZES_SORTED_BOTH_TRIES "\t"
	G_JOB_STEPS_COMPLETED_SO_FAR="STEP5";	


	printf "\n\nSTEP 6: Generate list of DISTINCT Executors:  Output File=$G_DISTINCT_EXECUTORS_BOTH_TRIES\n";
	cut -d"," -f1   $G_ALL_FILES_SIZES_SORTED_BOTH_TRIES | sort -n -u >$G_DISTINCT_EXECUTORS_BOTH_TRIES
	sed 's:^:\t\t:' $G_DISTINCT_EXECUTORS_BOTH_TRIES
	G_JOB_STEPS_COMPLETED_SO_FAR="STEP6";	


	printf "\n\nSTEP 7: VERIFY:\n"
	printf "\tTables with FileSize: ($G_TABLE_SIZE_1)\n";
	nl $G_TABLE_SIZE_1            |sed 's:^:	:';

	totalDistinct=$(wc -l $G_DISTINCT_EXECUTORS_BOTH_TRIES|cut -d" " -f1)
	printf "\n\n\tList of DISTINCT Executors: $totalDistinct\t\t(See File: $G_DISTINCT_EXECUTORS_BOTH_TRIES)\n";
	let n=0;
	for numExe in $(cat $G_DISTINCT_EXECUTORS_BOTH_TRIES)
	do
		n=$((n+1))
		total=$(grep "^${numExe}," $G_ALL_FILES_SIZES_SORTED_BOTH_TRIES |wc -l);
		printf "\t\t%4d/%-4d  NumExecutor %4d:  has %4d  tables\n"      "$n"   "$totalDistinct"    "$numExe"   "$total"
	done;
	G_JOB_STEPS_COMPLETED_SO_FAR="STEP7";	

	
	printf "\n\nSTEP 8: Generate Split Files and Assign NumExecutors:\n";
			# The 1st parameter is only used to formulate filenNmes/directoryNames
			# The 2nd parameter is the one that get splitted
	GenerateSplitFilesInTORUNDir	$G_INPUT_LIST				$G_ALL_FILES_SIZES_SORTED_BOTH_TRIES	\
									$G_DISTINCT_EXECUTORS_BOTH_TRIES	$G_LOG_DIR						\
									$G_FNSTR 				$G_PRIOR_APP_DIR							\
									$G_SESSION_DATE			"$G_BATCH_ID"  "\t";

	G_JOB_STEPS_COMPLETED_SO_FAR="STEP8";	

		#To override this function, set: G_SPLIT_FILE_BATCH_SIZE=large value, like a few thousand
		#Internal to this SubSplitPerTableBatchSize function it will set DIVISOR=1, hence not sub-splitting the input file
	printf "\n\nSTEP 9: Further Split Files based on G_SPLIT_FILE_BATCH_SIZE=$G_SPLIT_FILE_BATCH_SIZE:\n";
	SubSplitPerTableBatchSize	$G_SPLIT_FILE_BATCH_SIZE   $G_PRIOR_APP_DIR   $G_PRIOR_TORUN_DIR   $G_LOG_DIR   "\t"
	G_JOB_STEPS_COMPLETED_SO_FAR="STEP9";	

	printf "\n\nSTEP 10: Get actual data (like additional columns) specific for each Program_Type (BSTAT, DC, ...) :\n";
	ConvertGenericSplitFile_To_AppSplitFile    $G_PROGRAM_TYPE  $G_PRIOR_TORUN_DIR   $G_TORUN_DIR   $G_INPUT_LIST_PER_PROGRAM_TYPE $G_LOG_DIR   "\t"
fi;	#--}



# DEBUG
#exit;




if [[ ! -s $G_BATCHID_FILE ]]
then
	printf "\n\n\nERROR: Cannot find file \"$G_BATCHID_FILE\" to retrieve BATCH_ID\n\n\n"
	printf "\tSOLUTION:  Execute again with Reset option (-R) on command line, like:   \".../hyper_profile_desc_stats.sh   -R  1\"\n\n"
	printf "\t\t(-R must be appear BEFORE the last number)\n";
	exit -1;
fi;

# Must be able to retrieve BATCH_ID from file in case of previous failed runs:
export G_BATCH_ID=$(awk '{print $1}' $G_BATCHID_FILE);
printf "\n\n
==========
========== Retrieved BATCH_ID ==> $G_BATCH_ID (from File $G_BATCHID_FILE)
========== REMEMBER:  This BATCH_ID ($G_BATCH_ID) MUST exists in the DataBase also
==========
\n\n\n"

#==========            If You MANUALLY CLEARED this BATCH_ID in RoadMaster database ==> You MUST Use option -R  on command line  for this AutoSysJobNum 
#==========            (-R clears out all Checkpointed files, Linux/HDFS, for this Autosys JobNum, allowing to call MicroService GetWorkUnit to retrieve a new batch)
#==========

		### Normally, when you rerun a failed job, the previous BATCH_ID is still available in the Oracle Que, and in thie BATCH_ID file
		### ==> So everything works.  That BATCH_ID will be used to update Oracle Queue and insert Output Table.

if [[ -z "$G_BATCH_ID" ]]
then
	printf "\n\n\nERROR:  G_BATCH_ID cannot be NULL!  It must exist in file: \"$G_BATCHID_FILE\"\n\n\n"
	exit -1;
fi;


printf "${itab1}=======================================================================================================\n";
printf "${itab1}=============== Now, submit these Split Files to Spark, with G_BATCH_ID= $G_BATCH_ID  ===============\n";
printf "${itab1}=======================================================================================================\n\n";
DisplayDirectory $G_TORUN_DIR    "TORUN"     ""    "${itab1}";
printf "\n\n"


export G_HDFS_SPLITFILE_ARCHIVE_DIR="${G_HDFS_ARCHIVE_DIR}/${G_PROJECT_DIRNAME}/SPLITFILES_OF_${G_INPUT_LIST}"
	# Keep this HDFS dir going as it will hold the archive for input file
Make_1_HDFS_Dir	$G_HDFS_SPLITFILE_ARCHIVE_DIR  "ARCHIVE DIR for Split files of this input file"  "\t"


printf "\n\nSTEP 11: Submit ALL Split Files in G_TORUN_DIR ($G_TORUN_DIR) to SparkCluster:\n";
Execute	"$G_INPUT_LIST"               "$G_TORUN_DIR"          "$G_LOG_DIR"          "$G_FLAGS_DIR"        "$G_ARCHIVE_LINUX_DIR"	\
	"$G_NEW_SPARK_JOB_NAME"	          "$G_HDFS_INPUT_DIR"     "$G_HDFS_OUTPUT_DIR"  "$G_HDFS_CHECKPOINT_ROOT_DIR"	\
	"$G_HDFS_SPLITFILE_ARCHIVE_DIR"	  "$G_HDFS_HIVE_DATATYPES_DIR"  "$G_SESSION_DATE"     "$G_SUBMIT_MASTER"			\
	"$G_DEPLOY_MODE"      "$G_MAX_RETRY_THRESHOLD"     "$G_MUST_HAVE_HIVE_DATATYPES"    "$G_INCLUDE_SKIP_DATA_IN_OUTPUT"	\
	"$G_BIG_TABLE_ROWS_TO_SWITCH"   "$G_BIG_TABLE_TABLECOLS_BATCH_SIZE"   "$G_BIG_TABLE_STATCOLS_BATCH_SIZE"    $G_PROGRAM_TYPE  "$G_BATCH_ID" "\t";
G_JOB_STEPS_COMPLETED_SO_FAR="STEP11";	


#If this script gets to this point (ie, no failure) it means ALL Split_Files (for the original G_INPUT_LIST) have completed sucessfully


# This will not move any file due to change to logic.
# BEFORE:  All split files remaing until all are finished
# NOW:     Move split file out as soon as it finished to save time
# Even with checkpoint, it takes about 1-2 minutes per table just to check all associated with Checkpoints.  Too long!
printf "\n\nSTEP 12: Move_OS_and_HDFS_SplitFiles_To_Archive:\t\t$(GetCurrTime)\n"
Move_OS_and_HDFS_SplitFiles_To_Archive	"$G_TORUN_DIR"                   "$G_ARCHIVE_LINUX_DIR"  "$G_HDFS_INPUT_DIR" \
					"$G_HDFS_SPLITFILE_ARCHIVE_DIR"	 "$G_SESSION_DATE"       "\t";
G_JOB_STEPS_COMPLETED_SO_FAR="STEP12";


printf "\n\nSTEP 13: Remove entire HDFS ROOT Checkpoint Dir of this Input file: $G_HDFS_CHECKPOINT_ROOT_DIR:\n"
printf "\t(At this point All SplitFiles of the original Input file have been process SUCCESSFULLY. \n"
printf "\t(Hence no longer need any Checkpoint data.  Can now safely RECURSIVELY remove from ROOT dir (G_HDFS_CHECKPOINT_ROOT_DIR)\n"
hadoop fs -rm -R ${G_HDFS_CHECKPOINT_ROOT_DIR}
RC=$?
if [[ $RC -ne 0 ]]
then
	printf "\n\n\n\***** ERROR: Cannot recursively remove HDFS CHECKPOINT ROOTDIR: ${G_HDFS_CHECKPOINT_ROOT_DIR};  RC=$RC;\t\t$(GetCurrTime)\n\n\n";
	exit -1;
else
	printf "\tRC=$RC\n";
fi;
G_JOB_STEPS_COMPLETED_SO_FAR="STEP13";


#------------------------------------------------------------------------------------------------------------------------------------------------------------------
# These 2 steps are no longer used as we now use MicroService to read from Oracle Queue table automatically:
#	export G_HDFS_ORIGINAL_INPUTFILE_ARCHIVE_DIR="${G_HDFS_ARCHIVE_DIR}/${G_PROJECT_DIRNAME}";
#	Make_1_HDFS_Dir	$G_HDFS_ORIGINAL_INPUTFILE_ARCHIVE_DIR  "ARCHIVE DIR for ORIGINAL INPUT File"  "\t"
#
#	printf "\n\nSTEP 14: Move ORIGINAL HDFS input file (\"$G_INPUT_LIST\") to HDFS Archive Dir ($G_HDFS_ORIGINAL_INPUTFILE_ARCHIVE_DIR):\t\t$(GetCurrTime)\n";
#	MoveHDFSFileToHDFSArchive $G_INPUT_LIST "$G_HDFS_INPUT_DIR"   "$G_HDFS_ORIGINAL_INPUTFILE_ARCHIVE_DIR"   "$G_SESSION_DATE"   "\t"
	# The original HDFS input file might be split into mulitple input files (based on NumExecutors)
	# Any of these new input files might Succeed or Fail
	# So, for simplicity, just move this original HDFS input file to HDFS Arhive 
	# Do this move as the FINAL step so if anything goes wrong, this input file still available at HDFS for use
#------------------------------------------------------------------------------------------------------------------------------------------------------------------

printf "\n\nSTEP 14: Remove all OS logs older than $G_DAYS_TO_REMOVE days (under: $G_ROOT_DIR):\t\t$(GetCurrTime)\n";
RemoveOldFiles $G_DAYS_TO_REMOVE  $G_ROOT_DIR   "${G_ROOT_DIR}/LOGS"   "\t"
G_JOB_STEPS_COMPLETED_SO_FAR="STEP14";

G_JOB_STEPS_COMPLETED_SO_FAR="FINAL";	

touch $G_ROOT_DIR;		#Change the date of this Dir. Up till now only the subdirs under this dir have their dates changed.

#This last step must match variable G_LAST_STEP_TO_COMPLETE for job to be considered SUCCESS
G_JOB_STATUS="SUCCESS";	
G_JOB_STATUS_COMPLEMENT_STR=""
sleep 2;
exit  0;	# Use exit to invoke function Func_Trap_INT


#   Configurable number of bkg processes.  2 while loops
#   Global Flag to stop SplitFile gracefully
#   Utility to display outstanding tables to run
# Wait for BKG:  Depeneding on how many tables/sizes? that way it does not wait too long, but max 5 mins per loop?  or use use 2 mins per loop
# YARN LOG: re-caculate every loop so to get the latest imediately without having to wait to the end 
# LOOP BACK:  at some point it needs to quit to allow new installation of code.  The Stop feature will do it
#
# Ability to specify a priority when call GETWORKUNIT.  This way i can request that priority 
#
#

# RM columns should be the controlling, not Hive columnns

# Yes but we need to add COL_AVG_LEN to Basic Stats.

