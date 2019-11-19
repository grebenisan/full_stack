#-----------------------------------------------------------------------------------------------
# Purpose: Calculate 9 statistics for each column of a given table
#-----------------------------------------------------------------------------------------------
# Authors:
# Jun-01-2018	v2	Dan Grebenisan 			
#	- NumExecutors based on HDFS filesize (not Analyze output)
#	- All Column statistics of 1 input file go into 1 output JSON file
#	- Checkpoint per HDFS JSON file 
#
# Oct-20-2017	v1	Surendar Reddy Pandilla
#-----------------------------------------------------------------------------------------------

from pyspark.sql import SparkSession, functions, Row;	# Needed for: df.show();
from pyspark.sql.functions import col, asc;		# For checkpoint DataFrame

import sys;
import time             as TIME1;
from   time import time as TIME2;	# This package is different from  just "import time" package above.
					# This does NOT have function   strftime(). So alias as TIME2
import subprocess;
import math;
import re;
from   pyspark.sql.types import *	# Need this to prevent: name 'StructType' is not defined

####### from   pyspark.sql       import Row;	# Needed for: df.show();
					#https://stackoverflow.com/questions/1398674/display-the-time-in-a-different-time-zone
from datetime import datetime, timedelta;
from pytz import timezone;		#Must set that variable when calling Invoke_Pyspark from shell script
import pytz;
import gc;

import requests, json;			# For MicroServices


# Declare this Global class to avoid: EXCEPTION_HIVE_TYPE_NOTFOUND_IN_ROADMASTER) local variable 'EXCEPTION_HIVE_TYPE_NOTFOUND_IN_ROADMASTER' referenced before assignment)

class EXCEPTION_TABLE_NOTFOUND_IN_HIVE				(Exception):	pass;
class EXCEPTION_HIVE_COLTYPE_NOTFOUND_IN_RM			(Exception):	pass;
class EXCEPTION_HIVE_TABLE_NOTFOUND_IN_ROADMASTER	(Exception):	pass;
class EXCEPTION_COLUMNS_COUNT_MISMATCH				(Exception):	pass;
class EXCEPTION_MICROSERVICE_UPDATE_QUEUE			(Exception):	pass;
class EXCEPTION_MICROSERVICE_WRITE_OUTPUT			(Exception):	pass;
class EXCEPTION_TYPECAST_MAP						(Exception):	pass;
class EXCEPTION_CANNOT_GET_AUTHENTICATION_KEY		(Exception):	pass;
class EXCEPTION_OUTBOUND_MSG_FAIL					(Exception):	pass;

#TYPECAST_MAP =  {
#	# The index of this map should match Type derived from RoadMaster MicroService 
#	# Key:    RowsCount, UniqueCount, NullCount, MaxValue, MinValue, MeanValue, StdDevVal,  MaxLen,  MinLen
#	# These True/False values dictates how the SQL statement is built.
#	"STRING":	(True,      True,        True,      True,     True,     False,     False,     True,   True  ),
#	"CHAR":		(True,      True,        True,      True,     True,     False,     False,     True,   True  ),
#	"VARCHAR":	(True,      True,        True,      True,     True,     False,     False,     True,   True  ),
#	"VARCHAR2":	(True,      True,        True,      True,     True,     False,     False,     True,   True  ),
#	"NUMBER":	(True,      True,        True,      True,     True,     True,      True,      False,  False ),
#	"INT":		(True,      True,        True,      True,     True,     True,      True,      False,  False ),
#	"SMALLINT":	(True,      True,        True,      True,     True,     True,      True,      False,  False ),
#	"TINYINT":	(True,      True,        True,      True,     True,     True,      True,      False,  False ),
#	"BIGINT":	(True,      True,        True,      True,     True,     True,      True,      False,  False ),
#	"FLOAT":	(True,      True,        True,      True,     True,     True,      True,      False,  False ),
#	"DOUBLE":	(True,      True,        True,      True,     True,     True,      True,      False,  False ),
#	"DATE":		(True,      True,        True,      True,     True,     False,     False,     False,  False ),
#	"TIMESTAMP":(True,      True,        True,      True,     True,     False,     False,     False,  False ),
#	"CLOB":		(True,      False,       True,      False,    False,    False,     False,     True,   True  ),
#	"BLOB":		(True,      False,       False,     False,    False,    False,     False,     False,  False )
#	}



	# Turn all these to TRUE because for some column it might make sense
	# Ex:  VIN is a string.  Even MaxValue of String does not make sense, but for VIN, it might.
TYPECAST_MAP =  {
	# The index of this map should match Type derived from RoadMaster MicroService 
	# Key:    RowsCount, UniqueCount, NullCount, MaxValue, MinValue, MeanValue, StdDevVal,  MaxLen,  MinLen
	# These True/False values dictates how the SQL statement is built.
	"STRING":	(True,      True,        True,      True,     True,     True,      True,      True,   True ),
	"CHAR":		(True,      True,        True,      True,     True,     True,      True,      True,   True ),
	"VARCHAR":	(True,      True,        True,      True,     True,     True,      True,      True,   True ),
	"VARCHAR2":	(True,      True,        True,      True,     True,     True,      True,      True,   True ),
	"NUMBER":	(True,      True,        True,      True,     True,     True,      True,      True,   True ),
	"INT":		(True,      True,        True,      True,     True,     True,      True,      True,   True ),
	"SMALLINT":	(True,      True,        True,      True,     True,     True,      True,      True,   True ),
	"TINYINT":	(True,      True,        True,      True,     True,     True,      True,      True,   True ),
	"BIGINT":	(True,      True,        True,      True,     True,     True,      True,      True,   True ),
	"FLOAT":	(True,      True,        True,      True,     True,     True,      True,      True,   True ),
	"DOUBLE":	(True,      True,        True,      True,     True,     True,      True,      True,   True ),
	"DATE":		(True,      True,        True,      True,     True,     True,      False,     True,   True ),
	"TIMESTAMP":	(True,      True,        True,      True,     True,     True,      False,     True,   True ),
	"CLOB":		(True,      True,        True,      True,     True,     True,      True,      True,   True ),
	"BLOB":		(True,      True,        True,      True,     True,     True,      False,     True,   True )
}
	# Cannot perorm StdDev on TimeStamp/Date due to this error:
	#	"cannot resolve 'stddev_samp(CAST(temp_df_table.`CREATION_DATE` AS TIMESTAMP))' due to data type
	#	mismatch: argument 1 requires double type, however, 'CAST(temp_df_table.`CREATION_DATE` AS TIMESTAMP)' is of timestamp type



#---------------------------------------------------------------------------------------------------------------
def ChooseTableColumnBatchSize (tableRows):
	if    tableRows >= 10000000000:	# 10,000,000,000  = 10  billion rows
		cols=5;
	elif  tableRows >=  1000000000:	# 1,000,000,000   = 1   billion rows
		cols=10;
	elif  tableRows >=   700000000:	#   700,000,000   = 500 million rows
		cols=15;
	elif  tableRows >=   500000000:	#   500,000,000   = 500 million rows
		cols=20;
	elif  tableRows >=   300000000:	#   300,000,000   = 500 million rows
		cols=25;
	elif  tableRows >=   100000000:	#   100,000,000   = 100 million rows
		cols=30;
	elif  tableRows >=    50000000:	#    70,000,000   =  10 million rows
		cols=35;
	elif  tableRows >=    50000000:	#    50,000,000   =  50 million rows
		cols=40;
	elif  tableRows >=    30000000:	#    30,000,000   =  30 million rows
		cols=45;
	elif  tableRows >=    10000000:	#    10,000,000   =  10 million rows
		cols=50;
	elif  tableRows >=     5000000:	#     5,000,000   =   5 million rows
		cols=60;
	elif  tableRows >=     1000000:	#     1,000,000   =   1 million rows
		cols=70;
	elif  tableRows >=      500000:	#       500,000   =   500 K rows
		cols=80;
	elif  tableRows >=      100000:	#       100,000   =   100 K rows
		cols=100;
	else:
		cols=200;

	return cols;


#---------------------------------------------------------------------------------------------------------------
def GetHIVETypeCastMapDecision(key, sourceType, index, msg = "", leadingTabs=""):
	itab1=leadingTabs;
	itab2=itab1 + "\t";
	try:
		decision= TYPECAST_MAP [key.upper()][index];	# Pick out value at location: key[]index[]; Value= True or False
	except:
		# raise EXCEPTION_TYPECAST_MAP ("EXCEPTION_TYPECAST_MAP: Bad Input values: Key=" + Key + "; Index=" + str(index))
		# print (	"\n" +	itab1 +	"==> GetHIVETypeCastMapDecision");
		print (itab1 +	"Func " + msg.ljust(19) + ":  WARNING: No Value for Key: \"" +key +"\"  at Index=\"" + str(index) + "\"", end='')
		# print (			itab1 +	"<== GetHIVETypeCastMapDecision\n");

		if  key=="N/A" and sourceType=="CLOB":
			decision= TYPECAST_MAP ["CLOB"][index];	# Pick out value at location: key[]index[]; Value= True or False
			print ( "\tSpecial Case: Combo: (N/A and CLOB) ==> Return " + str(decision))
			# Reason: in RoadMaster table it maps CLOB to N/A.
			# But for our profile logic, we still want to profile CLOB as String using a few aggregate functions
			# (not as many as String)
			return decision;
		else:
			print ("==> Return FALSE")
			return False;
	return decision
#---------------------------------------------------------------------------------------------------------------
def UniqueCountFuncCall (hiveType, sourceType, colExpr, colNum, leadingTabs=""):
	# Example: colExpr  could be:
	#	1) just colum Name:             "DeptNum"
	#	2) an   column expression:	"cast(DeptNum as BigInt)"

	itab1=leadingTabs;
	itab2=itab1 + "\t";
	itab3=itab2 + "\t";

	if GetHIVETypeCastMapDecision (hiveType, sourceType, 1, "UniqueCountFuncCall", itab1):
				#Start with index 1 not 0  as 0 is RowCount, which is calculated elsewhere
		#s=" coalesce(cast(count(distinct("  + colExpr + ")) as string), '' )  as UniqCount" + str(colNum) + ",\n"; # ex: UniqCount5
		s=" coalesce(cast(count(distinct("  + colExpr + ")) as string), '' )\tas UniqCount"  + str(colNum) + ",\t-- " + sourceType +"\n";
	else:
		#s=" \"\" as UniqCount" + str(colNum) + ",\n";
		s=" \"\"\tas UniqCount" + str(colNum) + ",\t-- " + sourceType +"\n";
	return s;
#---------------------------------------------------------------------------------------------------------------
def NullCountFuncCall (hiveType, sourceType, colExpr, colNum, leadingTabs=""):
	itab1=leadingTabs;
	if GetHIVETypeCastMapDecision (hiveType, sourceType, 2, "NullCountFuncCall", itab1):
		#s="     coalesce (cast(sum(case when " + colExpr + " is null then 1 else 0 end) as string), '') as NullCount" + str(colNum) + ",\n";
		s="     coalesce (cast(sum(case when " + colExpr + " is null then 1 else 0 end) as string), '')\tas NullCount" + str(colNum) + ",\t-- " + sourceType +"\n";
	else:
		#s="     \"\" as NullCount" + str(colNum) + ",\n";
		s="     \"\"\tas UniqCount" + str(colNum) + ",\t-- " + sourceType +"\n";
	return s;
#---------------------------------------------------------------------------------------------------------------
def MaxValFuncCall (hiveType, sourceType, colExpr, colNum, leadingTabs=""):
	itab1=leadingTabs;
	if GetHIVETypeCastMapDecision (hiveType, sourceType, 3, "MaxValFuncCall", itab1):
		#s="     coalesce (cast(max(" + colExpr + ")  as string), '' ) as MaxVal"   + str(colNum) + ",\n"
		s="     coalesce (cast(max(" + colExpr + ")  as string), '' )\tas MaxVal"   + str(colNum) + ",\t-- " + sourceType +"\n";
	else:
		#s="     \"\" as MaxVal"    + str(colNum) + ",\n";
		s="     \"\"\tas UniqCount" + str(colNum) + ",\t-- " + sourceType +"\n";
	return s;
#---------------------------------------------------------------------------------------------------------------
def MinValFuncCall (hiveType, sourceType, colExpr, colNum, leadingTabs=""):
	itab1=leadingTabs;
	if GetHIVETypeCastMapDecision (hiveType, sourceType, 4, "MinValFuncCall", itab1):
		#s="     coalesce (cast(min(" + colExpr + ")  as string), '' ) as MaxVal"   + str(colNum) + ",\n";
		s="     coalesce (cast(min(" + colExpr + ")  as string), '' )\tas MaxVal"   + str(colNum) + ",\t-- " + sourceType +"\n";
	else:
		#s="     \"\" as MaxVal" + str(colNum) + ",\n";
		s="     \"\"\tas MaxVal" + str(colNum) + ",\t-- " + sourceType +"\n";
	return s;
#---------------------------------------------------------------------------------------------------------------
def MeanValFuncCall (hiveType, sourceType, colExpr, colNum, leadingTabs=""):
	itab1=leadingTabs;
	if GetHIVETypeCastMapDecision (hiveType, sourceType, 5, "MeanValFuncCall", itab1):
		#s="     coalesce (cast(avg("           + colExpr + ")  as string), '' ) as MeanVal"  + str(colNum) + ",\n";
		s="     coalesce (cast(avg("           + colExpr + ")  as string), '' )\tas MeanVal"  + str(colNum) + ",\t-- " + sourceType +"\n";
	else:
		#s="     \"\" as MeanVal" + str(colNum) + ",\n";
		s="     \"\"\tas MeanVal" + str(colNum) + ",\t-- " + sourceType +"\n";
	return s;
#---------------------------------------------------------------------------------------------------------------
def StdDevFuncCall (hiveType, sourceType, colExpr, colNum, leadingTabs=""):
	itab1=leadingTabs;
	if GetHIVETypeCastMapDecision (hiveType, sourceType, 6, "StdDevFuncCall", itab1):
		#s="     coalesce (cast(stddev("        + colExpr + ")  as string), '' ) as StdDev"   + str(colNum) + ",\n";
		s="     coalesce (cast(stddev("        + colExpr + ")  as string), '' )\tas StdDev"   + str(colNum) + ",\t-- " + sourceType +"\n";
	else:
		#s="     \"\" as StdDev" + str(colNum) + ",\n";
		s="     \"\"\tas StdDev" + str(colNum) + ",\t-- " + sourceType +"\n";
	return s;
#---------------------------------------------------------------------------------------------------------------
def MaxLenFuncCall (hiveType, sourceType, colExpr, colNum, leadingTabs=""):
	itab1=leadingTabs;
	if GetHIVETypeCastMapDecision (hiveType, sourceType, 7, "MaxLenFuncCall", itab1):
		#s="     coalesce (cast(max(length("    + colExpr + ")) as string), '' ) as MaxLen"   + str(colNum) + ",\n"
		s="     coalesce (cast(max(length("    + colExpr + ")) as string), '' )\tas MaxLen"   + str(colNum) + ",\t-- " + sourceType +"\n";
	else:
		#s="     \"\" as MaxLen" + str(colNum) + ",\n";
		s="     \"\"\tas MaxLen" + str(colNum) + ",\t-- " + sourceType +"\n";
	return s;
#---------------------------------------------------------------------------------------------------------------
def MinLenFuncCall (hiveType, sourceType, colExpr, colNum, leadingTabs=""):
	itab1=leadingTabs;
	if GetHIVETypeCastMapDecision (hiveType, sourceType, 8, "MinLenFuncCall", itab1):
		#s="     coalesce (cast(min(length("    + colExpr + ")) as string), '' ) as MinLen"   + str(colNum) + "\n"	# No last ',' on this LAST column
		s="     coalesce (cast(min(length("    + colExpr + ")) as string), '' )\tas MinLen"   + str(colNum) + "\t-- " + sourceType +"\n";
	else:
		#s="     \"\" as MinLen" + str(colNum) + "\n";	# No ',' on this LAST column
		s="     \"\"\tas MinLen" + str(colNum) + "\t-- " + sourceType +"\n";
	return s;
#---------------------------------------------------------------------------------------------------------------
def BuildSqlFor1Column (hiveType, sourceType, colExpr, colNum, sql,leadingTabs=""):
	itab1=leadingTabs;
	itab2=itab1 + "\t";
	# print ("BuildSqlFor1Column: " + str(colNum) + "  colExpr=" + colExpr);
	s1="";
	s2="";
	s3="";
	s4="";
	s5="";
	s6="";
	s7="";
	s8="";
	s9="";

	s1 = UniqueCountFuncCall (hiveType, sourceType, colExpr, colNum, itab1);
	s2 = NullCountFuncCall   (hiveType, sourceType, colExpr, colNum, itab1);
	s3 = MaxValFuncCall      (hiveType, sourceType, colExpr, colNum, itab1);
	s4 = MinValFuncCall      (hiveType, sourceType, colExpr, colNum, itab1);
	s5 = MeanValFuncCall     (hiveType, sourceType, colExpr, colNum, itab1);
	s6 = StdDevFuncCall      (hiveType, sourceType, colExpr, colNum, itab1);
	s7 = MaxLenFuncCall      (hiveType, sourceType, colExpr, colNum, itab1);
	s8 = MinLenFuncCall      (hiveType, sourceType, colExpr, colNum, itab1);

		#Keep adding new columns to the end of previous SQL string
	newsql = sql + ",\n"+ s1 + s2 + s3 + s4 + s5 + s6 + s7 + s8 + s9;
	return newsql;
#---------------------------------------------------------------------------------------------------------------
def getLocalTimeOfCity (CITY = "AUSTIN", TIMEZONE = "US/Central", leadingTabs=""):
	#This use package  pytz.  
	#Must set that variable when calling Invoke_Pyspark from shell script from the spark-submit command
	return "("+ CITY + "_TIME: " + datetime.now(pytz.timezone(TIMEZONE)).strftime('%Y/%m/%d %H:%M:%S') +")"
#---------------------------------------------------------------------------------------------------------------
def Increment_SUCCESS_COUNTS (LIST_OF_TABLE_STATUS_COUNTS):
	LIST_OF_TABLE_STATUS_COUNTS[0] += 1;

def Increment_FAIL_COUNTS (LIST_OF_TABLE_STATUS_COUNTS):
	LIST_OF_TABLE_STATUS_COUNTS[1] += 1;

def Increment_HIVE_TABLE_EMPTY_COUNTS (LIST_OF_TABLE_STATUS_COUNTS):
	LIST_OF_TABLE_STATUS_COUNTS[2] += 1;

def Increment_TABLENOTFOUND_IN_HIVE_COUNTS (LIST_OF_TABLE_STATUS_COUNTS):
	LIST_OF_TABLE_STATUS_COUNTS[3] += 1;

def Increment_TABLENOTFOUND_IN_RM_COUNTS (LIST_OF_TABLE_STATUS_COUNTS):
	LIST_OF_TABLE_STATUS_COUNTS[4] += 1;
#---------------------------------------------------------------------------------------------------------------
#--{
def Create_DataFrame_Over_HDFS_Dir (spark, hdfsDIR, dfSchema, uniqueDFname, msg, topNrows=0, debugVerbose=False, leadingTabs="") :
	itab1=leadingTabs;
	itab2=itab1 + "\t";
	itab3=itab2 + "\t";

	DFStartTime = TIME2();
	print ("\n"+ itab1 + "==> Function Create_DataFrame_Over_HDFS_Dir;\t\t" + getLocalTimeOfCity() );
	print (itab2 + "hdfsDIR      = " + hdfsDIR )
	print (itab2 + "uniqueDFname = " + uniqueDFname);
	print (itab2 + "msg          = " + msg);
	print (itab2 + "DFStartTime  = " + DFStartTime);
	try:
			# DF =spark.read.json(hdfsDIR, dfSchema );
			# 	NOTE: read.json()  will NOT raise exception if directory is empty.  But it's more difficult to perform
			#	ROWNUM() OVER () ... as the columns are nested within the string.
			#	So for these operations its better to use normal delimited files instead
			#	But read operations on delimited file raise exception when the Path cannot find files
		DF = spark.read.option("delimiter", u'\u001c').schema(dfSchema).csv(hdfsDIR + "/*");

			# Although read() can read multiple files, it only Works if there are files to read.  Otherwise it raises exception:
			# AnalysisException: 'Path does not exist: hdfs://edwbidevwar/DEV/EDW/.../../*;'
			# Must use Exception to catch it.
	except Exception as exp1:
		pattern=re.compile("^.*Path does not exist.*$")
		if pattern.search(str(exp1)):
			print("\n"+ itab2 + "WARNING: No Data Files found at this HDFS Dir\n\t\tThis is NORMAL ==> Create new EMPTY DataFrame for return:")
			DF = spark.createDataFrame (spark.sparkContext.emptyRDD(), dfSchema)
		else:
			print(itab2+ "EXCEPTION exp1: " + str(exp1) +"\n")
			raise;

	print ("\n" + itab2 + uniqueDFname + " DF RowsCount = " + str(DF.count())  + ";  Took: "  + \
		ElapsedTimeStrFrom(DFStartTime, TIME2(), debugVerbose) + " seconds\n");	# Count() is ACTION 
	DF.createOrReplaceTempView (uniqueDFname);

	if debugVerbose:  DisplayTopNrows (DF, topNrows, itab2, uniqueDFname);

	print (itab1 + "<== Function Create_DataFrame_Over_HDFS_Dir;\t\t" + getLocalTimeOfCity() );
	return DF
#--}
#---------------------------------------------------------------------------------------------------------------
#--{
def Create_CHECKPOINT_Flag  (	tableNum, schema, table, tableStartTime, orig_file_name,		\
				file_name, status, CHECKPOINT_DIR, rowsCountStr, columnsCountStr,	\
				sessionID, debugVerbose=False, leadingTabs="") :
	itab1=leadingTabs;
	itab2=itab1 + "\t";
	itab3=itab2 + "\t";

	if debugVerbose:
		print (itab1 + "==> Function Create_CHECKPOINT_Flag;\t\t" + getLocalTimeOfCity() );
		print (itab2 + "Flag Type	= " + status )
		print (itab2 + "Schema		= " + schema )
		print (itab2 + "Table		= " + table )
		print (itab2 + "CHECKPOINT_DIR	= " + CHECKPOINT_DIR + "\n")

	try:	#--{
		print(itab2 + "===== Create 1 '" + status + "' CHECKPOINT flag" );
		if debugVerbose: print(itab2 + "(at : " + CHECKPOINT_DIR + ")");
		tableCompleteTime = TIME1.strftime("%Y%m%d %H:%M:%S")

		checkpoint1Row    = [ (	tableNum, schema, table, tableStartTime, tableCompleteTime, orig_file_name,	\
					file_name, status, rowsCountStr, columnsCountStr, sessionID ) ]
					# Make sure these columns matches:  dfSchemaForCheckpoint
		if debugVerbose: print (itab2 + "===== Row Value = " + str(checkpoint1Row) )
			#print ("\t[0]=" + checkpoint1Row[0])

		# Use this method to create Field-Delimited file:
		checkpoint1RDD   = spark.sparkContext.parallelize(checkpoint1Row, 1).map (	\
					lambda x: Row(x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10] ))
		checkpoint1RowDF = spark.createDataFrame(checkpoint1RDD, dfSchemaForCheckpoint)
		checkpoint1RowDF.write.mode("append").option("delimiter", u'\u001c').csv( CHECKPOINT_DIR )

		#if debugVerbose: checkpoint1RowDF.show(1,False);	
		checkpoint1RowDF.show(1,False);
		del checkpoint1RowDF

	except Exception as expCrCKPFlag:
		DisplayExceptionMessage (expCrCKPFlag, tableNum, schema, table, "expCrCKPFlag", "Create_CHECKPOINT_Flag()", itab2)
		print (itab1 + "<== Function Create_CHECKPOINT_Flag;\t\t" + getLocalTimeOfCity() );
		raise expCrCKPFlag;
	# --} try: Create CHECKPOINT
	if debugVerbose: print (itab1 + "<== Function Create_CHECKPOINT_Flag;\t\t" + getLocalTimeOfCity() );
#--}
#---------------------------------------------------------------------------------------------------------------
#--{
def Create_DataFrame_From_DWColumns_List (DWColumsList, dfSchemaForDWColumns, df_name, topNrows, debugVerbose=False, leadingTabs=""):
	itab1=leadingTabs;
	itab2=itab1 + "\t";
	print (itab1 + "==> Function Create_DataFrame_From_DWColumns_List;\t\t" + getLocalTimeOfCity() );

	if debugVerbose: print ("DWColumsList="+str(DWColumsList));

	try:	#--{ try
		#rdd = spark.sparkContext.parallelize(DWColumsList, 1).map (lambda x: Row(x[0]));
		rdd = spark.sparkContext.parallelize(DWColumsList, 1).map (lambda x: Row(x));
					# "x" represents a element in the input list DWColumsList

		print (itab2 + "rdd.count()   = " + str(rdd.count()) + " rows")	#1 row for each column
		newDF=spark.createDataFrame (rdd, dfSchemaForDWColumns)
		print (itab2 + "newDF.count() = " + str(newDF.count()) + " rows")
		if rdd.count() != newDF.count():
			print("\n\n\n***** ERROR: Counts mismatch!\n\n\n")
			raise;

		#DisplayTopNrows (newDF, topNrows, itab2, df_name)
		DisplayTopNrows (newDF, -1, itab2, df_name)

	except Exception as CrDFFromDWColsExp:
		print("\n\n\n***** ERROR: (Exception CrDFFromDWColsExp) Cannot Create DataFrame for: \"" +	\
			df_name + "\";   " + getLocalTimeOfCity()  )
		print(itab2 + "Exception Message: " + str(CrDFFromDWColsExp))
		raise;
	# --} try: Create new DataFrame of all columns for this 1 table
	print (itab1 + "<== Function Create_DataFrame_From_DWColumns_List;\t\t" + getLocalTimeOfCity() );
	return newDF;
#--}
#---------------------------------------------------------------------------------------------------------------
#--{
def Create_DataFrame_From_List (statsForALLCol, dfSchemaForOutputTable, df_name, topNrows, debugVerbose=False, leadingTabs=""):
	itab1=leadingTabs;
	itab2=itab1 + "\t";

	if debugVerbose:
		print (itab1 + "==> Function Create_DataFrame_From_List;\t\t" + getLocalTimeOfCity() );
		print (itab2 + str(dfSchemaForOutputTable));
		print (itab2 + "df_name        = " + df_name);
		print (itab2 + "topNrows       = " + str(topNrows));
		print (itab2 + "statsForALLCol = " + str(statsForALLCol));

		#print (itab2 + "dfSchemaForOutputTable:")
		#[ print (f.dataType) for f in dfSchemaForOutputTable.fields ];

	try:	#--{ 
                #                                                    | 1 = 1 partition only
                #                                                    v
		rdd = spark.sparkContext.parallelize(statsForALLCol, 1).map (	\
			lambda x: Row(x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8],x[9],x[10],x[11], x[12], x[13],x[14], x[15], x[16] ));
		newDF=spark.createDataFrame (rdd, dfSchemaForOutputTable)

		if rdd.count() != newDF.count():
			print("\n\n\n***** ERROR: Counts mismatch!\n\n\n")
			raise;

		#This is the output.  Always print it out regardless of debugVerbose
		print (itab2 + "rdd.count()   = " + str(rdd.count()) + " rows;  newDF.count() = " + str(newDF.count()) + " rows")
		DisplayTopNrows (newDF, topNrows, itab2, df_name)

	except Exception as CrDFFromListExp:
		print("\n\n\n***** ERROR: (Exception CrDFFromListExp) Cannot Create Final DataFrame for Table: \"" + \
			df_name + "\";   " + getLocalTimeOfCity()  )

		print(itab2 + "Exception Message 1: " + str(CrDFFromListExp))
		print(itab2 + "Exception Message 2: " + str(sys.exc_info()[0]) + "\n\n\n" );
		raise;
	# --} try: Create new DataFrame of all columns for this 1 table

	if debugVerbose: print (itab1 + "<== Function Create_DataFrame_From_List;\t\t" + getLocalTimeOfCity() + "\n");
	return newDF;
#--}
#---------------------------------------------------------------------------------------------------------------
#--{
def Create_ROWSDATA_in_HDFS (newDF, HDFS_DIR,  tableNum, schema, table, debugVerbose=False, topNrows=0, leadingTabs=""):
	itab1=leadingTabs;
	itab2=itab1 + "\t";
	itab3=itab2 + "\t";

	print (itab1 + "==> Function Create_ROWSDATA_in_HDFS;\t\t" + getLocalTimeOfCity() );
	if debugVerbose:
		print (itab2 + "op_dir         = " + op_dir);
		print (itab2 + "HDFS_DIR       = " + HDFS_DIR);
		print (itab2 + "file_name      = " + file_name);
		print (itab2 + "orig_file_name = " + orig_file_name);
		print (itab2 + "topNrows       = " + str(topNrows) +"\t\t(for display only)");

			# Similar to def Create_Output_As_JSON_Row .  See it for explanation
	try:	#--{ try
		startTime = TIME2();
		newDF.write.mode("append").option("delimiter", u'\u001c').csv( HDFS_DIR )
		print (	itab2 + "===== Step 1/1: Write to ROWSDATA HDFS file"	+	\
						"; RowsCount: " + str(newDF.count())			+	\
						"; Took: "      + ElapsedTimeStrFrom (startTime, TIME2(), debugVerbose)	+ "; "+ HDFS_DIR );

	except Exception as expCrROWSDATA:
		DisplayExceptionMessage (expCrROWSDATA, tableNum, schema, table,"expCrROWSDATA", "Create_ROWSDATA_in_HDFS()", itab2)
		raise;
	# --} try: Create new DataFrame of all columns for this 1 table

	print (itab1 + "<== Function Create_ROWSDATA_in_HDFS;\t" + getLocalTimeOfCity() );
#--}
#---------------------------------------------------------------------------------------------------------------
def ElapsedTimeStrFrom (StartTimeT2, EndTimeT2=TIME2(), debugVerbose=False, leadingTabs=""):
	# Display TimeElapsed from input-parameter to EndTimeT2 , or till now if not passed in.
	# Format:  hrs mins secs  (with plural as needed)
	# Only display unit if it has a value.  ie, if hr is 0, then NOT display 0 hrs

	#EndTimeT2=TIME2()
	itab1=leadingTabs;
	itab2=itab1 + "\t";

	# elapsedInt=int(math.ceil(TIME2() - StartTimeT2) ) ;	
	elapsedInt=int(math.ceil(EndTimeT2 - StartTimeT2) ) ;	
	elapsedStr=str(elapsedInt)
	hrsInt=elapsedInt//3600;	# hours;    integer division
	minsInt=elapsedInt//60;		# minutes;  integer division
	secsInt=elapsedInt%60;		# seconds;  modulus

	hrsStr  = "hr";
	minsStr = "min";
	secsStr = "sec";

	if (hrsInt  > 1): hrsStr="hrs";
	if (minsInt > 1): minsStr="mins";
	if (secsInt > 1): secsStr="secs";

	finalStr="";
	if (hrsInt   >= 1 ): finalStr = str(hrsInt)  + " " + hrsStr;
	if (finalStr != ""): finalStr = finalStr + " ";
	if (minsInt  >= 1 ): finalStr = finalStr + str(minsInt) + " " + minsStr;
	if (finalStr != ""): finalStr = finalStr + " ";
	if (secsInt  >= 1 ): finalStr = finalStr + str(secsInt) + " " + secsStr;

	#if debugVerbose:
	#	print ("\n");
	#	print ("StartTimeT2="+str(StartTimeT2) + "; EndTimeT2="+str(EndTimeT2)+"; ElapsedInt="+str(elapsedInt)+";  ElapsedStr="  + elapsedStr);
	#	print ("hrsInt="     +str(hrsInt)      + "; minsInt="  +str(minsInt)  +"; secsInt="   +str(secsInt)   + "  ==> finalStr="+ finalStr);
	#	print ("\n");

	return finalStr;
#---------------------------------------------------------------------------------------------------------------
def DisplayExceptionMessage (exp, tableNum, schema, table, expName, funcName, leadingTabs=""):
	itab1=leadingTabs;
	itab2=itab1 + "\t";
	itab3=itab2 + "\t";
	print("\n\n" + itab1 + "********** ERROR: Exception at Table #" + str(tableNum) + ": " + schema + "." + table  )
	print(itab2+ "(Exception "+ expName+": " + str(exp) + ")" )
	#print(itab2+ "(Exception TYPE NAME: "+ type(exp).__name__ + ")\n" )
#---------------------------------------------------------------------------------------------------------------
def CreateSchemaForDWColumns(leadingTabs=""):
	itab1=leadingTabs;
	itab2=itab1 + "\t";
	dfSchemaForDWColumns = StructType     (						\
			[                                                               \
				StructField("Column_Nm",      StringType(),  True)	\
			]                                                               \
	)
			#Column Name should match dfCheckpointHiveColumnsType so I could user LEFT_ANTI in DataFrame
	return dfSchemaForDWColumns; 
#---------------------------------------------------------------------------------------------------------------
def CreateSchemaForOutputTable(leadingTabs=""):
	itab1=leadingTabs;
	itab2=itab1 + "\t";

	#This is for Original JSON fieldnames:  Keep case-sensitive for colunns as in original code
	dfSchemaForOutputTable = StructType     (					\
			[                                                               \
				StructField("Table_Num",         StringType(),  True),  \
				StructField("Schema_Name",       StringType(),  True),  \
				StructField("table_name",        StringType(),  True),  \
				StructField("columname",         StringType(),  True),	\
				StructField("Total_count",       StringType(),  True),  \
				StructField("unique_count",      StringType(),  True),  \
				StructField("null_count",        StringType(),  True),  \
				StructField("max_value",         StringType(),  True),  \
				StructField("min_value",         StringType(),  True),  \
				StructField("mean_value",        StringType(),  True),  \
				StructField("std_dev",           StringType(),  True),  \
				StructField("max_length",        StringType(),  True),  \
				StructField("min_length",        StringType(),  True),  \
				StructField("ins_gmt_ts",        StringType(),  True),  \
				StructField("Comment",           StringType(),  True),  \
				StructField("Orignal_File_Name", StringType(),  True),  \
				StructField("Split_File_Name",   StringType(),  True)   \
			]															\
		)
	return dfSchemaForOutputTable
#---------------------------------------------------------------------------------------------------------------
def CreateSchemaForTableTimeSummary (leadingTabs=""):
	itab1=leadingTabs;
	itab2=itab1 + "\t";
	dfSchemaForTableTimeSummary = StructType	(					\
			[														\
				StructField("Line",			IntegerType(), True),	\
				StructField("BatchID",		StringType(),  True),	\
				StructField("TableID",		StringType(),  True),	\
				StructField("Schema",		StringType(),  True),	\
				StructField("Table",		StringType(),  True),	\
				StructField("Status",		StringType(),  True),	\
				StructField("DATAROWS",		StringType(),  True),	\
				StructField("RM#Cols",		StringType(),  True),	\
				StructField("HIVE#Cols",	StringType(),  True),	\
				StructField("OutBound1",	StringType(),  True),	\
				StructField("HiveTable",	StringType(),  True),	\
				StructField("HiveCols",		StringType(),  True),	\
				StructField("HvRowsCnt",	StringType(),  True),	\
				StructField("RMGetTypes",	StringType(),  True),	\
				StructField("ColsFilter",	StringType(),  True),	\
				StructField("Profiling",	StringType(),  True),	\
				StructField("WriteOutput",	StringType(),  True),	\
				StructField("UpdateQueue",	StringType(),  True),	\
				StructField("OutBound2",	StringType(),  True),	\
				StructField("TableStartTime",StringType(), True),	\
				StructField("TableEndTime",	StringType(),  True),	\
				StructField("Total",		StringType(),  True)	\
			]														\
		);
			# Use StringType so it can be empty string in case program crashed
	return dfSchemaForTableTimeSummary ;
#---------------------------------------------------------------------------------------------------------------
def Create1TableTimeSummaryList (	tableNum, schema, table, tableID, batchID, status,						\
									rowsCount, RMColumns, HiveColumns,	tableStartTime, tableStartTimeT2,	\
									OutBoundMS1_Elapsed,		\
									HiveCheckTable_Elapsed,		\
									HiveRowsCount_Elapsed,		\
									HiveGetColumns_Elapsed,		\
									RoadMasterMS_Elapsed,		\
									ColsFilter_Elapsed,			\
									Profile_Elapsed,			\
									InsertOutputMS_Elapsed,		\
									UpdateQueueMS_Elapsed,		\
									OutBoundMS2_Elapsed,		\
									debugVerbose=False, topNrows=0, leadingTabs=""):
	itab1=leadingTabs;
	itab2=itab1 + "\t";
	itab3=itab2 + "\t";
	if debugVerbose: print (itab1 + "==> Function Create1TableTimeSummaryList;\t\t" + getLocalTimeOfCity() )
	tableElapsed =	str(int(math.ceil(TIME2() - tableStartTimeT2)) ) + " secs";	#Seconds

	oneList      =	(	tableNum,							\
						batchID,							\
						tableID,							\
						schema,								\
						table,								\
						status,								\
						rowsCount,							\
						RMColumns,							\
						HiveColumns,						\
						OutBoundMS1_Elapsed,				\
						HiveCheckTable_Elapsed,				\
						HiveRowsCount_Elapsed,				\
						HiveGetColumns_Elapsed,				\
						RoadMasterMS_Elapsed,				\
						ColsFilter_Elapsed,					\
						Profile_Elapsed,					\
						InsertOutputMS_Elapsed,				\
						UpdateQueueMS_Elapsed,				\
						OutBoundMS2_Elapsed,				\
						tableStartTime,						\
						TIME1.strftime("%Y%m%d %H:%M:%S"),	\
						tableElapsed
					);
	if debugVerbose: print (itab1 + "<== Function Create1TableTimeSummaryList;\t\t" + getLocalTimeOfCity() )
	return oneList;
#---------------------------------------------------------------------------------------------------------------
def CreateSchemaForCheckpoint (leadingTabs=""):
	itab1=leadingTabs;
	itab2=itab1 + "\t";
	dfSchemaForCheckpoint = StructType	(								\
			[															\
				StructField("Table_Num",          IntegerType(), True), \
				StructField("Hive_Schema_Nm",     StringType(),  True), \
				StructField("Table_Nm",           StringType(),  True), \
				StructField("StartTime",          StringType(),  True), \
				StructField("CompleteTime",       StringType(),  True), \
				StructField("OriginalScriptName", StringType(),  True), \
				StructField("SplitScriptName",    StringType(),  True), \
				StructField("Comment",            StringType(),  True), \
				StructField("RowsCount",          StringType(),  True), \
				StructField("ColumnsCount",       StringType(),  True), \
				StructField("SESSION_ID",         StringType(),  True)  \
			]															\
		)
	return dfSchemaForCheckpoint
#---------------------------------------------------------------------------------------------------------------
def CreateSchemaForHiveColumnTypes (leadingTabs=""):
	itab1=leadingTabs;
	itab2=itab1 + "\t";
		# This is the row format returned by RoadMaster
		# For every table it returns all source columns and converted Hive Data Type
	dfCheckpointHiveColumnsType = StructType	(					\
			[														\
				StructField("ASMS",           StringType(),  True),	\
				StructField("Hive_Schema_Nm", StringType(),  True),	\
				StructField("Table_Nm",       StringType(),  True),	\
				StructField("Column_Nm",      StringType(),  True),	\
				StructField("SourceType",     StringType(),  True),	\
				StructField("HiveType",       StringType(),  True),	\
				StructField("ColId",          StringType(),  True),	\
				StructField("TabldId",        StringType(),  True)	\
			]														\
		)
	return dfCheckpointHiveColumnsType
#---------------------------------------------------------------------------------------------------------------
def CreateSchemaForORACLEOutputTable (leadingTabs=""):
	itab1=leadingTabs;
	itab2=itab1 + "\t";

	#Matching Oracle table Chevelle.PROF_COL_STAT
	dfSchemaForORACLEOutputTable =  StructType	(						\
			[															\
				StructField("COL_ID",            StringType(),  True),	\
				StructField("CRT_TS",            StringType(),  True),	\
				StructField("PROF_START_TS",     StringType(),  True),	\
				StructField("TABLE_ID",          StringType(),  True),	\
				StructField("PROF_END_TS",       StringType(),  True),	\
				StructField("BATCH_ID",          StringType(),  True),	\
				StructField("TOT_ROW_CNT",       StringType(),  True),	\
				StructField("COL_VAL_UNIQ_CNT",  StringType(),  True),	\
				StructField("COL_VAL_NULL_CNT",  StringType(),  True),	\
				StructField("COL_MAX_VAL",       StringType(),  True),	\
				StructField("COL_MIN_VAL",       StringType(),  True),	\
				StructField("COL_MEAN_VAL",      StringType(),  True),	\
				StructField("COL_STDEV_VAL",     StringType(),  True),	\
				StructField("COL_MAX_LEN",       StringType(),  True),	\
				StructField("COL_MIN_LEN",       StringType(),  True),	\
				StructField("PROF_CMT",          StringType(),  True),	\
				StructField("ORIG_FILE_NM",      StringType(),  True),	\
				StructField("SPLIT_FILE_NM",     StringType(),  True),	\
				StructField("CRT_BY",            StringType(),  True)	\
			]															\
		);
	return dfSchemaForORACLEOutputTable;
#---------------------------------------------------------------------------------------------------------------
#--{
def    UpdateQueueUsingMicroService (	tableNum, tbl_nm,  tableID, BATCH_ID, SRVR_NM, TaskCD_To_UpdateTo,	\
										tableStartTimeT1_ForMS, URL_UpdateQueue,							\
										G_MICROSERVICE_AUTHORIZATION,debugVerbose=False, leadingTabs=""):
	itab1=leadingTabs;
	itab2=itab1 + "\t";
	itab3=itab2 + "\t";

	l_EndTimeStr=TIME1.strftime("%Y-%m-%d %H:%M:%S")
	headers = {"Content-Type": "application/json", "Cache-Control": "no-cache", "Authorization": G_MICROSERVICE_AUTHORIZATION }

	print (itab1 + "==> Function UpdateQueueUsingMicroService;\t\t" + getLocalTimeOfCity() );
	print (itab2 + "tbl_nm   = " + tbl_nm )
	print (itab2 + "BATCH_ID = " + str(BATCH_ID) )
	print (itab2 + "Table_ID = " + tableID	);
	print (itab2 + "TaskCD   = " + str(TaskCD_To_UpdateTo) + "\t==> " + ConvertTaskStatCodeToString (TaskCD_To_UpdateTo));
	print (itab2 + "tableStartTimeT1_ForMS: "  + tableStartTimeT1_ForMS )
	print (itab2 + "URL_UpdateQueue:       \"" + URL_UpdateQueue + "\"")
	print (itab2 + "headers:               \"" + str(headers)  + "\"")

	try:
		update_data = {
			"table_id":     tableID,
			"batch_id":     BATCH_ID,
			"srvr_nm":      SRVR_NM,
			"task_stat_cd": TaskCD_To_UpdateTo,
			"fail_cnt":     0,
			"max_retry":    30,		# MicroService will overwrite this with its own value
			"upd_by":       "Chevelle",
			"upd_ts":       tableStartTimeT1_ForMS	# StartTime for this table.  This way you can cross-reference easily
		}

		if debugVerbose: print ("\n" + itab2 + "Row to update Queue=" + str(update_data))
			# If i did not put str() around update_date, it would just skip to the end of the function, not raising any exception

		startTime   = TIME2();
		print ("\n" + itab2+ "Calling MicroService: " + URL_UpdateQueue)
		with requests.session() as s:
			s.headers.update(headers)
			output = s.put( URL_UpdateQueue,  json=update_data)
		print (itab3 +"Took: " + ElapsedTimeStrFrom (startTime, TIME2(), debugVerbose) );

		if output.status_code==200:
			print (itab3 + "output.status_code   = " + str(output.status_code) );
			print ("\n" + itab3 + "==> MicroService Return Status: SUCCESSFUL\t\t(at UpdateQueueUsingMicroService());  status_code=" + str(output.status_code)+ "\n")
		else:
			print (itab3 + "output.status_code   = " + str(output.status_code) );
			print (itab3 + "output.status_reason = " +     output.reason)
			print (itab3 + "output.content       = " + str(output.content))
			print ("\n\n***** ERROR: MicroService Return Status: FAIL\t\t(at UpdateQueueUsingMicroService());  status_code=" + str(output.status_code) + "\n\n");
			print (itab3 + "======= NOT RE-RAISING this exception as there is no other place to store these info ======= \n\n" );
			print (itab1 + "<== Function UpdateQueueUsingMicroService;\t\t" + getLocalTimeOfCity() )
			raise EXCEPTION_MICROSERVICE_UPDATE_QUEUE ("MicroService FAILED at UpdateQUEUE()");

	except EXCEPTION_MICROSERVICE_UPDATE_QUEUE:
		print (itab2 + "********** ERROR: Cannot update QUEUE to TASK_CD = " + str(TaskCD_To_UpdateTo)  + "; BATCH_ID=" + BATCH_ID + "\n\n" );
		print (itab3 + "======= NOT RE-RAISING this exception as there is no other place to store these info =======\n\n" );
		#raise EXCEPTION_MICROSERVICE_UPDATE_QUEUE;

	except:
		print("\n\n\n" + itab1 + "********** ERROR: at UpdateQueueUsingMicroService (): UNKNOWN Exception: " + str(sys.exc_info()[0]) + "\n\n\n" );
		print (itab3 + "======= NOT RE-RAISING this exception as there is already the last endpoint =======\n\n" );

	print (itab1 + "<== Function UpdateQueueUsingMicroService;\t\t" + getLocalTimeOfCity() );
#--}
#---------------------------------------------------------------------------------------------------------------
#--{
def WriteToOUTPUTtableUsingMicroService	(	tableNum, schema, table, tbl_nm,  BATCH_ID, DF, URL,					\
											microserviceAuthorization, ONE_ROADMASTER_TABLE_LIST, tableStartTime,	\
											debugVerbose=False, leadingTabs=""):
	itab1=leadingTabs;
	itab2=itab1 + "\t";
	itab3=itab2 + "\t";

	MicroServiceHeader = {"Content-Type": "application/json", "Cache-Control": "no-cache", "Authorization": microserviceAuthorization }

	print (itab1 + "==> Function WriteToOUTPUTtableUsingMicroService;\t\t" + getLocalTimeOfCity() )
	print (itab2 + "BATCH_ID:            \"" + BATCH_ID                 + "\"" );
	print (itab2 + "tableStartTime:      \"" + tableStartTime           + "\"" );
	print (itab2 + "URL:                 \"" + URL                      + "\"" );
	print (itab2 + "MicroServiceHeader:  \"" + str(MicroServiceHeader)  + "\"" );

	print (itab2 + "Converting Input DataFrame into RDD Json of rows:  df_json")
	df_json = DF.toJSON();
	print (itab2 + "Type of df_json    = " + str(type(df_json)) );	#<class 'pyspark.rdd.RDD'>
	if debugVerbose:
		print (itab2 + "Type of input DF   = " + str(type(DF))      );	#<class 'pyspark.sql.dataframe.DataFrame'>
			#EXCEPTION: <class 'UnboundLocalError'>
		print (itab2 + "Content of df_json = " + str(df_json)       );	# MapPartitionsRDD[647] at toJavaRDD at NativeMethodAccessorImpl.java:0
		print (itab2 + "Type of ONE_ROADMASTER_TABLE_LIST = " + str(type(ONE_ROADMASTER_TABLE_LIST)) );
		print (itab2 + "ONE_ROADMASTER_TABLE_LIST         = " + str(ONE_ROADMASTER_TABLE_LIST)       );
		#print (itab2 + "df_json         = " )
		#df_json.show();

	l_EndTimeStr=TIME1.strftime("%Y-%m-%d %H:%M:%S")

	newAllRows			= [];
	newAllRows_of_STR	= [];
	rownum=0;
	print ("\n" + itab2 + "LOOP: Build LIST of DICT objects from input DataFrame:");
	for row in df_json.collect():	#--{				#for row in df_json:
		startTime = TIME2();
		rownum    = rownum+1;
		j         = json.loads(row)
		cName     = j["columname"]

		# Print these out in separat group so in case some exception occurs you can see some relevant print outs
		if debugVerbose:
			print ("\n" + itab3 + "Row #"            + str(rownum)  + ") " + str(row) );
			print (       itab3 + "Type of j     = " + str(type(j)     )    );	# <class 'dict'>
			print (       itab3 + "Type of row   = " + str(type(row)   )    );	# <class 'str'>
			print (       itab3 + "Type of cName = " + str(type(cName) )    );	# <class 'str'>
			print (       itab3 + "Hive colName  = " + cName                );

#                                           | 
#                                           v This space right here is causing it to say:  Hive_Schema_nm NOT FOUND
#Column_ID_LIST=[ (TableId,ColId, Column_Nm, Hive_Schema_nm, Table_Nm)  for (ASMS,Hive_Schema_Nm,Table_Nm,Column_Nm,SourceType,HiveType,ColId,TableId)	\
#						in ONE_ROADMASTER_TABLE_LIST						\
#						if Hive_Schema_Nm==schema and Table_Nm==table and Column_Nm==cName  ]



		# For each Hive ColumnName Get ColID from RoadMaster LIST, to save to OUTPUT table 
		#      #No space allowed in this LEFT Tupole (error otherwise)                    # space is OK in this RIGHT Tuple
		column_ID_LIST=[ (TableId,ColId,Column_Nm,Hive_Schema_Nm,Table_Nm)  for (ASMS,Hive_Schema_Nm,Table_Nm,Column_Nm,SourceType, HiveType,ColId,TableId)	\
						in ONE_ROADMASTER_TABLE_LIST							\
						if  (Hive_Schema_Nm == schema.lower()  or Hive_Schema_Nm == schema.upper() )	\
						and (Table_Nm       == table.lower()   or Table_Nm       == table.upper()  )	\
						and (Column_Nm      == cName.lower()   or Column_Nm      == cName.upper()  )	\
				]
		#If this list

		column_ID_LIST_count = len(column_ID_LIST);      # If column exists in Roadmaster count should be 1
		print (itab3 + "RoadMaster column_ID_LIST       = " + str(column_ID_LIST)		);
		print (itab3 + "RoadMaster column_ID_LIST_count = " + str(column_ID_LIST_count)	);

#		if  column_ID_LIST_count == 0:	#--{
#			print (itab6 + "***** WARNING: Cannot find Col_ID in RoadMaster for this Hive Column: " + cName);
#			print (itab6 + "      ==> Skip saving this row to Oracle Output table (as it would fail the FK on this Col_Id)\n");
#
#				# 
#				#							+---------------+
#				#							|				|
#				#	+---------------+		|				|
#				#	|				|		|				|
#				#	|				|		+---------------+
#				#	|				|
#				#	|				|
#				#	+---------------+
#
#
#		else:	#--}{

		xTableID = column_ID_LIST[0][0].upper()
		xColID   = column_ID_LIST[0][1].upper()
				# WATCH FOR THIS:  ROADMASTER might not have column that HIVE has.  Might cause exception   indexError here

		if debugVerbose:
			print (itab3 + "column_ID_LIST:_count of Tuples = " + str(column_ID_LIST_count));
			print (itab3 + "xTableID               = " + xTableID );
			print (itab3 + "xColID                 = " + xColID );


			#Output columns:
			# |Table_Num|Schema_Name |table_name |columname|Total_count|unique_count|null_count|max_value|min_value|mean_value|std_dev|
			#  max_length|min_length|ins_gmt_ts  |Comment|Orignal_File_Name|Split_File_Name                 

			# Some Errors:
			#	prof_end_ts:  built-in function strftime> is not JSON serializable
			#	Can not deserialize value of type long from String \\"N/A\\": not a valid Long value\\n at

			#Build new object to send to MicroService to save in Oracle Output table
		new1Row =  {								\
				"col_id"            :  xColID,			\
				"crt_ts"            :  tableStartTime,	\
				"prof_start_ts"     :  tableStartTime,	\
				"table_id"          :  xTableID,		\
				"prof_end_ts"       :  l_EndTimeStr,	\
				"batch_id"          :  BATCH_ID,		\
				"tot_row_cnt"       : j["Total_count"],	\
				"col_val_uniq_cnt"  : j["unique_count"],\
				"col_val_null_cnt"  : j["null_count"],	\
				"col_max_val"       : j["max_value"],	\
				"col_min_val"       : j["min_value"],	\
				"col_mean_val"      : j["mean_value"],	\
				"col_stdev_val"     : j["std_dev"],		\
				"col_max_len"       : j["max_length"],	\
				"col_min_len"       : j["min_length"],	\
				"prof_cmt"          : j["Comment"],		\
				"orig_file_nm"      : j["Orignal_File_Name"],	\
				"split_file_nm"     : j["Split_File_Name"],		\
				"crt_by"            : "Chevelle"				\
			}; # This new1Row type is <class 'dict'>

		newAllRows.append( new1Row);

		if debugVerbose:
			print (itab3 +	"Type of new1Row=" + str(type(new1Row ))  +    ";\t\tType of newAllRows="  + str(type(newAllRows) ) +
					";\t\tTook: " + ElapsedTimeStrFrom (startTime,TIME2(), debugVerbose) + "  for this row ");

				# WHEN it tries to parallelize into an RDD and convert into RDD, these String fields get chopped into INDIVIDUAL for each field
				#This is type: <class 'str'> 
				#			new1Row_STR =   '{'	 +		\
				#				'"col_id"            : "' + xColID                    + '",' + 	\
				#				'"crt_ts"            : "' + tableStartTime            + '",' +	\
				#				'"prof_start_ts"     : "' + tableStartTime            + '",' +	\
				#				'"table_id"          : "' + xTableID                  + '",' +	\
				#				'"prof_end_ts"       : "' + l_EndTimeStr              + '",' +	\
				#				'"batch_id"          : "' + BATCH_ID                  + '",' +	\
				#				'"tot_row_cnt"       : "' + j["Total_count"]          + '",' +	\
				#				'"col_val_uniq_cnt"  : "' + j["unique_count"]         + '",' +	\
				#				'"col_val_null_cnt"  : "' + j["null_count"]           + '",' +	\
				#				'"col_max_val"       : "' + j["max_value"]            + '",' +	\
				#				'"col_min_val"       : "' + j["min_value"]            + '",' +	\
				#				'"col_mean_val"      : "' + j["mean_value"]           + '",' +	\
				#				'"col_stdev_val"     : "' + j["std_dev"]              + '",' +	\
				#				'"col_max_len"       : "' + j["max_length"]           + '",' +	\
				#				'"col_min_len"       : "' + j["min_length"]           + '",' +	\
				#				'"prof_cmt"          : "' + j["Comment"]              + '",' +	\
				#				'"orig_file_nm"      : "' + j["Orignal_File_Name"]    + '",' +	\
				#				'"split_file_nm"     : "' + j["Split_File_Name"]      + '",' +	\
				#				'"crt_by"            : "Chevelle"'	;


				#	new1Row       is a DICT  object ==> it prints out in DIFFERENT order from what is declared above
				#	new1Row_Tuple is a TUPLE object ==> it prints out in SAME order as declared
				# Convert above Json Dict object to string object
				# This object is for Debugging only (inside debugVerbose) so it can be displayed nicely in DataFrame
			new1Row_Tuple =   (
					str(xColID)            ,
					tableStartTime         ,
					tableStartTime         ,
					str(xTableID)          ,
					l_EndTimeStr           ,
					str(BATCH_ID )         ,
					j["Total_count"]       ,
					j["unique_count"]      ,
					j["null_count"]        ,
					j["max_value"]         ,
					j["min_value"]         ,
					j["mean_value"]        ,
					j["std_dev"]           ,
					j["max_length"]        ,
					j["min_length"]        ,
					j["Comment"]           ,
					j["Orignal_File_Name"] ,
					j["Split_File_Name"]   ,
					"Chevelle");

			print (itab3 +	"Type of new1Row_Tuple=" + str(type(new1Row_Tuple )));			# <class 'tuple'>; 
			print (itab3 +	"Type of newAllRows_of_STR="  + str(type(newAllRows_of_STR)));	# <class 'dict'>
			print (itab3 +	"new1Row_Tuple=" + str((new1Row_Tuple ))  );
			newAllRows_of_STR.append(new1Row_Tuple);	# <class 'list'>
			print ("\n");

#		#--} IF Hive column exist in RoadMaster

	#--} FOR loop


# Ex:
#                Content of ONE_ROADMASTER_TABLE_LIST:  (Should be same as ONE_ROADMASTER_TABLE_DF above)
#                        [Row(ASMS='NA', Hive_Schema_Nm='DL_EDGE_BASE_NXTGENVC_55777_BASE_GMVCBP_GMEVC_XFOPEL_B2B', Table_Nm='MPV', Column_Nm='MODELYEAR', SourceType='VARCHAR', HiveType='VARCHAR', ColId='600100830A98A533AF379B2CA1281B7C6ADBA295', TabldId='02E043BBAB9088142C30A19E2D83B035673A54AB'), Row(ASMS='N/A', Hive_Schema_Nm='DL_EDGE_BASE_NXTGENVC_55777_BASE_GMVCBP_GMEVC_XFOPEL_B2B', Table_Nm='MPV', Column_Nm='GMDRIVE_MPV', SourceType='VARCHAR', HiveType='VARCHAR', ColId='1B9E002D2B87B5349EFE6761D265946BC4F31BB6', TabldId='02E043BBAB9088142C30A19E2D83B035673A54AB'), ...

#+----+------------------+-------------+---------+----------+--------+----------------------------------------+----------------------------------------+
#|ASMS|Hive_Schema_Nm    |Table_Nm     |Column_Nm|SourceType|HiveType|ColId                                   |TabldId                                 |
#+----+------------------+-------------+---------+----------+--------+----------------------------------------+----------------------------------------+
#|N/A |DL_EDGE_BASE_TIE_2|AWBEZEICHNUNG|NR       |VARCHAR   |VARCHAR |BCEEC5992836BED5E80A1698BDC8D8D7F7BFA3FE|01BEC66DE163E8AC6CDA6AE9D7FD493100FB33E0|
#+----+------------------+-------------+---------+----------+--------+----------------------------------------+----------------------------------------+
#
#	statsForALLCol = [(2,'SCHEMA_NAME', 'SCHEMAName.TableName', 'ColumnName', '0', '0', '', '', '', '', '', '', '', '20180917131910', 'SUCCESS', 'tbl_list800.txt', '20180917_131228__j800__e010__t2__tbl_list800.txt'), ...]

	numRows=len(newAllRows)
	if debugVerbose:
		print ("\n"+ itab2 + "Type of newAllRows="            + str(type(newAllRows))        + "; numRows="+str(numRows) );
		print (      itab2 + "Content of newAllRows: "        + str(newAllRows)              + "\n");
		print ("\n"+ itab2 + "Type of newAllRows_of_STR="     + str(type(newAllRows_of_STR)) + "; numRows="+str(numRows) );
		print (      itab2 + "Content of newAllRows_of_STR: " + str(newAllRows_of_STR)       + "\n");
		print ("\n"+ itab2 + "Convert to DF for display only for DEBUG purpose:  This is same info as delivered to MicroService to be save in Oracle.")

		#------------------------------------------------------------------------------------------------------------------------------------------------
		# For some reason Cannot use this approach as this approach expects a normal string, not json
		#	rdd = spark.sparkContext.parallelize(newAllRows, 1).map (	\
		#		lambda x: Row(x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8],x[9],x[10],x[11],x[12],x[13],x[14],x[15],x[16],x[17],x[18] ));
		#	print (      itab2 + "Type of rdd=" + str(type(rdd)));	# type of rdd=<class 'pyspark.rdd.PipelinedRDD'>
		#	newDF=spark.createDataFrame (rdd, dfSchemaForORACLEOutputTable)
		#
		# This command keeps giving this error: NameError.  But it works at PYSPARK commandline
		#	tempDF = spark.read.json(sc.parallelize(newAllRows_of_STR)); 
		#	tempDF.show(truncate=False)
		#------------------------------------------------------------------------------------------------------------------------------------------------
		try:	#This temp_rdd object is only for displaying as DataFrame for debugging (inside  if debugVerbose block)
			temp_rdd = spark.sparkContext.parallelize(newAllRows_of_STR, 1).map (	\
				lambda x: Row(x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8],x[9],x[10],x[11], x[12], x[13],x[14], x[15], x[16],x[17],x[18]));
			tempDF=spark.createDataFrame (temp_rdd, dfSchemaForORACLEOutputTable)
			tempDF.show(truncate=False)
				# By default  show()  only shows the top 20 rows; and it prints message "only showing top 20 rows"
			del tempDF;
		except:
			print("\n\n\n" + itab2 + "***** ERROR: EXCEPTION: " + str(sys.exc_info()[0]) ); #  <class 'NameError'>


	# See: https://stackoverflow.com/questions/43232169/converting-a-dataframe-into-json-in-pyspark-and-then-selecting-desired-fields/43235939
	# for oneRow in df_json.collect(): allRows = allRows  + oneRow;
	#	newAllRows = DF.toJSON().map(lambda j: json.loads(j)).collect();

	if numRows >= 1:
		#try:
		startTime   = TIME2();
		print (itab2+ "- Write JSON  to Oracle Output table using MicroService: " + URL)
		with requests.session() as s:
			s.headers.update(MicroServiceHeader)
			output = s.post( URL,  json=newAllRows)

		print (itab3 +	"Took: " + ElapsedTimeStrFrom (startTime,TIME2(), debugVerbose) + "  for " + str(numRows)+" rows");
		if output.status_code==200:
			print (itab3 +	"output.status_code   = " + str(output.status_code) );
			print ("\n" + itab3 +	"==> MicroService Return Status: SUCCESSFUL\t\t(at WriteToOUTPUTtableUsingMicroService());  status_code=" + str(output.status_code)+ "\n");
		else:
			print (itab3 + "output.status_code   = " + str(output.status_code) );
			print (itab3 + "output.status_reason = " + output.reason);
			print (itab3 + "output.content       = " + str(output.content));
			print ("\n\n\n"+itab3 +"********** ERROR: at WriteToOUTPUTtableUsingMicroService(): Return Status: FAIL\t\tstatus_code=" +str(output.status_code) + "\n\n");
			print (itab1 + "<== Function WriteToOUTPUTtableUsingMicroService;\t\t" + getLocalTimeOfCity() )
			raise EXCEPTION_MICROSERVICE_WRITE_OUTPUT ("MicroService FAILED at WriteOUPUT");
	else:
		print (itab2+ "Skip writing due to  0 rows (numRows" + str(numRows) + ")\n")

	print (itab1 + "<== Function WriteToOUTPUTtableUsingMicroService;\t\t" + getLocalTimeOfCity() )
#--}
#---------------------------------------------------------------------------------------------------------------
#--{
def WriteRowsToOutput (	statsForALLCol,  dfSchemaForOutputTable,  dfSchemaForORACLEOutputTable,
			file_name,       orig_file_name,     op_dir,   op_dir2,      schema, table, tbl_nm,
			tableNum,        BATCH_ID,                comment,  URL_Write1TableToOutput,
			microserviceAuthorization,    ONE_ROADMASTER_TABLE_LIST,
			tableStartTime, debugVerbose=False,      topNrows=0,      leadingTabs=""):

	# Purpose: Takes input as collection: either a DATAFRAME or LIST 
	#	If it's a LIST it converts to DATAFRAME
	#	- Writes To Jason  using:  newDF.write.json.

	itab1=leadingTabs;
	itab2=itab1 + "\t";
	itab3=itab2 + "\t";
	print (itab1 + "==> Function WriteRowsToOutput;\t\t" + getLocalTimeOfCity() )

	objType=str(type(statsForALLCol));	#Get its type

	if debugVerbose:
		print (itab2 + "Type of param1	= " + objType )
		print (itab2 + "file_name	= " + file_name )
		print (itab2 + "orig_file_name	= " + orig_file_name )
		## print (itab2 + "op_dir		= " + op_dir )
		## print (itab2 + "op_dir2	= " + op_dir2 )
		print (itab2 + "schema		= " + schema )
		print (itab2 + "table		= " + table )
		print (itab2 + "tbl_nm		= " + tbl_nm )
		print (itab2 + "tableNum	= " + str(tableNum) )
		print (itab2 + "comment		= " + comment)
		print (itab2 + "tableStartTime	= " + tableStartTime)
		print (itab2 + "topNrows	= " + str(topNrows) )
		print (itab2 + "Type of objType = " + str(type(objType)))
		print (itab2 + "Type of statsForALLCol  = " + str(type(statsForALLCol)) )
		print (itab2 + "URL_Write1TableToOutput	= " + URL_Write1TableToOutput + "\n")

	if (objType=="<class 'pyspark.sql.dataframe.DataFrame'>") or (objType=="<class 'list'>") or (objType=="<type 'list'>"):
		pass
	else:
		print ("\n\n\n***** ERROR: This method only accepts 'list' or 'dataframe' only (1st parameter)\n\n\n\n");
		print (itab1 + "<== Function WriteRowsToOutput;\t\t" + getLocalTimeOfCity() )
		raise

	if (objType=="<class 'pyspark.sql.dataframe.DataFrame'>"):
		newDF = statsForALLCol;

		if debugVerbose:
			print (itab2 + "===== Input Parameter \"statsForALLCol\" is of type DATAFRAME: " + objType);
			print (itab3+"Type of objType:        " + str(type(objType)))
			print (itab3+"Type of statsForALLCol: " + str(type(statsForALLCol)) )

	if (objType=="<class 'list'>") or (objType=="<type 'list'>"):
		if debugVerbose:
			print (itab2 + "===== Input Parameter \"statsForALLCol\" is of type LIST: " + objType ); print (itab2 + "==> Create DataFrame from this List:" );
		newDF = Create_DataFrame_From_List (statsForALLCol, dfSchemaForOutputTable, "OUTPUT_DF", topNrows, debugVerbose, itab3);


	try:
		print (itab2 + "Write to Oracle Output table using MicroService:")
		WriteToOUTPUTtableUsingMicroService (	tableNum, schema, table, tbl_nm,  BATCH_ID, newDF, URL_Write1TableToOutput,	\
							microserviceAuthorization,	ONE_ROADMASTER_TABLE_LIST,	\
							tableStartTime,	debugVerbose, itab2);
	except :
		print("\n\n\n" + itab2 + "********** ERROR: at WriteRowsToOutput _1_ (): EXCEPTION: " + str(sys.exc_info()[0]) );
		#print (itab2 + "Type of sys.exc_info()[0]= " + str(type(sys.exc_info()[0])));   # <class 'type'>
		print (itab1 + "<== Function WriteRowsToOutput;\t\t" + getLocalTimeOfCity() )
		raise;

	try:
		print (itab2+"Delete Dataframe newDF to free up memory:");
		del newDF;
	except:
		print("\n\n\n" + itab1 + "********** ERROR: at WriteRowsToOutput _2_ (): EXCEPTION:" + str(sys.exc_info()[0]) + "\n\n\n")
		print (itab1 + "<== Function WriteRowsToOutput;\t\t" + getLocalTimeOfCity() )
		raise;	#Should not occur, but just in case 

	print (itab1 + "<== Function WriteRowsToOutput;\t\t" + getLocalTimeOfCity() )
#--}
#---------------------------------------------------------------------------------------------------------------
#--{
def DisplayTopNrows (df, topNrows, leadingTabs, msg=""):
	itab1=leadingTabs;
	itab2=itab1 + "\t";
	itab3=itab2 + "\t";
	print ("\n" + itab1 + "==> Function DisplayTopNrows;\t\t" + getLocalTimeOfCity() )
	objType=str(type(df));	#Get its type
	if (objType=="<class 'pyspark.sql.dataframe.DataFrame'>"):
		totalRows		= df.count()
		totalRowsSTR	= "row";
		if (totalRows > 1): totalRowsSTR="rows"

		_topNrows=1000;	#Force to display up to 1,000 rows.  
						#This is useful when displaying number of columns of a table,
						#such as table returned from RoadMaster MicroService.
						#Helpful to debug to see what were actually there at that time

		if _topNrows < 0 :
			print ("Content of DataFrame: " +  msg + "  (Display ALL: " + str(totalRows) + " " + totalRowsSTR +")")
			df.show(1000000000,False)
		elif  _topNrows == 0 :
			pass;	#Donot display 
		else:
			print ("Content of: " + msg + "  (Display top " + str(_topNrows) + ";  Total=" + str(totalRows) + " " + totalRowsSTR +")")
			df.show(_topNrows, False)
				# If you specify a number of lines to display, and the DataFrame has more rows
				# then the function show() will display "only showing top N rows"
	else:
		print ("\n\n\n\tWARNING: Cannot Display as this object is not DataFrame\n\n\n")
	print (itab1 + "<== Function DisplayTopNrows;\t\t" + getLocalTimeOfCity() )
#--}
#---------------------------------------------------------------------------------------------------------------
#--{
def CastToCorrectHiveType (destType, sourceType, tabcol, leadingTabs=""):
	itab1=leadingTabs;
	itab2=itab1 + "\t";
		# PURPOSE:  Return the correct casted Hive Expression (casted to destType) for a given columnName (tabcol)
		#
		#   destType	is the HIVE DataType that ROADMASTER says it should be changed to
		#   tabcol	is the Current Hive Column, which is STRING: its declared type (in DDL) is String
		#		(currently all columns in Hive are String)
		#
		# OUTPUT:	1 output :   either:   "HiveColumnName, or "cast(HiveColumnName as someType)"

	if   destType=="STRING":    castColExpr = tabcol;	#return same columnNAME; no need to apply any Function to this column
	elif destType=="VARCHAR":   castColExpr = tabcol;
	elif destType=="CHAR":      castColExpr = tabcol;
	elif destType=="CLOB":      castColExpr = tabcol;	#CLOB is considered as STRING
	elif destType=="NUMBER":    castColExpr = "cast("+tabcol + " as " + "BIGINT" + ")";
	elif destType=="TINYINT":   castColExpr = "cast("+tabcol + " as " + destType + ")";
	elif destType=="SMALLINT":  castColExpr = "cast("+tabcol + " as " + destType + ")";
	elif destType=="INT":       castColExpr = "cast("+tabcol + " as " + destType + ")";
	elif destType=="BIGINT":    castColExpr = "cast("+tabcol + " as " + destType + ")";
	elif destType=="FLOAT":     castColExpr = "cast("+tabcol + " as " + destType + ")";
	elif destType=="DOUBLE":    castColExpr = "cast("+tabcol + " as " + destType + ")";
	elif destType=="DATE":      castColExpr = "cast("+tabcol + " as " + destType + ")";
	elif destType=="TIMESTAMP": castColExpr = "cast("+tabcol + " as " + destType + ")";
	elif destType=="N/A" and sourceType=="CLOB":
		castColExpr = tabcol;
		print ("(SpecialCase)", end='');  # This is SPECIAL CASE
	else:
		# Sometimes Roadmaster gives "N/A", like for CLOB ....
		# print ("***** WARNING: UNKNOWN MAP-TYPE (\"" + destType + "\") for Column: " + tabcol+ " ==> Set to original Hive type (String)!");
		# print (itab1+"***** WARNING: UNKNOWN Hive Type (\"" + destType + "\")   ==> Set to original Hive type (String)!");
		print ("\t***UNKNOWN Type: \"" + destType + "\";   sourceType=\""+sourceType + "\""  , end='');
		castColExpr  = tabcol;     #Original Type
	return castColExpr;
#--}
#---------------------------------------------------------------------------------------------------------------
#--{
def ExtractAndAppendToList_1Batch (	tableNum, schema, table, totalTABLERows, timestr, columnsList,	\
					oneDataRow, batchNum, DFColumnsBatchSize, comment,		\
					debugVerbose=False, leadingTabs=""):
	itab1=leadingTabs;
	itab2=itab1 + "\t";
	itab3=itab2 + "\t";
	print ("\n" + itab1 + "==> Function ExtractAndAppendToList_1Batch\t\t"  + getLocalTimeOfCity() +"\n")
	print (       itab2 + "BATCH_NUM = " + str(batchNum) + ";   BatchSize=" + str(DFColumnsBatchSize) )
	if debugVerbose: print (itab2 + "columnsList = \n"  + str(columnsList) );

	maxColLen=max( len(x) for x in columnsList)

	dfStartTime = TIME2();	#time()
	col0   = 0;
	colNum = 0;
	statsForALLCol = [];
	statsFor1Col   = [];
	try:
		for colName in columnsList:	#--{ For Loop to loop over columns to build next sublist of same table
			columnStartTime	= TIME2(); #time()
			colNum		= colNum + 1;	
			s1		= str(colNum) + ") " ;	#use for debug display
			col1  = col0+1;	
			col2  = col1+1;
			col3  = col2+1;
			col4  = col3+1;
			col5  = col4+1;
			col6  = col5+1;
			col7  = col6+1;
			col8  = col7+1;

			#if debugVerbose: print (itab2 + "Column " + str(colNum) +" of Batch "+ str(batchNum) + ")  " + colName);
			#print (itab3 + "col1 pt=" + str(col1)  + " ==> '" + str(oneDataRow[0][col1]) + "'" );

			# Extract set of 9 columns from  oneDataRow row into LIST:
			statsFor1Col  =	[  (	tableNum, schema,  tbl_nm,  colName,   str(totalTABLERows),    oneDataRow[0][col1],	\
						oneDataRow[0][col2], oneDataRow[0][col3], oneDataRow[0][col4], oneDataRow[0][col5],	\
						oneDataRow[0][col6], oneDataRow[0][col7], oneDataRow[0][col8],	\
						timestr,        comment,      orig_file_name, file_name		\
					)  ]

			# Append new set of stats of current column to List of columns (for building DataFrame later)
			statsForALLCol = statsForALLCol  + statsFor1Col;

			col0  = col8;
			if debugVerbose:	#DoNOT use itab because this is a long line.  Print at begin of line
				#print (	"Col # " + s1.ljust(4) + colName.ljust(maxColLen)	+	\
				print (	"\t" + "Col # " + s1 + colName.ljust(maxColLen)	+	\
					"; Took: "    + ElapsedTimeStrFrom (columnStartTime,TIME2(), debugVerbose)	+	\
					"; VALUES: " + str(statsFor1Col)			+	\
					";\t(==> New col0 pointer= " + str(col0) + ")"		);
	except Exception as expEAL:		#
		DisplayExceptionMessage (expEAL, tableNum, schema, table, "expEAL", "ExtractAndAppendToList_1Batch()", itab1)
		print (itab1 + "***** ERROR: ExtractAndAppendToList_1Batch() raises exception expEAL\n\n");
		print (itab1 + "<== Function ExtractAndAppendToList_1Batch   " + getLocalTimeOfCity() + ")\n")
		raise expEAL;
	#--} 

	if debugVerbose: print ("\n" + itab2 + "===== Final List: statsForALLCol:"+ str(statsForALLCol)  + "\n\n")
	print (itab2 + "Took: " + ElapsedTimeStrFrom(dfStartTime,TIME2(), debugVerbose)  + " to build this List of this Batch # " + \
			str(batchNum) + ".  Function ExtractAndAppendToList_1Batch()");
	print (itab1 + "<== Function ExtractAndAppendToList_1Batch\t\t" + getLocalTimeOfCity() + ")")
	return statsForALLCol;
#--}
#---------------------------------------------------------------------------------------------------------------
#--{
def Execute_1Batch (tableNum, schema, table, timestr, sql, batchNum, DFColumnsBatchSize, comment, debugVerbose=False, leadingTabs=""):
	itab1=leadingTabs;
	itab2=itab1 + "\t";
	itab3=itab2 + "\t";
	print ("\n" + itab1 + "==> Function Execute_1Batch;\t\t" + getLocalTimeOfCity() )
	print (itab2 + "batchNum = " + str(batchNum) + ";   BatchSize=" + str(DFColumnsBatchSize) )

	# Execute command to calculate statistics.  It might fail for error such as:  No Priv to select ...
	startTime = TIME2();	#time()
	try:	#--{
		result      = spark.sql(sql);	# Execute SQL statement. Type: DataFrame.  
						# NOTE: spark.sql  is actually a TRANSFORM statement, not ACTION yet
						# So it returns very fast.
			# This dfTotalRows should be 1 row as it is a select count(*)

		if debugVerbose: print (itab2+ "Type of Result =" +  str(type(result)) )

		print (itab2+ "Result.count()\tBEFORE:\t" +  getLocalTimeOfCity())
		dfTotalRows = result.count();	# Use Count() as ACTION to force Spark to execute this SQL
		print (itab2+ "Result.count()\tAFTER:\t" +  getLocalTimeOfCity())
		if debugVerbose: print (itab2+ "Type of dfTotalRows =" + str(type(dfTotalRows)) );

			# Count() is still a TRANSFORM command         (complete very fast)
			# Take(n) and Show() are actual ACTION command (complete much longer)

			#This show() is taking a long time, so lets comment it out:
			#print ("\n"+itab2+ "Result.show(1)\tBEFORE :\t\t" +  getLocalTimeOfCity())
			#result.show(1, False)
			#print ("\n"+itab2+ "Result.show(1)\tAFTER :\t\t" +  getLocalTimeOfCity())

		print ("\n"+itab2+ "Result.take(1)  BEFORE:\t" +  getLocalTimeOfCity())
		take1=result.take(1);	#Type: [Row(TableName='...', Col_Nm='....', TotalCount='...'
		print (itab2+ "Result.take(1)  AFTER:\t" +  getLocalTimeOfCity())
		if debugVerbose: print (itab2+ "Type of take1 =" +  str(type(take1)) )

		print ("\n" + itab2 +	"Total Rows Returned=" + str(dfTotalRows) +							\
								";  Took: " + ElapsedTimeStrFrom(startTime,TIME2(), debugVerbose) +	\
								" to execute this SQL.\t\t" + getLocalTimeOfCity() );
		del result;
	except Exception as exe1Batch:	#Could be due to error such as: Permission of reading HDFS files
		DisplayExceptionMessage (exe1Batch, tableNum, schema, table, "exe1Batch", "Execute_1Batch()", itab1)
		print (itab1 + "***** ERROR: Execute_1Batch() raises Exception: exe1Batch\n\n");
		print (itab1 + "<== Function Execute_1Batch;\t" + getLocalTimeOfCity() )
		raise exe1Batch;
	#--}
	print (itab1 + "<== Function Execute_1Batch;\t" + getLocalTimeOfCity() )
	return take1
#--}
#---------------------------------------------------------------------------------------------------------------
#--{
def Execute_1SUBBatch (tableNum, schema, table, timestr, sql, batchNum, DFColumnsBatchSize, comment, debugVerbose=False, leadingTabs=""):
	itab1=leadingTabs;
	itab2=itab1 + "\t";

	print (itab1 +	"==> Function Execute_1SUBBatch;\t\t" + getLocalTimeOfCity() )
	take1=Execute_1Batch (tableNum, schema, table, timestr, sql, batchNum, DFColumnsBatchSize, comment, debugVerbose, itab2)
	print (itab1 +	"<=> Function Execute_1SUBBatch;\t\t" + getLocalTimeOfCity() )
		#Even though this function calls Execute_1Batch() directly we still want to have this function
		# so it will batch with the other function: BuildSQL_1_SUB_Batch()    (ie, so less confusion later)

	return take1
#--}
#---------------------------------------------------------------------------------------------------------------
#--{
def BuildSQL_1_SUB_Batch (	tableNum, schema, table, registeredTABLE_DFname, totalTABLERows,		\
				ONE_ROADMASTER_TABLE_DF,	ONE_ROADMASTER_TABLE_LIST,			\
				timestr, subTableColumnsList, startColNum, batchNum, DFColumnsBatchSize,	\
				STATCOLS_BATCH_SIZE,	LEFT, RIGHT, comment,	debugVerbose=False, leadingTabs=""):

	# 1 table: with a bunch of columns of that table, divided into batches of columns
	itab1=leadingTabs;
	itab2=itab1 + "\t";
	itab3=itab2 + "\t";
	if debugVerbose: print (itab1 +	"==> Function BuildSQL_1_SUB_Batch;\t\t" + getLocalTimeOfCity() )
	subTableColumnsList_count=len(subTableColumnsList)

	print (itab2 +	"batchNum = " + str(batchNum) + ";   TABLE ColumnsBatchSize=" + str(DFColumnsBatchSize)	);
	print (itab2 +	"STATCOLS_BATCH_SIZE		= " + str(STATCOLS_BATCH_SIZE)		);
	print (itab2 +	"subTableColumnsList_count	= " + str(subTableColumnsList_count)	);
	print (itab2 +	"subTableColumnsList		= " + str(subTableColumnsList)		); 

	startTime       = TIME2();	# = time()	
	tableColumnsStr = " "
	#### sql  = "select "+ "'" + tbl_nm +  "' as TABLE_NM,\n";  # Quotes around tbl_nm to make it a literal
	sql  = "select "+ "'" + tbl_nm +  "' as TABLE_NM,\n";  # Quotes around tbl_nm to make it a literal
	colNum=startColNum

	try:	#--{
		if ONE_ROADMASTER_TABLE_DF.count() == 0: 
			#Here cannot find Hive columms in DataFrame for this table
			print ("\n"+itab2 + "***** ERROR: RoadMaster does NOT have this TABLE: \"" + schema +"."+ table + "\" *****\n");
			if Check_MUST_HAVE_HIVE_DATATYPES:
				print (itab3 + "==> RAISE: EXCEPTION_HIVE_TABLE_NOTFOUND_IN_ROADMASTER");
				raise EXCEPTION_HIVE_TABLE_NOTFOUND_IN_ROADMASTER("***** RoadMaster does not have this table: \"" + schema +"."+ table + "\" *****");
			else:
				print (itab3+ "Option \"Check_MUST_HAVE_HIVE_DATATYPES\" was specified ==> Ignore this error to let it go thru");
		else:
			RoadMasterHasTable = True

		for tabcol in [c for c in subTableColumnsList ]:	# --{ For consistency use FOR loop like the other function.
									#     But actually for this method there is only 1 column
			#--------------------------------------------------------------------------------------------------
			# This DataFrame filter takes too long, like 30 seconds per column (1 row):
			# switch over to use LIST instead
			#	cond3 = col("Column_Nm"  )     == tabcol.upper();
			#	column_Type_DF          = ONE_ROADMASTER_TABLE_DF.filter( cond1  & cond2 & cond3)
			#	column_Type_DF_count    = column_Type_DF.count()
			#--------------------------------------------------------------------------------------------------

			lookup2_StartTime = TIME2();    # = time()
			#See   def CreateSchemaForHiveColumnTypes for these 6 columns names/types
			column_Type_LIST=[ (SourceType,HiveType)  for (ASMS,Hive_Schema_Nm,Table_Nm,Column_Nm,SourceType,HiveType,ColId,TableId)	\
								in ONE_ROADMASTER_TABLE_LIST						\
								if Hive_Schema_Nm==schema and Table_Nm==table and Column_Nm==tabcol ]

			column_Type_LIST_count    = len(column_Type_LIST);      # should be 1 row return
			if debugVerbose:
				print (	"\n"+ itab2+   "Filtering on Column " + tabcol + " against RoadMasterList took: "	+ \
					ElapsedTimeStrFrom(lookup2_StartTime,TIME2(), debugVerbose) +	"  ==>  Filtered Rows Count="			+ \
					str(column_Type_LIST_count) + ";   "  + getLocalTimeOfCity());
				print (itab2+ "RoadMaster Column Mapping: "   + str(column_Type_LIST));

			if column_Type_LIST_count == 0:         # if column notfound in RoadMaster
				#Here cannot find columm in DataFrame HIVE_ALL_COLUMN_TYPES_DF.filter.  Then set it to STRING
				destType   = "STRING"
				SourceType = "STRING"
				print (	"\n\n***** ERROR: RoadMaster does NOT have this COLUMN: \"" + tabcol + \
					"\" for this table: \"" + schema +"."+ table + "\" *****")

				if Check_MUST_HAVE_HIVE_DATATYPES:
					print (itab2+ "==> RAISE: EXCEPTION_HIVE_COLTYPE_NOTFOUND_IN_RM");
					raise EXCEPTION_HIVE_COLTYPE_NOTFOUND_IN_RM("***** RoadMaster does NOT have this column: \"" +	\
						tabcol + "\" for this table: \"" + schema +"."+ table + "\" *****");
				else:
					print (itab2+ "Option \"Check_MUST_HAVE_HIVE_DATATYPES\" was specified ==> Ignore this error to let it go thru");

			else:
				#Here found column type in RoadMaster.  Set to it.
				SourceType = column_Type_LIST[0][0].upper()
				destType   = column_Type_LIST[0][1].upper()


			destTypeUpper=destType.upper()
			sourceTypeUpper=SourceType.upper()

			#newTabCol="coalesce("   + tabcol +   ",'NULL','', " + tabcol +")";
			newTabCol=tabcol;

			# Don't print as it interferes with actual output
			#   print ("============= newTabCol=" + newTabCol + ";  destTypeUpper="+destTypeUpper + "; sourceTypeUpper="+sourceTypeUpper);
			castCol1  = CastToCorrectHiveType (destTypeUpper, sourceTypeUpper,  newTabCol, itab2)

			#print ( itab3 +"Col#"+str(colNum)+":\t"+ tabcol.ljust(maxColLen) + "\t==> castCol1: \"" + castCol1 )
			tableColumnsStr    = tableColumnsStr+" "+tabcol;	# List of all columnNames so far

			#NOTE: This function is for APPROACH=REGULAR TABLE PROCESSING: ie, must have all 8 Stat columns for each TableColumn (in each batch)
			#      (For LARGE TABLE PROCESSING: there is a loop to allow a subset of Stat columns per Table Column; It will combine them correctly)

			print (itab2+ "Loop to Append STAT Expresssions for " + str(RIGHT-LEFT+1) + " STAT cols;  LEFT=" + str(LEFT)+ "; RIGHT="+str(RIGHT) );
			i=LEFT+1;
			while i<= RIGHT+1:	#--{
				# 8 STAT columns: each is associated with a very specific function, such as MIN(), MAX
				# Hence we can apply different function depending on column datatype
				if   i==1:
					# GetHIVETypeCastMapDecision(destType, i);
					print (itab3+"i="+str(i) + ") UniqCount");
					sql=sql+ " coalesce(cast(count(distinct("     + castCol1 + ")) as string), '')  as UniqCount"  + str(colNum);
				elif i==2:
					print (itab3+"i="+str(i) + ") NullCount");
					sql=sql+ "     coalesce (cast(sum(case when " + castCol1 + " is null then 1 else 0 end) as string), '') as NullCount" + str(colNum);
				elif i==3:
					print (itab3+"i="+str(i) + ") MaxVal");
					sql=sql+ "     coalesce (cast(max("           + castCol1 + ")  as string), '' ) as MaxVal"   + str(colNum);
				elif i==4:
					print (itab3+"i="+str(i) + ") MinVal");
					sql=sql+ "     coalesce (cast(min("           + castCol1 + ")  as string), '' ) as MinVal"   + str(colNum);
				elif i==5:
					print (itab3+"i="+str(i) + ") MeanVal");
					if (destType=="DATE") or (destType=="TIMESTAMP"):
						#sql=sql+"  \"N/A\" as MeanVal"  + str(colNum);	
							# Replace "N/A" with "" out due to error with MicroServic3:
							#    Can not deserialize value of type long from String \\"N/A\\": not a valid Long value\\n at

						sql=sql+"   \"\" as MeanVal"  + str(colNum)
					else:
						sql=sql+"     coalesce (cast(avg("           + castCol1 + ")  as string), '' ) as MeanVal"  + str(colNum);

				elif i==6:
					print (itab3+"i="+str(i) + ") StdDev")
					if (destType=="DATE") or (destType=="TIMESTAMP"):
						#sql=sql+"  \"N/A\" as StdDev"  + str(colNum)
						sql=sql+"   \"\" as StdDev"  + str(colNum)
					elif destType=="CLOB":						# Need this to avoid error:   "NaN"
						sql=sql+"   \"\" as StdDev"  + str(colNum)
					else:
						sql=sql+  "     coalesce (cast(stddev("        + castCol1 + ")  as string), '' ) as StdDev"   + str(colNum);
				elif i==7:
					print (itab3+"i="+str(i) + ") MaxLen")
					if (destType=="DATE") or (destType=="TIMESTAMP"):
						#sql=sql+"  \"N/A\" as MaxLen"  + str(colNum)
						sql=sql+"   \"\" as MaxLen"  + str(colNum)
					else:
						sql=sql+ "     coalesce (cast(max(length("    + castCol1 + ")) as string), '' ) as MaxLen"   + str(colNum)
				elif i==8:
					print (itab3+"i="+str(i) + ") MinLen")
					if (destType=="DATE") or (destType=="TIMESTAMP"):
						#sql=sql + "   \"N/A\" as MinLen"  + str(colNum)
						sql=sql + "    \"\" as MinLen"  + str(colNum)
					else:
						sql=sql + "     coalesce (cast(min(length("    + castCol1 + ")) as string), '' ) as MinLen"   + str(colNum);
				else:
					print ("\n\n\n***** ERROR: Bad STAT column Number:  i=" + str(i) + "\n\n\n")
					sys.exit(-1)

				if i< RIGHT+1:
					sql=sql+ ",\n"

				i += 1;
			#--}
			print (itab2+ "End Loop to Append STAT Expresssions ");
			sql=sql + "\nfrom " + registeredTABLE_DFname
			print ("\n"+itab2+"===== sql=\n" + sql)

		#--}
	except EXCEPTION_COLUMNS_COUNT_MISMATCH:
		DisplayExceptionMessage (EXCEPTION_COLUMNS_COUNT_MISMATCH, tableNum, schema, table,	\
					"EXCEPTION_COLUMNS_COUNT_MISMATCH", "BuildSQL_1_SUB_Batch()", itab1)
		print (itab1 + "Function BuildSQL_1_SUB_Batch() raises: EXCEPTION_COLUMNS_COUNT_MISMATCH");
		print (itab1 + "<== Function BuildSQL_1_SUB_Batch;   " + getLocalTimeOfCity() + ")\n")
		raise EXCEPTION_COLUMNS_COUNT_MISMATCH;

	except EXCEPTION_HIVE_TABLE_NOTFOUND_IN_ROADMASTER:
		DisplayExceptionMessage (EXCEPTION_HIVE_TABLE_NOTFOUND_IN_ROADMASTER, tableNum, schema, table, \
					"EXCEPTION_HIVE_TABLE_NOTFOUND_IN_ROADMASTER", "BuildSQL_1_SUB_Batch()", itab1)
		print (itab1 + "Function BuildSQL_1_SUB_Batch() raises: EXCEPTION_HIVE_TABLE_NOTFOUND_IN_ROADMASTER");
		print (itab1 + "<== Function BuildSQL_1_SUB_Batch;   " + getLocalTimeOfCity() + ")\n")
		raise EXCEPTION_HIVE_TABLE_NOTFOUND_IN_ROADMASTER;

	except EXCEPTION_HIVE_COLTYPE_NOTFOUND_IN_RM:
		DisplayExceptionMessage (EXCEPTION_HIVE_COLTYPE_NOTFOUND_IN_RM, tableNum, schema, table, \
					"EXCEPTION_HIVE_COLTYPE_NOTFOUND_IN_RM", "BuildSQL_1_SUB_Batch()", itab1)
		print (itab1 + "Function BuildSQL_1_SUB_Batch() raises: EXCEPTION_HIVE_COLTYPE_NOTFOUND_IN_RM");
		print (itab1 + "<== Function BuildSQL_1_SUB_Batch;   " + getLocalTimeOfCity() + ")\n")
		raise EXCEPTION_HIVE_COLTYPE_NOTFOUND_IN_RM;

	except Exception as expB1BsubSQL:
		DisplayExceptionMessage (expB1BsubSQL, tableNum, schema, table,  "expB1BsubSQL", "BuildSQL_1_SUB_Batch()", itab1)
		print (itab1 + "Function BuildSQL_1_SUB_Batch() raises Exception: expB1BsubSQL");
		print (itab1 + "<== Function BuildSQL_1_SUB_Batch;   " + getLocalTimeOfCity() + ")\n")
		raise expB1BsubSQL;
	#--}
	if debugVerbose: print (itab1 +	"<== Function BuildSQL_1_SUB_Batch;\t\t" + getLocalTimeOfCity() + "\n")
	newColumnsList=[subTableColumnsList[0]]
	return (newColumnsList, sql)
#--}
#---------------------------------------------------------------------------------------------------------------
#--{
def BuildSQL_1Batch (	tableNum,     schema,    table,     registeredTABLE_DFname, totalTABLERows,	\
						ONE_ROADMASTER_TABLE_DF,    ONE_ROADMASTER_TABLE_LIST,						\
						timestr,      subColumnsList,       startColNum,							\
						batchNum,     DFColumnsBatchSize,   STATCOLS_BATCH_SIZE,					\
						comment,      debugVerbose=False,   leadingTabs=""):

	# 1 table: with a bunch of columns of that table, divided into batches of columns
	itab1=leadingTabs;
	itab2=itab1 + "\t";
	itab3=itab2 + "\t";
	itab4=itab3 + "\t";
	itab5=itab4 + "\t";

	totalSTATColumns=8;
	# 8 is the maximum number of basic stats columns to collect
	# These  8 cols are from: dfSchemaForOutputTable
	# 	StructField("Total_count",       StringType(),  True),  <== This 1st column is NOT included as it is now calculated ONCE for entire table
	#	# (Previously this Total_Count is repeated for every TABLE COLUMN)
	# 1. StructField("unique_count",      StringType(),  True),  \
	# 2. StructField("null_count",        StringType(),  True),  \
	# 3. StructField("max_value",         StringType(),  True),  \
	# 4. StructField("min_value",         StringType(),  True),  \
	# 5. StructField("mean_value",        StringType(),  True),  \
	# 6. StructField("std_dev",           StringType(),  True),  \
	# 7. StructField("max_length",        StringType(),  True),  \
	# 8. StructField("min_length",        StringType(),  True),  \

	subColumnsList_count=len(subColumnsList)

	print (itab1 +	"==> Function BuildSQL_1Batch;\t\t" + getLocalTimeOfCity() )
	print (itab2 +	"schema			= " + schema );
	print (itab2 +	"table			= " + table );
	print (itab2 +	"batchNum		= " + str(batchNum) + "       <=====================");
	print (itab2 +  "DFColumnsBatchSize	= " + str(DFColumnsBatchSize) );
	print (itab2 +  "startColNum		= " + str(startColNum) );
	print ("\n");

	startTime       = TIME2();	# time()	
	colNum          = startColNum;	# Starting Table Column Num (due to batching of columns)
					# (like: table has 1000 columns but only do 100 column at a time as DataFrame has max about 2500 columns)
	tableColumnsStr = " "
	sql             = "select "+ "'" + tbl_nm +  "' as TABLE_NM";  # Quotes around tbl_nm to make it a literal
	newSQL		= "select "+ "'" + tbl_nm +  "' as TABLE_NM";  # Quotes around tbl_nm to make it a literal

			# No need to ColumName in this SQL as it will be added in the next function ExtractAndAppendToList_1Batch()
			# Both of these function have same list "subColumnsList" to both set of columns are in same order

	try:

		#This block works, but unnecessary.  Keep creating 2 extra dataframes just cost time, even though it is small
		#OneRoadMasterTable_DF  = HIVE_ALL_COLUMN_TYPES_DF.filter( cond1  & cond2 )
		#	#This DF has all columns for this table, include DW colunmns to be ignored.
		#
		#if debugVerbose: OneRoadMasterTable_DF.show() 
		#
		#print (itab2+"Create DF of SubColumnsList:")
		#SUB_COLUMNS_DF_NAME = "SUB_COLUMNS_DF"
		#SUB_COLUMNS_DF      = Create_DataFrame_From_DWColumns_List (subColumnsList, dfSchemaForDWColumns,	\
		#					SUB_COLUMNS_DF_NAME,	topNrows, debugVerbose, itab3)
		#
		#print ("\n"+ itab2+"Join with Roadmaster DF to get correct DataType:")
		#
		#ONE_ROADMASTER_TABLE_DF         = SUB_COLUMNS_DF.join(OneRoadMasterTable_DF, "Column_Nm", "inner")
		#if debugVerbose: ONE_ROADMASTER_TABLE_DF.show() 
		#	# The resultant DF join does NOT preserver the order of rows of the 1st input table: SUB_COLUMS_DF.
		#	# But this does NOT matter as the For loop on columns below operates on the "subColumnsList", 
		#	# which is the same column order of Table 


#-----------------------------------------------------------------------------------------------
# WEIRD:  Different number of columns betweeen Source and Hive
#		DL_EDGE_BASE_BIS_13426_BASE_OBISDGXP_BIS.BIS_ORDER_LINE
# MicroService return 56 cols in DataFrame
# subColumnList (from Hive) returns 57:  Extra column: order_grp_id            string
#-----------------------------------------------------------------------------------------------

		if debugVerbose:
			print (itab2 + "subColumnsList of CURRENT BATCH = " + str(subColumnsList)            + "\n" );
			print (itab2 + "ONE_ROADMASTER_TABLE_LIST       = " + str(ONE_ROADMASTER_TABLE_LIST) + "\n" );

		print (itab2 + "1. Count of Hive       TABLE Columns = " + str(subColumnsList_count) );
		print (itab2 + "2. Count of RoadMaster TABLE Columns = " + str(len(ONE_ROADMASTER_TABLE_LIST) ) );

		### This comparision is not correct as this Hive Count is only for this current batch
		### if len(ONE_ROADMASTER_TABLE_LIST) != subColumnsList_count:
		### 	print ("\n" + itab2 + "WARNING: These 2 Counts are DIFFERENT, but check the BATCH");
		### 	print (       itab2 + "Each Hive column will be checked against RoadMaster columns to detect missing columns\n\n");

		if debugVerbose:
			print ("\n"+ itab2 + "Definition from RoadMaster for this table:  ONE_ROADMASTER_TABLE_DF")
			ONE_ROADMASTER_TABLE_DF.show(100000, False)

		if ONE_ROADMASTER_TABLE_DF.count() == 0:
			#Here cannot find Hive columms in DataFrame for this table
			print ("\n"+itab2 + "***** ERROR: RoadMaster does NOT have this table: \"" + schema +"."+ table + "\" *****\n");
			if Check_MUST_HAVE_HIVE_DATATYPES:
				print (itab2 + "==> RAISE: EXCEPTION_HIVE_TABLE_NOTFOUND_IN_ROADMASTER");
				raise EXCEPTION_HIVE_TABLE_NOTFOUND_IN_ROADMASTER("***** RoadMaster does not have this table: \"" + schema +"."+ table + "\" *****");
			else:
				print (itab2+ "Option \"Check_MUST_HAVE_HIVE_DATATYPES\" was specified ==> Ignore this error to let it go thru");
		else:
			RoadMasterHasTable = True

		maxColLen        = max( len(x) for x in subColumnsList);
		maxSourceTypeLen = max([len(SourceType) for (ASMS,Hive_Schema_Nm,Table_Nm,Column_Nm,SourceType,HiveType,ColId,TableId) in ONE_ROADMASTER_TABLE_LIST ]);
		maxHiveTypeLen   = max([len(HiveType)   for (ASMS,Hive_Schema_Nm,Table_Nm,Column_Nm,SourceType,HiveType,ColId,TableId) in ONE_ROADMASTER_TABLE_LIST ]);
		maxMapLen        = maxSourceTypeLen + maxSourceTypeLen + 10;	# 10 extra columns for display this string: [('TIMESTAMP', 'TIMESTAMP')]
		if debugVerbose:
			print (itab2 + "maxColLen        = " + str(maxColLen)        );
			print (itab2 + "maxSourceTypeLen = " + str(maxSourceTypeLen) );
			print (itab2 + "maxHiveTypeLen   = " + str(maxHiveTypeLen)   );
			print (itab2 + "maxMapLen        = " + str(maxMapLen)        );


		totalColumnsSize=len(str(subColumnsList_count))
		print (	"\n"+itab2+"LOOP on Hive SubColumnsList:  (of BuildSQL_1Batch() )" );
		for tabcol in [c for c in subColumnsList ]:		# --{ for each column in Batch build 8 statistics
			#### colNum  += 1;	### DO NOT  INCRENENT ColNum here.
						### Increment it at bottom of For loop as it already started with a "StartColNum" value 

			# 1) Get its real type from ROADMASTER file by: Check if there is a rows for column in Dataframe.  
			#    If there is     ==> get its type
			#    If there is NOT ==> set its type=STRING

			print ( itab3 + "Col #"+str(colNum).ljust(totalColumnsSize) +":  "+ tabcol.ljust(maxColLen) + "  ", end='');
				# This  ColNum is of the CURRENT BATCH.  So, say, for Batch#2 the first column displayed could be 101
				# assuming each batchSize=100

				# This DataFrame filter below takes too long, like 30 seconds per column (1 row):
				# switch over to use LIST instead
					#cond3 = col("Column_Nm") == tabcol.upper();
					#column_Type_DF          = ONE_ROADMASTER_TABLE_DF.filter( cond1  & cond2 & cond3)
					#column_Type_DF_count    = column_Type_DF.count()

			lookup2_StartTime = TIME2();	# = time()	
			#See   def CreateSchemaForHiveColumnTypes for these columns names/types


			#For each HiveCol, Filter on RM to get ColID:
			column_Type_LIST=[ (SourceType,HiveType)  for (ASMS,Hive_Schema_Nm,Table_Nm,Column_Nm,SourceType,HiveType,ColId,TableId)	\
								in ONE_ROADMASTER_TABLE_LIST							\
								if  (Hive_Schema_Nm == schema.lower()  or Hive_Schema_Nm == schema.upper() )	\
								and (Table_Nm       == table.lower()   or Table_Nm       == table.upper()  )	\
								and (Column_Nm      == tabcol.lower()  or Column_Nm      == tabcol.upper() )	\
						]

			column_Type_LIST_count    = len(column_Type_LIST);      # should be 1 row return

			#if debugVerbose: print (itab4 +	"Column Filter Took: " + ElapsedTimeStrFrom(lookup2_StartTime,TIME2(), debugVerbose) +	\
			#				"  ==>  Filtered Rows Count: " + str(column_Type_LIST_count)    + ";\t"	+
			#				" RoadMaster Column Mapping: " + str(column_Type_LIST));

			if column_Type_LIST_count == 0:         # if column notfound in RoadMaster
				#Here cannot find columm in DataFrame HIVE_ALL_COLUMN_TYPES_DF.filter.  Then set it to STRING
				destType   = "STRING"
				SourceType = "STRING"
				print (	"\n\n\n***** ERROR: RoadMaster does NOT have this COLUMN: \"" + tabcol + \
					"\" for this table: \"" + schema +"."+ table + "\" *****\n")

				if Check_MUST_HAVE_HIVE_DATATYPES:
					print (itab4+ "==> RAISE: EXCEPTION_HIVE_COLTYPE_NOTFOUND_IN_RM");
					raise EXCEPTION_HIVE_COLTYPE_NOTFOUND_IN_RM("***** RoadMaster does NOT have this column: \"" +	\
						tabcol + "\" for this table: \"" + schema +"."+ table + "\" *****");
				else:
					print (itab4+ "Option \"Check_MUST_HAVE_HIVE_DATATYPES\" was specified ==> Ignore this error to let it go thru");

			else:
				#Here found column type in RoadMaster.  Set to it.
				#destType = column_Type_DF.take(1)[0]["HiveType"].upper()
				SourceType = column_Type_LIST[0][0].upper()
				destType   = column_Type_LIST[0][1].upper()

# Example:
# +----+--------------------+--------+-------------+------------+--------+
# |ASMS|      Hive_Schema_Nm|Table_Nm|    Column_Nm|  SourceType|HiveType|
# +----+--------------------+--------+-------------+------------+--------+
# | 335|DL_EDGE_BASE_DID_...|  T35501| SELLG_SCE_NO|     CHAR(2)|  STRING|	<== The (n) after the types are REMOVED  (from HiveType column)
# | 335|DL_EDGE_BASE_DID_...|  T35501|      SITE_NO|  INTEGER(0)|  BIGINT|
# | 335|DL_EDGE_BASE_DID_...|  T35501| BUSN_FUNC_CD|DECIMAL(5|0)|   FLOAT|
# | 335|DL_EDGE_BASE_DID_...|  T35501| FINC_TERM_DT|     DATE(0)|    DATE|
# | 335|DL_EDGE_BASE_DID_...|  T35501| XYZ         |     CLOB   |     N/A|
# +----+--------------------+--------+-------------+------------+--------+
#
# Error Message from Spark:
# cannot resolve 'avg(CAST(temp_df_table.`EFCTV_DT` AS DATE))' due to data type mismatch: function average requires numeric types, not DateType
# REASON: Max(timestamp) is OK  but Avg(timestamp) is NOT
#         ==> Must have 2 casts, one for each different Agg function

			destTypeUpper=destType.upper()
			sourceTypeUpper=SourceType.upper()

#---------------------------------------------------------------------------------------------------------------------------------
# NOTE: function STDDEV() only works with actual number or NULL, not empty string (''), which we currenly use represent NULL
# Here is SQL running in spark-sql:
#	select
#	     stddev(           regexp_replace(DEST_ENVIRONMENT_ID, '^NULL$', ''  ))                    as A,
#	     stddev(           regexp_replace(DEST_ENVIRONMENT_ID, '^NULL$', null))                    as B,
#	     coalesce(stddev(  regexp_replace(DEST_ENVIRONMENT_ID, '^NULL$', null)), 'returned NULL')  as C
#	from SVC_EDGE_BASE_PPM_40568_MPPM_KINTANA_CBA.KWFL_STEP_TRANSACTIONS ;
#
# This table has the string literal 'NULL' in this column, not just '' or actual NULL
#
# Result:	NaN        NULL        returned NULL
#           ^          ^           ^     
#           |col1      |col2       |col3
#
# Col1 = Even afer regexp_replace yields '', this still cause STDDEV to return NaN
#---------------------------------------------------------------------------------------------------------------------------------


			#print ( itab3 +"Col#"+str(colNum)+":\t"+ tabcol.ljust(maxColLen) + "\t==> castCol1: \"" + castCol1 + "\"")
			#if debugVerbose: print ("\t==> Final Expression: \"" + castCol1 + "\"  ; newTabCol=" + newTabCol + ";  destTypeUpper="+destTypeUpper);
			#if debugVerbose: print (itab4 +	"Took: " + ElapsedTimeStrFrom(lookup2_StartTime,TIME2(), debugVerbose) +	\
			#				"  ==>  Filtered RowsCount: " + str(column_Type_LIST_count)    + ";\t"	+
			#				" Column Mapping: " + str(column_Type_LIST));

			print ("; Mapping: "+str(column_Type_LIST).ljust(maxMapLen), end='');

			#newTabCol="coalesce("   + tabcol +   ",'NULL','', " + tabcol +")";
			newTabCol = tabcol;
			# Don't print as it interferes with actual output
			# print ("============= newTabCol=" + newTabCol + ";  destTypeUpper="+destTypeUpper + "; sourceTypeUpper="+sourceTypeUpper);
			castCol1  = CastToCorrectHiveType (destTypeUpper, sourceTypeUpper,  newTabCol, itab2)

			print ("\t==> Final expression: \"" + castCol1 + "\";\tTook:" + ElapsedTimeStrFrom(lookup2_StartTime,TIME2()));

			tableColumnsStr    = tableColumnsStr+" "+tabcol;	# List of all table columnNames so far (not StatColumns)

				# Reason for using function coalesce() to '':
				# For JSON,   for null values we must provide either string 'NULL' or ''
				#    Without the string, JSON writer will NOT display that column.  
				# Hence the JSON output will be missing those columns
				# For ORACLE, for null values of NUMERIC we need to provide ''

### Remove this "cast(count()" as it is the SAME for all columns (it's a RowsCount)
###	cast(count("  + tabcol   + ")  as string) as TotalCount"     + str(colNum) + ",\n" \
### For     SMALL number of rows/columns this extra count is ok
### But for LARGE number of rows/columns this extra count is cost-prohibitive


	#NOTE: This function is used for REGULAR TABLE PROCESSING: ie, must have all 8 Stat columns for each TableColumn (in each batch)
	#      (For LARGE TABLE PROCESSING: there is a loop to allow a subset of Stat columns per Table Column; It will combine them correctly)

	#For every TableColum genereate 8 Stat columns:
	# Use coalesce as string(...) as the outermost function to force it NOT to produce the word NULL,
	# as the word NULL cause the write to JSON to miss columns.
	# Some aggregate function (such as AVG (), COUNT(), ..., never produces the value "NULL", but other function do
	#1st 4 Aggregates : apply all column types

#------------------------------------------------------------------------------------------------------------------------------------
#	# This is OLD logic, keep here for reference:
#				sql=sql + ",\n"                                                               \
#	+ " coalesce(cast(count(distinct("          + castCol1 + ")) as string), '' )  as UniqCount"      + str(colNum) + ",\n" \
#	+ "     coalesce (cast(sum(case when " + castCol1 + " is null then 1 else 0 end) as string), '') as NullCount" + str(colNum) + ",\n"        \
#	+ "     coalesce (cast(max("           + castCol1 + ")  as string), '' ) as MaxVal"   + str(colNum) + ",\n" \
#	+ "     coalesce (cast(min("           + castCol1 + ")  as string), '' ) as MinVal"   + str(colNum) + ","
#	
#		#2nd 4 columns:
#				if (destType=="DATE") or (destType=="TIMESTAMP"):
#					sql=sql + "\n"	\
#	+ "     \"\" as MeanVal"  + str(colNum) + ",\n"	\
#	+ "     \"\" as StdDev"   + str(colNum) + ",\n"	\
#	+ "     \"\" as MaxLen"   + str(colNum) + ",\n"	\
#	+ "     \"\" as MinLen"   + str(colNum)
#	
#						# Cannot use "N/A" with "" out due to error with MicroService:
#						#	+ "   \"N/A\" as MeanVal"  + str(colNum) + ",\n"	\
#	
#				elif (destType=="CLOB"):
#					sql=sql + "\n"	\
#	+ "     \"\" as MeanVal"  + str(colNum) + ",\n"	\
#	+ "     \"\" as StdDev"   + str(colNum) + ",\n"	\
#	+ "     coalesce (cast(max(length("    + castCol1 + ")) as string), '' ) as MaxLen"   + str(colNum) + ",\n"	\
#	+ "     coalesce (cast(min(length("    + castCol1 + ")) as string), '' ) as MinLen"   + str(colNum)
#	
#				else:
#					sql=sql + "\n"								\
#	+ "     coalesce (cast(avg("           + castCol1 + ")  as string), '' ) as MeanVal"  + str(colNum) + ",\n"	\
#	+ "     coalesce (cast(stddev("        + castCol1 + ")  as string), '' ) as StdDev"   + str(colNum) + ",\n"	\
#	+ "     coalesce (cast(max(length("    + castCol1 + ")) as string), '' ) as MaxLen"   + str(colNum) + ",\n"	\
#	+ "     coalesce (cast(min(length("    + castCol1 + ")) as string), '' ) as MinLen"   + str(colNum)
### if debugVerbose: print ("\n" + itab3 + "SQL of Current Batch=\n"  + sql + "\n")  
#
#NOTE: For the last 2 cols (MAX_LEN, MIN_LEN) there is NO casting
#Reason: if this column is numeric, Spark might convert it to a Exponentical number, and give incorrect length
# Ex 1: Numeric Value:  345678901234.2345      ==> Spark prints: 3.456789012342345E11
#       This length is: 17 chars               ==> This length is:      20  (due to the E11 at the end)
#
# For JSON,   for null values we must provide either string 'NULL' or ''
#    Without the string, JSON writer will NOT display that column.  
# Hence the JSON output will be missing those columns
# For ORACLE, for null values of NUMERIC we need to provide ''
#------------------------------------------------------------------------------------------------------------------------------------

			newSQL=BuildSqlFor1Column (destTypeUpper,sourceTypeUpper,castCol1, colNum, newSQL, itab4);
			colNum         += 1;	#Move it here since it has starting value
		#--}
		print ("\n"+itab2+"END LOOP on COLUMNS");

		sql=sql + "\nfrom " + registeredTABLE_DFname
		newSQL=newSQL + "\nfrom " + registeredTABLE_DFname

		tableColumnsStr = tableColumnsStr.lstrip();
		newColumnsList  = re.sub("[^\w]", " ",  tableColumnsStr).split();	#Convert to List 

		print ("\n" + "NewColumnsList       = "  + str(newColumnsList) + "\n")
		print ("\n" + "SQL of CURRENT Batch =\n" + newSQL + "\n");

		print (itab2 + "Took: " + ElapsedTimeStrFrom(startTime,TIME2(), debugVerbose) + \
				" to build this SQL for this SubColumsList.  Function BuildSQL_1Batch()\n\n");

	except EXCEPTION_COLUMNS_COUNT_MISMATCH:
		DisplayExceptionMessage (EXCEPTION_COLUMNS_COUNT_MISMATCH, tableNum, schema, table, "EXCEPTION_COLUMNS_COUNT_MISMATCH", "BuildSQL_1Batch()", itab2)
		print (itab2 + "Function BuildSQL_1Batch() raises: EXCEPTION_COLUMNS_COUNT_MISMATCH");
		print (itab1 + "<== Function BuildSQL_1Batch;   " + getLocalTimeOfCity() + ")\n")
		raise EXCEPTION_COLUMNS_COUNT_MISMATCH;

	except EXCEPTION_HIVE_TABLE_NOTFOUND_IN_ROADMASTER:
		DisplayExceptionMessage (EXCEPTION_HIVE_TABLE_NOTFOUND_IN_ROADMASTER, tableNum, schema, table,"EXCEPTION_HIVE_TABLE_NOTFOUND_IN_ROADMASTER", "BuildSQL_1Batch()", itab2)
		print (itab2 + "Function BuildSQL_1Batch() raises: EXCEPTION_HIVE_TABLE_NOTFOUND_IN_ROADMASTER");
		print (itab1 + "<== Function BuildSQL_1Batch;   " + getLocalTimeOfCity() + ")\n")
		raise EXCEPTION_HIVE_TABLE_NOTFOUND_IN_ROADMASTER;

	except EXCEPTION_HIVE_COLTYPE_NOTFOUND_IN_RM:
		DisplayExceptionMessage (EXCEPTION_HIVE_COLTYPE_NOTFOUND_IN_RM, tableNum, schema, table, "EXCEPTION_HIVE_COLTYPE_NOTFOUND_IN_RM", "BuildSQL_1Batch()", itab2)
		print (itab2 + "Function BuildSQL_1Batch() raises: EXCEPTION_HIVE_COLTYPE_NOTFOUND_IN_RM");
		print (itab1 + "<== Function BuildSQL_1Batch;   " + getLocalTimeOfCity() + ")\n")
		raise EXCEPTION_HIVE_COLTYPE_NOTFOUND_IN_RM;

	except Exception as expB1BSQL:
		DisplayExceptionMessage (expB1BSQL, tableNum, schema, table, "expB1BSQL", "BuildSQL_1Batch()", itab2)
		print (itab2 + "Function BuildSQL_1Batch() raises Exception: expB1BSQL");
		print (itab1 + "<== Function BuildSQL_1Batch;   " + getLocalTimeOfCity() + ")\n")
		raise expB1BSQL;

	print (itab1 + "<== Function BuildSQL_1Batch;\t\t" + getLocalTimeOfCity() + ")")
	return (newColumnsList, newSQL)
#--}
#---------------------------------------------------------------------------------------------------------------
#--{
def Process1BatchOfColumns (	tableNum, schema, table,  tableID, BATCH_ID, registeredTABLE_DFname,	\
								totalTABLERows,	ONE_ROADMASTER_TABLE_DF, ONE_ROADMASTER_TABLE_LIST,		\
								timestr,  subColumnsList, startColNum, batchNum,  DFColumnsBatchSize,	\
								STATCOLS_BATCH_SIZE,	comment,  APPROACH, debugVerbose=False,  leadingTabs=""):
	itab1=leadingTabs;
	itab2=itab1 + "\t";
	itab3=itab2 + "\t";
	itab4=itab3 + "\t";
	print (itab1 + "==> Function Process1BatchOfColumns;\t\t" + getLocalTimeOfCity() )
	print (itab2 +	"Table="          + table + ";  BatchNum = "    + str(batchNum)    +				\
			";  startColNum=" + str(startColNum) + ";   DFColumnsBatchSize=" + str(DFColumnsBatchSize) +	\
			";  STATCOLS_BATCH_SIZE=" + str(STATCOLS_BATCH_SIZE) );

	# RULE: 
	# REGULAR TABLE PROCESSING:	STATCOLS MUST   BE always 8; TABLE_COLS could be 1 .. DFColumnsBatchSize (configurable)
	# LARGE   TABLE PROCESSING:	STATCOLS could  BE 1..8;     TABLE_COLS could be 1 .. DFColumnsBatchSize (configurable)

	if APPROACH == 1:	# 8 is the maximum number of basic stats columns to collect.
				# Hence, 8 means do ALL 8 BasicStat columnns at once for all table columns in the Batch (DFColumnsBatchSize)
				# These  8 cols are from: dfSchemaForOutputTable

		# 	StructField("Total_count",       StringType(),  True),  <== This is NOT included as it is now calculated ONCE for entire table
		#		# (Previously this Total_Count is repeated for every TABLE COLUMN)
		# 1. StructField("unique_count",      StringType(),  True),  \
		# 2. StructField("null_count",        StringType(),  True),  \
		# 3. StructField("max_value",         StringType(),  True),  \
		# 4. StructField("min_value",         StringType(),  True),  \
		# 5. StructField("mean_value",        StringType(),  True),  \
		# 6. StructField("std_dev",           StringType(),  True),  \
		# 7. StructField("max_length",        StringType(),  True),  \
		# 8. StructField("min_length",        StringType(),  True),  \

		print ("\n\n"+itab2+"***** APPROACH 1: REGULAR TABLES PROCESSING:\n");
		try:	#--{
			columnsList,sql	= BuildSQL_1Batch (	tableNum, schema, table, registeredTABLE_DFname, totalTABLERows,	\
					ONE_ROADMASTER_TABLE_DF, ONE_ROADMASTER_TABLE_LIST, timestr, subColumnsList, startColNum,	\
					batchNum, DFColumnsBatchSize,	STATCOLS_BATCH_SIZE, comment, debugVerbose, itab2);

			oneDataRow	= Execute_1Batch  (	tableNum, schema, table, timestr, sql, batchNum,	\
								DFColumnsBatchSize, comment, debugVerbose, itab2)

			allColumnsList	= ExtractAndAppendToList_1Batch (	tableNum,    schema,       table,   totalTABLERows, timestr,	\
										columnsList, oneDataRow,   batchNum, DFColumnsBatchSize,	\
										comment,     debugVerbose, itab2)
		except Exception as expP1BCa:		#--}
			DisplayExceptionMessage (expP1BCa, tableNum, schema, table, "expP1BCa", "Process1BatchOfColumns()", itab1)
			print (itab1 + "Function Process1BatchOfColumns() reraises Exception: expP1BCa");
			print (itab1 + "<== Function Process1BatchOfColumns;   " + getLocalTimeOfCity() )
			raise expP1BCa;
	else:
		# This branch: Because the table are so large (like billion rows or more), it takes too much memory to do all BASIC STAT columns 
		#              for all TABLE COLUMNs in the Batch (DFColumnsBatchSize)
		# Solution:    for each TABLE column: do N-STAT colmnns (specified in STATCOLS_BATCH_SIZE) 

		print ("\n"+itab2+"***** APPROACH 2: LARGE TABLE PROCESSING:\n");
		try:	#--{
			totalSTATColumns	= 8;	# See above for reason of this value 8
										#Remember: Difference between TableColumns vs STATcolumns

			colName		= subColumnsList[0];	# <== [0] = MUST process only 1 TABLEColumn at a time.
			allColumnsList	= []
			oneColList	= [str(tableNum), schema,  tbl_nm,  colName,   str(totalTABLERows)];	#Initialize list

			print (	"\n" + itab2 + "LOOP over " + str(STATCOLS_BATCH_SIZE) + " STAT cols (out of Total " + str(totalSTATColumns) +	\
				" STAT columns) to calculate Basic Column statistics");
			STATColBatch=0;
			LEFT=0;
			while LEFT < totalSTATColumns:	#Loop over all STAT columns for current TABLE Column
				STATColBatch += 1;

				RIGHT = LEFT + STATCOLS_BATCH_SIZE-1;
				if RIGHT>= totalSTATColumns: RIGHT=totalSTATColumns-1
				print ("\n"+ itab2 + "STAT_ColBatch# " + str(STATColBatch) + " (of TABLEColumnBatch# " + str(batchNum) +	\
					"  for column " + colName+ ")   LEFT="+str(LEFT)+ "; RIGHT="+str(RIGHT) + ";  TotalElements=" + str(RIGHT-LEFT+1) );
				print ("\n"+ itab3 + "Step 1/3. Build SQL statement for this set of STAT columns (of this table column " + colName + "):")

				columnsList, sql = BuildSQL_1_SUB_Batch (	tableNum, schema, table, registeredTABLE_DFname,					\
															totalTABLERows,	ONE_ROADMASTER_TABLE_DF, ONE_ROADMASTER_TABLE_LIST,	\
															timestr, subColumnsList, startColNum, batchNum, DFColumnsBatchSize,	\
															STATCOLS_BATCH_SIZE,  LEFT, RIGHT, comment, debugVerbose, itab4);

				print ("\n"+ itab3+"Step 2/3. Execute this set of STAT columns:")

				#Can invoke the same function: Execute_1Batch() (used in the other IF branch) as it only requires SQL string, no schema yet
				oneDataRow = Execute_1Batch (	tableNum, schema, table, timestr, sql, batchNum,	\
												DFColumnsBatchSize, comment, debugVerbose, itab4)
				if debugVerbose:
					# oneDataRow is actually a Spark Row type, but Python reports as List
					print ("\n" + itab4+"Type of oneDataRow=" + str(type(oneDataRow)));	# Type of oneDataRow=<class 'list'>
					print (itab4+ "subColumnsList= "   + str(subColumnsList))
					print (itab4+ "oneDataRow= "       + str(oneDataRow))

				print ("\n"+ itab3+"Step 3/3. Loop to Extract/Append STAT columns (size=" + str(STATCOLS_BATCH_SIZE) + ") into oneColList: " +\
					";  LEFT=" + str(LEFT) + ";  RIGHT=" + str(RIGHT) + "\n")
				J=1;	#J starts at 0 because the ondDataRow starts at 0;  
				K=LEFT;	#K is to display accumulating index value
				while K <= RIGHT:
					V=" "
					if oneDataRow[0][J] != '': V=oneDataRow[0][J];
					oneColList.extend([V]);
					if debugVerbose: print ("["+ str(K) + "]=\"" + oneDataRow[0][J] + "\"\t==> " + str(oneColList) );
					K+=1;
					J+=1;

				LEFT=RIGHT+1;

			oneColList.extend ( [timestr]       );	# Must have [] around value; otherwise Python splits into individual chars
			oneColList.extend ( [comment]       );
			oneColList.extend ( [orig_file_name]);
			oneColList.extend ( [file_name]     );

			oneColTuple=tuple(oneColList);
			allColumnsList.append(oneColTuple);	# Format:  [ (a,b,c), (a,b,c), (a,b,c) ]
			if debugVerbose: 
				print ("\n\n" + itab2+"oneColList    = " + str(oneColList)    +"\n")
				print (         itab2+"allColumnsList= " + str(allColumnsList)+"\n")

		except Exception as expP1BCb:		#--}
			DisplayExceptionMessage (expP1BCb, tableNum, schema, table, "expP1BCb", "Process1BatchOfColumns()", itab1)
			print (itab1 + "Function Process1BatchOfColumns() reraises Exception: expP1BCb");
			print (itab1 + "<== Function Process1BatchOfColumns;   " + getLocalTimeOfCity() )
			raise expP1BCb;
	print (itab1 + "<== Function Process1BatchOfColumns;   " + getLocalTimeOfCity() )
	return allColumnsList;
#--}
#---------------------------------------------------------------------------------------------------------------
#--{
def Create_DFFor1RoadMasterTable (	tableNum, schema, table, tableID, BATCH_ID, URL_GetTableByID,
					topNrows, debugVerbose=False, leadingTabs=""):
	# Purpose: For a given Table, This MicroService will return all cols from RoadMaster Database
	itab1=leadingTabs;
	itab2=itab1 + "\t";
	itab3=itab2 + "\t";
	itab4=itab3 + "\t";

	#EX: url = "https://edge-gateway.cp-epg2i.domain.com/roadmaster/tables/0B7B85EC69513C6978A0EB8CD88FD0CF05460A11?hiveType=true"

	### DEBUG  bad TableID
	### Make TableID invalid to prove the fact that the MicroSerice raises exception
	### if tableNum%2==1: tableID=tableID + "_____XXXXX_____DEBUG_____"
	### tableID=tableID + "_____xxxxxxxxx_DEBUG_xxxxxxxxx";

	url = URL_GetTableByID + "/" + tableID + "?hiveType=true"

	headers = {"Content-Type":'application/json',"Cache-Control": "no-cache"} #if requesting json object
		# No need Authorization in header as this is a READ-ONLY.  For INSERT/UPDATE then must provide Authorization

	print (itab1 + "==> Create_DFFor1RoadMasterTable;\t\t" + getLocalTimeOfCity() )
	print (itab2 + "Schema           = " + schema);
	print (itab2 + "Table            = " + table);
	print (itab2 + "BATCH_ID         = " + str(BATCH_ID) )
	print (itab2 + "Table_ID         = " + tableID	);
	print (itab2 + "URL_GetTableByID = " + URL_GetTableByID +"\n");
	print (itab2 + "url     = \"" + url + "\"");
	print (itab2 + "headers = " + str(headers) + "\n" + itab3 + "(No need to specify Authorization as this is Read-Only)\n");

	startTime  = TIME2();
	try:
		print (itab2 + "Step A: Extracting Response Object for table: " + schema + "." + table);
		response = requests.get(url, headers=headers)
	except Exception as expCrDF1MS_1:
		DisplayExceptionMessage (expCrDF1MS_1, tableNum, schema, table, "expCrDF1MS_1", "Create_DFFor1RoadMasterTable()", itab2)
		print ("\n\n" + itab2 + "********** ERROR: RoadMaster MicroService Failed while performing Step1 for Table_ID = \"" + tableID  + "\"");
		print (         itab1 + "<== Function Create_DFFor1RoadMasterTable;\t\t" + getLocalTimeOfCity() );
		raise;

	try:
		print (itab2 + "Step B: Extracting Json Data object to extract Table Info:");
		data = response.json() #If expecting json.
			# This is where it raises exception if tableID is not found.
			# It should return 404 for not found, but it raises exception instead
			# This probably has to do with url connecting to gateway intead of actual PCF server
			# So keep this Step2 exception at the outermost 

		try:
			print (itab2 + "Step C: Extracting Status_Code from Response object");
			status_code = response.status_code #200 code means 'ok'. anything else is an exception
			print (itab3 +	"Status_code: "+str(status_code) + "; dataLen:"+ str(len(data)) + 	\
					";  MicroService Took: " + ElapsedTimeStrFrom(startTime,TIME2(), debugVerbose) )
			if debugVerbose: print (itab3 + "JSON Data Output = " + str(data) + "\n");
		except Exception as expCrDF1MS_3:
			DisplayExceptionMessage (expCrDF1MS_3, tableNum, schema, table, "expCrDF1MS_3", "Create_DFFor1RoadMasterTable()", itab2)
			print (itab2 + "***** ERROR: RoadMaster MicroService Failed to Retrieve Columns (at Step3)\n\n");
			print (itab1 + "<== Function Create_DFFor1RoadMasterTable;\t\t" + getLocalTimeOfCity() );
			raise;

		if status_code == 200:
			print ("\n" + itab3 + "==> MicroService Return Status: SUCCESSFUL; (at Create_DFFor1RoadMasterTable())  status_code=" + str(status_code) + "\n\n");

			#  200 means successful, but could be empty
			print (itab2 + "Step D: Build: List of Dict objects:"); # [ {ColName1:HiveColType1}, {ColName1:HiveColType1}, ... ]

			HiveSchemaName= data[u'tgtTableBaseDb'].upper();	#Column From MicroService
			HiveTableName = data[u'tgtTableNm'].upper();		#Column From MicroService

			ListOfColumns = [ (u'N/A', HiveSchemaName, HiveTableName, v[u'tgtColNm'].upper(),v[u'srcColDataType'].upper(),v[u'hiveDataType'].upper(), v[u'colId'], tableID) for v in data[u'columns'] ]

			counts = len(ListOfColumns)
			if debugVerbose: 
				print (itab3+"Type of ListOfColumns: "+str(type(ListOfColumns))+";     Number of Columns: "+str(counts) + "\n")
				print (itab3+"ListOfColumns=" + str(ListOfColumns) + "\n\n")
	
			df_Name="ONE_ROADMASTER_TABLE_DF";
			print (itab2 + "Step E: Convert List to DataFrame\n"); # like: [ {ColName1:HiveColType1}, {ColName1:HiveColType1}, ... ]
			rdd = spark.sparkContext.parallelize(ListOfColumns, 1).map (lambda x: Row(x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7]));
			newDF=spark.createDataFrame (rdd, dfSchemaForHiveColumnTypes)
			print (itab3 + "rdd.count()=" + str(rdd.count()) + " rows;  newDF.count()=" + str(newDF.count()) + " rows")
			if rdd.count() != newDF.count():
				print("\n\n\n***** ERROR: Counts mismatch!\n\n\n")
				raise;	#Raise to cause it to go back to caller and continue Loop for next table

			if debugVerbose: DisplayTopNrows (newDF, topNrows, itab2, df_Name)
		else:
			print ("\n\n" + itab2 + "***** ERROR: RoadMaster MicroService failed at Step3;  Status Code = " +	\
					str(status_code) +  "; dataLen:"+ str(len(data)) + "\n\n");
			print (itab2 + "==> Spark Exits with code -1 -- JOB FAIL\n\n")
			print (itab1 + "<== Function Create_DFFor1RoadMasterTable;   " + getLocalTimeOfCity() )
			raise;	#Raise to cause it to go back to caller and continue Loop for next table

	except Exception as expCrDF1MS_2:
		DisplayExceptionMessage (expCrDF1MS_2, tableNum, schema, table, "expCrDF1MS_2", "Create_DFFor1RoadMasterTable()", itab2)
		print ("\n\n"+itab2 + "***** ERROR: RoadMaster MicroService Failed while performing Step2 for Table_ID = \"" + tableID  + "\"");
		print (       itab2 + "This usually means that it CANNOT find this TableID in RoadMaster using the above URL!\n");
		print (       itab1 + "<== Function Create_DFFor1RoadMasterTable;\t\t" + getLocalTimeOfCity() );
		raise;

	print (itab1 + "<== Function Create_DFFor1RoadMasterTable;\t\t" + getLocalTimeOfCity() +"\n" )
	return newDF  ;
#--}
#---------------------------------------------------------------------------------------------------------------
#--{
def Process1NewTable (	tableNum,  schema,   table,  tableID,  BATCH_ID,  registeredTABLE_DFname,  tableRowsCount,	\
			timestr,   allTableColumns,           DFColumnsBatchSize,             statusIfSUCCESSFUL,	\
			BIG_TABLE_ROWS_TO_SWITCH,  BIG_TABLE_TABLECOLS_BATCH_SIZE, BIG_TABLE_STATCOLS_BATCH_SIZE,	\
			URL_GetTableByID,	   ONE_ROADMASTER_TABLE_DF, ONE_ROADMASTER_TABLE_LIST,			\
			debugVerbose=False,        leadingTabs=""):

	itab1=leadingTabs;
	itab2=itab1 + "\t";
	itab3=itab2 + "\t";
	itab4=itab3 + "\t";
	print (itab1 + "==> Function Process1NewTable;\t\t" + getLocalTimeOfCity() )
	if debugVerbose:
		print (itab2 + "tableNum = " + str(tableNum));
		print (itab2 + "schema   = " + schema);
		print (itab2 + "table    = " + table);
		print (itab2 + "tableID  = " + tableID);
		print (itab2 + "BATCH_ID = " + BATCH_ID);
		print (itab2 + "tableRowsCount = " + str(tableRowsCount));
		print (itab2 + "BIG_TABLE_ROWS_TO_SWITCH	= " + str(BIG_TABLE_ROWS_TO_SWITCH) );
		print (itab2 + "BIG_TABLE_TABLECOLS_BATCH_SIZE	= " + str(BIG_TABLE_TABLECOLS_BATCH_SIZE) );
		print (itab2 + "BIG_TABLE_STATCOLS_BATCH_SIZE	= " + str(BIG_TABLE_STATCOLS_BATCH_SIZE)  );
		print (itab2 + "statusIfSUCCESSFUL		= " + statusIfSUCCESSFUL +"\n");

	lookup1StartTime  = TIME2();  
	##### print (itab2+"1. Get DataTypes for Columns of this 1 table From RoadMaster Table:")
	##### print (itab2+"1. Get DataTypes for Columns of this 1 table From MicroService:")
	cond1 =  col("Hive_Schema_Nm" ) == schema ;
	cond2 =  col("Table_Nm"  )      == table ;

#               [print (ASMS +"\t|\t" + Hive_Schema_Nm+ "\t|\t"+ Table_Nm+"\t|\t"+Column_Nm+"\t|\t"+SourceType+"\t|\t"+ HiveType) \
#                       for (ASMS,Hive_Schema_Nm,Table_Nm,Column_Nm,SourceType,HiveType)  in  one_table_LIST ]
#                       # See: def CreateSchemaForHiveColumnTypes() for these column names
#
# This ListComprehension works only if ALL columns in the tuple are available.  If any colunns are missing it gives this error:
#                       unsupported operand type(s) for +: 'NoneType' and 'str'

	#-------------------------------------------------------------------------------------------------------------------------------------
	# Explanation:
	#	DFColumnsBatchSize            = how many TABLE columns      (of curr table)  to process at a time
	#	BIG_TABLE_STATCOLS_BATCH_SIZE = how many BASIC STAT columns (of each Column) to process at a time

	# At this point, for SOME table even if it is defined i Hive, the user does not actually create the HDFS directory
	# Ex: Table DL_EDGE_BASE_GCRM_45735_BASE_VOCOBIP_VOCOBI_ETL.W_USER_D_20160511123906
	# Its location is (from Hive SHOW CREATE TABLE command):
	#	LOCATION
	#	  'hdfs://edwbidevwar/DEV/DL/EDGE_BASE/GCRM-45735/VOCOBIP-VOCOBI_ETL/W_USER_D_20160511123906/Data'
	# But when tries to display it : hadoop fs -ls .... it says:  "No such file or directory"
	# This further cause error from Spark.sql(sql) command:
	#	An error occurred while calling o403.collectToPython.
	#	: org.apache.spark.sql.catalyst.errors.package$TreeNodeException: execute, tree:
	#	Exchange SinglePartition
	#	+- *HashAggregate(keys=[], functions=[partial_count(1)], output=[count#864L])
	#	   +- HiveTableScan MetastoreRelation dl_edge_base_gcrm_45735_base_vocobip_vocobi_etl, w_user_d_20160511123906
	#	.......
	# Caused by: org.apache.hadoop.mapred.InvalidInputException: Input path does not exist: hdfs://edwbidevwar/DEV/DL/EDGE_BASE/GCRM-45735/VOCOBIP-VOCOBI_ETL/W_USER_D_20160511123906/Data
	#	.......
	#-------------------------------------------------------------------------------------------------------------------------------------

	print (itab2 +"1.  ==> RowsCount = "  + str(tableRowsCount)  + " rows (for " +   schema+"."+table+")\t\t" + getLocalTimeOfCity());
	print (itab3 +"BIG_TABLE_ROWS_TO_SWITCH = " + str(BIG_TABLE_ROWS_TO_SWITCH) + " rows\n");

	if tableRowsCount > BIG_TABLE_ROWS_TO_SWITCH:
		DFColumnsBatchSize     = 1	#How many TablesColumns per batch:  1 column and up to max
			#(Spark seems to hang or out of space if table is too large and too many columns and stat columns performing at one time.)
			#(So set DFColumnsBatchSize=1 to do 1 column at a time)

				# For LARGE tables, each TableColumn may compute 1 to 8 STAT columns.
				# There will be a loop to combine them into list of 8 STAT columns
		STATCOLS_BATCH_SIZE=BIG_TABLE_STATCOLS_BATCH_SIZE;	#User can specify this at command line
		APPROACH=2
		print (itab3+"==> Use LARGE TABLE PROCESSING APPROACH (ie, select N-STAT columns for 1 TableColumn at a time)")

	else:
		# For NON-LARGE tables, each TableColumn must compute ALL 8 STAT columns
		STATCOLS_BATCH_SIZE=8
		APPROACH=1
		print (itab3+"==> Use REGULAR TABLE PROCESSING APPROACH (ie, select ALL STAT columns for batch of TableColumns at a time)")

	newDFColumnsBatchSize = ChooseTableColumnBatchSize (tableRowsCount);


	print (itab4+"Set these variables:");
	print (itab4+"a) TABLES Columns BatchSize (DFColumnsBatchSize)  = " + str(newDFColumnsBatchSize)   );
	print (itab4+"b) STAT   Columns BatchSize (STATCOLS_BATCH_SIZE) = " + str(STATCOLS_BATCH_SIZE)  );
	print (itab4+"c) ==> APPROACH             = " + str(APPROACH)  + "\n")

	AllTableColumnsDataList = []
	totalColumns=len(allTableColumns);

	maxpt=totalColumns-1;   # max pointer = length-1 due to zero-based
	l=-1;                   # left  pointer for every bacth
	r=-1;                   # right pointer for every bacth

	print (itab2 +	"2.  ======= PURPOSE: Split TABLE_Columns into Batches (Each batch="+str(DFColumnsBatchSize)	+ \
			" TABLEColumns), and sequentially process 1 batch at time" );
	print (itab3 +	"=======	Table has		= " + str(totalColumns)  + " columns;");

	b1=totalColumns//newDFColumnsBatchSize;	# integer division
	totalBatches=b1;						#
	m1=totalColumns%newDFColumnsBatchSize;		# modulus
	if m1 > 0: totalBatches=b1+1;			# If there are any remainder, then that's 1 additional  batchsize




	print (itab3 +	"=======	TableColumnsBatchSize	= " + str(newDFColumnsBatchSize)	);
	print (itab3 +	"=======		b1		= " + str(b1)	);
	print (itab3 +	"=======		m1		= " + str(m1)	);
	print (itab3 +	"=======		totalBatches	= " + str(totalBatches)	);
	print (itab3 +	"=======	STATCOLS_BATCH_SIZE	= " + str(STATCOLS_BATCH_SIZE)	);

	startColNum =  1;
	batchNum    =  0;
	r           = -1;
	print ("\n\n" + itab3 +	"LOOP thru Batches of TABLE_COLUMNS  (BATCH OUTER LOOP):" );
	print (itab3 +"#--{")
	while (r < maxpt):	#--{	Loop thru all Batches of columns
		startColNum = (newDFColumnsBatchSize*batchNum)+1;	#initial:  batch.  1st Column of new batch
		batchNum    = batchNum+1;
		l           = r+1;			# l = left pointer
		r           = l+newDFColumnsBatchSize-1;	# r = right pointer
		if (r>=maxpt):  r=maxpt;		# Move right pointer past limit to stop WHILE loop

		subColumnsList=allTableColumns[ l : l + newDFColumnsBatchSize];	#Slice

		#It's OK to go past the right as Python will limit to the last element
		print ("\n"+itab4 +	"===== 2A. TABLE_Columns BATCH #" + str(batchNum)				+ \
					";  l=" + str(l) + "; r="+ str(r) + ";  Length:" + str(len(subColumnsList))	+ \
					";  newDFColumnsBatchSize=" + str(newDFColumnsBatchSize) + "\t\t" + getLocalTimeOfCity()) 
			# At the last batch, these 2 values (l and r) might be different: len(subColumnsList)  and newDFColumnsBatchSize

		if debugVerbose: print (itab4+ "List of TableColumns in this Batch = " + str(subColumnsList) + "\n")
				#  Must apply str(subColumnsList); other this error: Can't convert 'list' object to str implicitly

		try:	# --{
			OneBatchOfTableColumnsDataList   = Process1BatchOfColumns (							\
				tableNum, schema, table, tableID, BATCH_ID, registeredTABLE_DFname, tableRowsCount,			\
				ONE_ROADMASTER_TABLE_DF, ONE_ROADMASTER_TABLE_LIST, timestr, subColumnsList, startColNum, batchNum,	\
				newDFColumnsBatchSize, STATCOLS_BATCH_SIZE, statusIfSUCCESSFUL, APPROACH, debugVerbose, itab4)

			# This variable, OneBatchOfTableColumnsDataList,  contains all columns matching definition dfSchemaForOutputTable 

		except Exception as exp1NewTable:		#--}
			DisplayExceptionMessage (exp1NewTable, tableNum, schema, table, "exp1NewTable", "Process1NewTable()", itab2)
			print (itab1 + "Function Process1NewTable() reraises Exception: exp1NewTable", itab1);
			print (itab1 + "<== Function Process1NewTable;   " + getLocalTimeOfCity() )
			raise;	# Let the caller decides what to do

		print ("\n"+itab3+"Append this output List to Master list:");
		AllTableColumnsDataList = AllTableColumnsDataList + OneBatchOfTableColumnsDataList #Appending to master list
		if debugVerbose: print ("\n"+itab3+"AllTableColumnsDataList=" + str(AllTableColumnsDataList)+"\n");
	#--}
	print (itab2 +	"END OF BATCH OUTERLOOP\n" + itab2 + "#--}\t\t"            + getLocalTimeOfCity() +"\n");
	print (itab1 + "<== Function Process1NewTable;\t\t(return List)\t\t" + getLocalTimeOfCity() )
	return AllTableColumnsDataList
#--}
#---------------------------------------------------------------------------------------------------------------
#--{
def CreateInitialDataFrames (	inputTablesDF_Name, debugVerbose=False, leadingTabs=""):
	itab1=leadingTabs;
	itab2=itab1 + "\t";
	itab3=itab2 + "\t";
	if debugVerbose: print ("\n\n\n\n" + itab1 + "==> Function CreateInitialDataFrames;\t\t" + getLocalTimeOfCity() )

	#------------------------------------------------------------------------------------------
	#   comment="SUCCESS"				
	#   comment="FAILED"				
	#   comment="FAILED (THRESHOLD EXCEEDED)" 		
	#   comment="TABLE NOT FOUND IN HIVE"		Can rerun now as table might be available now
	#   comment="TABLE NOT FOUND IN ROADMASTER"	Can rerun now as table might be available now
	#   comment="SKIP (DATA FROM PREV CKP)";  		???
	#------------------------------------------------------------------------------------------
	# This run should only consist of those table that are NOT in SUCCESS Chkpoint Dir
	# ==> Use LEFT Join to Eliminate those tables 

	#tablesToRerun_SQL ="SELECT row_number()  over (order by schema asc,table asc) as TableNum, I.schema, I.table, I.tableID, \"\" as Comment\n"	+	\
	#	"FROM  " + inputTablesDF_Name	+ " I";
	tablesToRerun_SQL ="SELECT row_number()  over (order by schema asc,table asc) as TableNum, I.schema, I.table, I.tableID, I.fileSize, \"\" as Comment\n"	+	\
		"FROM  " + inputTablesDF_Name	+ " I";

	TABLES_TO_RERUN_DF      = spark.sql(tablesToRerun_SQL);	#Execute sql
	TABLES_TO_RERUN_DF_NAME = "TABLES_TO_RERUN_DF"
	TABLES_TO_RERUN_DF.createOrReplaceTempView (TABLES_TO_RERUN_DF_NAME);

	if debugVerbose:
		print (itab2+ "========== 1) List of tables To RERUN:");
		DisplayTopNrows (TABLES_TO_RERUN_DF, -1, itab2, "TABLES_TO_RERUN_DF")

	if debugVerbose: print ("\n"+itab1 + "<== Function CreateInitialDataFrames;\t\t" + getLocalTimeOfCity() + ")\n\n")
	return (TABLES_TO_RERUN_DF);
#--}
#---------------------------------------------------------------------------------------------------------------
#--{
def CreateOutputForZeroRow (	tableNum, totalTables, schema, table, tbl_nm, tableID, batchID,					\
								ColumnsList, RowsCount,	tableStartTime, Orignal_File_Name,  Split_File_Name,	\
								LIST_OF_TABLE_STATUS_COUNTS, debugVerbose=False, topNrows=0, leadingTabs=""):
	itab1=leadingTabs;
	itab2=itab1 + "\t";
	itab3=itab2 + "\t";
	print (itab1 + "==> Function CreateOutputForZeroRow;\t\t" + getLocalTimeOfCity() )
	print (itab2 + "RowsCount   = " + str(RowsCount) );
	print (itab2 + "ColumnsList = " + str(ColumnsList) );
	print ("\n");

	FinalOutputList=[]
	colNum=0;
	try:
		for tabcol in [c for c in ColumnsList ]:	#--{
			colNum += 1;
			# Create output List for 1 column. This columns should match schema: dfSchemaForOutputTable

			# 17 columns:  This schema is for the Original JSON output.  
			#dfSchemaForOutputTable = StructType     (
			#	"Table_Num",         "Schema_Name",       "table_name",        "columname",         
			#	"Total_count",       "unique_count",      "null_count",        "max_value",         
			#	"min_value",         "mean_value",        "std_dev",           "max_length",        
			#	"min_length",        "ins_gmt_ts",        "Comment",           "Orignal_File_Name", 
			#	"Split_File_Name",   

			OutputFor1Col = (	tableNum,   schema, table,  tabcol,				\
						RowsCount,  "",   "",    "",   "",   "",    "",   "",     "",	\
						tableStartTime, "SUCCESS",      Orignal_File_Name,  Split_File_Name);

			print ( itab2 + "Hive Col #"+str(colNum) + ") " + tabcol + ": " + str(OutputFor1Col))
			FinalOutputList.append(OutputFor1Col);
	except:
		raise;
	#--}
	if debugVerbose:
		print ("\n");
		print (itab2, "Final Output List");
		print (itab2, FinalOutputList);	
		print ("\n");
	print (itab1 + "<== Function CreateOutputForZeroRow;\t\t" + getLocalTimeOfCity() );
	return FinalOutputList;
#--}
#---------------------------------------------------------------------------------------------------------------
def AddTableStatus_to_TIME_SUMMARY_LIST (	TIME_SUMMARY_LIST, STATUS,										\
											RM_TotalColumnsStr, HIVE_TotalColumnsStr, tableRowsCountStr,	\
											tableStartT1,				\
											tableStartTime,				\
											OutBoundMS1_Elapsed,		\
											HiveCheckTable_Elapsed,		\
											HiveRowsCount_Elapsed,		\
											HiveGetColumns_Elapsed,		\
											RoadMasterMS_Elapsed,		\
											ColsFilter_Elapsed,			\
											Profile_Elapsed,			\
											InsertOutputMS_Elapsed,		\
											UpdateQueueMS_Elapsed,		\
											OutBoundMS2_Elapsed,		\
											debugVerbose=False, topNrows=0, leadingTabs=""):
	itab1=leadingTabs;
	itab2=itab1 + "\t";
	itab3=itab2 + "\t";
	itab4=itab3 + "\t";
	if debugVerbose: print (itab1 + "==> Function AddTableStatus_to_TIME_SUMMARY_LIST;\t\tSTATUS=" + STATUS + "\t\t" + getLocalTimeOfCity() )
	try:

		# The order of these columns must match schema: dfSchemaForTableTimeSummary 
		row=Create1TableTimeSummaryList (	tableNum, schema, table, tableID, BATCH_ID, STATUS,				\
											tableRowsCountStr, RM_TotalColumnsStr, HIVE_TotalColumnsStr,	\
											tableStartT1, tableStartTime,	\
											OutBoundMS1_Elapsed,			\
											HiveCheckTable_Elapsed,			\
											HiveRowsCount_Elapsed,			\
											HiveGetColumns_Elapsed,			\
											RoadMasterMS_Elapsed,			\
											ColsFilter_Elapsed,				\
											Profile_Elapsed,				\
											InsertOutputMS_Elapsed,			\
											UpdateQueueMS_Elapsed,			\
											OutBoundMS2_Elapsed,			\
											debugVerbose, topNrows, itab2);
		TIME_SUMMARY_LIST.append (row);
		if debugVerbose:
			print (itab2 + "Type of row  = " + str(type(row)));
			print (itab2 + "Value of row = " + str(row));

	except:
		print("\n\n\n" + itab1 + "********** ERROR: at AddTableStatus_to_TIME_SUMMARY_LIST(): " +	\
			str(sys.exc_info()[0]) + "\n\n\n")
		print (itab1 + "<== Function AddTableStatus_to_TIME_SUMMARY_LIST;\t\t" + getLocalTimeOfCity() )
		raise;

	if debugVerbose: print (itab1 + "<== Function AddTableStatus_to_TIME_SUMMARY_LIST;\t\t" + getLocalTimeOfCity() )
#---------------------------------------------------------------------------------------------------------------
#--{
def GetColumnsFor1TableFromRoadMaster (	tableNum, totalTables, schema, table, tbl_nm, tableID,  BATCH_ID,	\
										URL_GetTableByID, URL_UpdateQueue,	TIME_SUMMARY_LIST,				\
										tableStartTimeT1_ForMS, tableStartTimeT1, tableStartTimeT2,			\
										LIST_OF_TABLE_STATUS_COUNTS, debugVerbose=False, topNrows=0, leadingTabs=""):
	itab1=leadingTabs;
	itab2=itab1 + "\t";
	itab3=itab2 + "\t";
	print (itab1 + "==> Function GetColumnsFor1TableFromRoadMaster;\t\t" + getLocalTimeOfCity() )
	print (itab2 + "TABLE_ID         = " + tableID);
	print (itab2 + "BATCH_ID         = " + BATCH_ID + ";  tableNum = " +str(tableNum)+";  totalTables = "+str(totalTables));
	print (itab2 + "schema           = " + schema);
	print (itab2 + "table            = " + table);
	print (itab2 + "tbl_nm           = " + tbl_nm);
	print (itab2 + "URL_GetTableByID = " + URL_GetTableByID);
	print (itab2 + "URL_UpdateQueue  = " + URL_UpdateQueue);

	try:
		RoadMasterMS_StartTime = TIME2();	#time()		Type: Float
		print ("\n");
		ONE_ROADMASTER_TABLE_DF  = Create_DFFor1RoadMasterTable (tableNum, schema, table, tableID, BATCH_ID,	\
									URL_GetTableByID, 10, debugVerbose, itab2);

		# This DataFrame ONE_ROADMASTER_TABLE_DF  should contain all columns of this table in RoadMaster.
		# Ideally it should be same as Hive table (minus the DW columns)
		# But for some they could be different. 


# Content of DataFrame: ONE_ROADMASTER_TABLE_DF  (Display ALL: 91 rows)
# +----+----------------------------------------------+-----------+-----------------+----------+--------+---------------+---------------+
# |ASMS|Hive_Schema_Nm                                |Table_Nm   |Column_Nm        |SourceType|HiveType|ColId          |TabldId        |
# +----+----------------------------------------------+-----------+-----------------+----------+--------+---------------+---------------+
# |N/A |DL_EDGE_BASE_PRTS_13123_BASE_PRTS000P_PRTS_DYN|LN_DATA_RAW|DSE2COMMENTS     |CLOB      |CLOB    |3BC612DCDC7E479|AA6CFA82E45FC39|

		if (ONE_ROADMASTER_TABLE_DF.count()==0):
			print (itab2 +	"DF count=0.   Normally should have some rows returned, not 0 rows.");
			print (itab2 +	"But if RoadMaster does not have this table/column and if option "	+ \
					"OverWrite (-o) is specified on command line then this 0-count is OK.");

		RoadMasterMS_Elapsed = str(int(math.ceil(TIME2() - RoadMasterMS_StartTime) ) )+ " secs" ;	#Seconds

	except: 
		Increment_TABLENOTFOUND_IN_RM_COUNTS (LIST_OF_TABLE_STATUS_COUNTS);
		print ("\n\n" + itab1 + "********** ERROR: at GetColumnsFor1TableFromRoadMaster(): Exception: " + \
				str(sys.exc_info()[0])  + "\n\n" );
		print (itab1 + "<== Function GetColumnsFor1TableFromRoadMaster;\t\t" + getLocalTimeOfCity() )
		raise;
	#--} try:  
	print (itab1 + "<== Function GetColumnsFor1TableFromRoadMaster;\t\t" + getLocalTimeOfCity() )
	return ONE_ROADMASTER_TABLE_DF  ;
#--}
#---------------------------------------------------------------------------------------------------------------
#--{
#  NOT CURRENTLY USE
#  WIll use this DEF once i incorporate the Get BATCH of roadmaster:
def FilterHiveColumnsAgainstRoadMasterColumns (	tableNum, totalTables, schema, table, tbl_nm,		\
				tableID,  BATCH_ID,	ONE_ROADMASTER_TABLE_DF, URL_UpdateQueue,	TIME_SUMMARY_LIST,	\
				tableStartTimeT1_ForMS, tableStartTimeT1,tableStartTimeT2,							\
				LIST_OF_TABLE_STATUS_COUNTS, debugVerbose=False, topNrows=0, leadingTabs=""):

	itab1=leadingTabs;
	itab2=itab1 + "\t";
	itab3=itab2 + "\t";
	print (itab1 + "==> Function FilterHiveColumnsAgainstRoadMasterColumns;\t\t" + getLocalTimeOfCity() )
	print (itab2 + "TABLE_ID    = " + tableID);
	print (itab2 + "BATCH_ID    = " + BATCH_ID + ";  tableNum = " + str(tableNum) + ";  totalTables = " + str(totalTables));
	print (itab2 + "schema      = " + schema);
	print (itab2 + "table       = " + table);
	print (itab2 + "tbl_nm      = " + tbl_nm);


	try:
		cond1 =  (col("Hive_Schema_Nm" ) == schema.lower() ) | (col("Hive_Schema_Nm" ) == schema.upper() );
		cond2 =  (col("Table_Nm")        == table.lower()  ) | (col("Table_Nm" )       == table.upper() );
		ONE_ROADMASTER_TABLE_LIST  = ONE_ROADMASTER_TABLE_DF.filter( cond1  & cond2 ).collect();  #Get all columns for this table
			#Note:   collect() turns DataFrame into List (not DataFrame anymore)

		if debugVerbose:
			print (itab3 + "Content of ONE_ROADMASTER_TABLE_LIST:  (Should be same as ONE_ROADMASTER_TABLE_DF above)"    )
			print (str(ONE_ROADMASTER_TABLE_LIST) +"\n\n");	# Long line, so print at beginning of line. no itabN

		count_of_LIST = len(ONE_ROADMASTER_TABLE_LIST)
		count_of_DF   = ONE_ROADMASTER_TABLE_DF.count()

		RM_TotalColumnsStr=str( count_of_DF  ) ;
		print (itab2 + "Count of columns in ONE_ROADMASTER_TABLE_LIST:  " + str( count_of_LIST) )
		print (itab2 + "Count of columns in ONE_ROADMASTER_TABLE_DF:    " + str( count_of_DF  ) )

		if count_of_LIST != count_of_DF:
			print ("\n\n\nERRROR:  These 2 counts cannot be different.\n\n")
			raise;
	except:
		print ("\n\n\n********** ERROR: at FilterHiveColumnsAgainstRoadMasterColumns(): Exception: " + \
				str(sys.exc_info()[0])  + "\n\n\n" );
		print (itab1 + "<== Function FilterHiveColumnsAgainstRoadMasterColumns;\t\t" + getLocalTimeOfCity() )
		raise;

	print (itab1 + "<== Function FilterHiveColumnsAgainstRoadMasterColumns;\t\t" + getLocalTimeOfCity() )
	return ONE_ROADMASTER_TABLE_LIST;
#--}
#---------------------------------------------------------------------------------------------------------------
#--{
def CheckIfTableExistsInHIVE (	tableNum, totalTables, schema, table, tbl_nm, tableID,  BATCH_ID,				\
								TIME_SUMMARY_LIST, tableStartTimeT1_ForMS, tableStartTimeT1,tableStartTimeT2,	\
								LIST_OF_TABLE_STATUS_COUNTS, debugVerbose=False, topNrows=0, leadingTabs=""):
	itab1=leadingTabs;
	itab2=itab1 + "\t";
	itab3=itab2 + "\t";
	print (itab1 + "==> Function CheckIfTableExistsInHIVE;\t\t" + getLocalTimeOfCity() )
	if debugVerbose:
		print (itab2 + "TABLE_ID    = " + tableID);
		print (itab2 + "BATCH_ID    = " + BATCH_ID);
		print (itab2 + "schema      = " + schema);
		print (itab2 + "table       = " + table);
		print (itab2 + "tableNum    = " + str(tableNum) );
		print (itab2 + "totalTables = " + str(totalTables));
		print (itab2 + "tbl_nm      = " + tbl_nm);

	try:	# --{ try: for each Table
		tbl = spark.table(tbl_nm);	# Type: org.apache.spark.sql.DataFrame
		print (itab2 + "==> Table FOUND in Hive Database")

	except:
		Increment_TABLENOTFOUND_IN_HIVE_COUNTS (LIST_OF_TABLE_STATUS_COUNTS);
		print ("\n\n\n********** ERROR: at CheckIfTableExistsInHIVE(): Exception: " + \
				str(sys.exc_info()[0])  + "\n\n\n" );
		raise;

	#--}
	print (itab1 + "<== Function CheckIfTableExistsInHIVE;\t\t" + getLocalTimeOfCity() )
	return tbl;
#--}
#---------------------------------------------------------------------------------------------------------------
#--{
def GetHIVEColumns (	tableNum, totalTables, schema, table, tbl_nm, tableID,   BATCH_ID,	\
						tbl ,  registeredDFname, TIME_SUMMARY_LIST, tableStartTimeT1_ForMS,	\
						tableStartTimeT1, tableStartTimeT2,  LIST_OF_TABLE_STATUS_COUNTS,	\
						debugVerbose=False,   topNrows=0,    leadingTabs=""):
	itab1=leadingTabs;
	itab2=itab1 + "\t";
	itab3=itab2 + "\t";

	print (itab1 + "==> Function GetHIVEColumns;\t\t" + getLocalTimeOfCity() )
	print (itab2 + "TABLE_ID    = " + tableID);
	print (itab2 + "BATCH_ID    = " + str(BATCH_ID));
	print (itab2 + "tableNum    = " + str(tableNum));
	print (itab2 + "totalTables = " + str(totalTables));
	print (itab2 + "schema      = " + schema);
	print (itab2 + "table       = " + table);

	try:	# --{ 
		print ("\n"+ itab2 + "Step 1: Register DataFrame pointing to Hive Table " + tbl_nm + "\t\t" + getLocalTimeOfCity() );
		#This view is used as the FROM clause of the SELECT statement below
		dfStartTime = TIME2();	#time()	
		tbl.createOrReplaceTempView (registeredDFname);
		totalCols = tbl.columns.count;

		print (itab2 + "Step 2: Remove all Unneeded DW Columns (preserves original Column order)");
		allHIVETableColumnsNoDWC=list(map(lambda x:  x.upper(),tbl.columns));	#Uppercase all elements
		for x in DW_COLS_TO_SKIP_LIST:
			if x.upper() in allHIVETableColumnsNoDWC:
				allHIVETableColumnsNoDWC.remove(x)
		totalColumnsNoDWC=len(allHIVETableColumnsNoDWC);

		if debugVerbose:
			print (itab2 + "Type of totalColumnsNoDWC        = " + str(type(totalColumnsNoDWC)       ));	# Type: 'int'
			print (itab2 + "Type of allHIVETableColumnsNoDWC = " + str(type(allHIVETableColumnsNoDWC)));	# Type: 'list'

		print ("\n" + itab2 + "Final Columns remain  = " + str(totalColumnsNoDWC)  + "   <================== " )
		print (       itab2 + "List of final columns = " + str(allHIVETableColumnsNoDWC))
	except:
		print ("\n\n\n********** ERROR: at GetHIVEColumns(): Exception: " + str(sys.exc_info()[0]) + "\n\n")
		raise;

	#--} try:  
	print (itab1 + "<== Function GetHIVEColumns;\t\t" + getLocalTimeOfCity() )
	return allHIVETableColumnsNoDWC
#--}
#---------------------------------------------------------------------------------------------------------------
#--{
def GetHIVEtableRowsCount (	tableNum, totalTables, schema, table, tbl_nm, tableID, BATCH_ID,		\
							tbl ,  registeredDFname, TIME_SUMMARY_LIST,  tableStartTimeT1_ForMS,	\
							tableStartTimeT1,	tableStartTimeT2,  LIST_OF_TABLE_STATUS_COUNTS,		\
							debugVerbose=False,   topNrows=0,    leadingTabs=""):
	itab1=leadingTabs;
	itab2=itab1 + "\t";
	itab3=itab2 + "\t";
	print (itab1 + "==> Function GetHIVEtableRowsCount;\t\t" + getLocalTimeOfCity() )
	print (itab2 + "TABLE_ID         = " + tableID);
	print (itab2 + "BATCH_ID         = " + BATCH_ID + ";  tableNum = " + str(tableNum) + ";  totalTables = " + str(totalTables));
	print (itab2 + "schema           = \"" + schema           + "\"");
	print (itab2 + "table            = \"" + table            + "\"");
	print (itab2 + "tbl_nm           = \"" + tbl_nm           + "\"");
	print (itab2 + "registeredDFname = \"" + registeredDFname + "\"");

	try:	# --{ 
		# Get table rowscount immediately ONE TIME (previously it recalculates rowscounts over and over for EVERY table column
		temp_sql="select count(*)  as RowsCount from " + registeredDFname;
		temp_rowsCountDF= spark.sql(temp_sql);	# Execute SQL statement. Type: DataFrame.  
		totalTABLERows=temp_rowsCountDF.take(1)[0][0];
		print (itab2 +"==> RowsCount = "  + str(totalTABLERows)  );
	except:
		print ("\n\n\n********** ERROR: at GetHIVEtableRowsCount(): EXCEPTION: " + \
				str(sys.exc_info()[0])  + "\n\n\n" );
		raise;
	#--} try:  

	print (itab1 + "<== Function GetHIVEtableRowsCount;\t\t" + getLocalTimeOfCity() )
	return totalTABLERows;
#--}
#---------------------------------------------------------------------------------------------------------------
#--{
def DisplayLIST_OF_TABLES_COUNT (LIST_OF_TABLE_STATUS_COUNTS, totalInputTables, leadingTabs=""):
	itab1=leadingTabs;
	itab2=itab1 + "\t";
	itab3=itab2 + "\t";
	itab4=itab3 + "\t";
	itab5=itab4 + "\t";

	print (itab1 + "==> Function DisplayLIST_OF_TABLES_COUNT;\t\t" + getLocalTimeOfCity() )
	#LIST_OF_TABLE_STATUS_COUNTS=[0,0,0,0,0];	# 1st element = totalTables_SUCCESS
	#						# 2nd element = totalTables_FAILED
	#						# 3rd element = totalTables_EMPTY
	#						# 4th element = totalTables_TABLENOTFOUND_IN_HIVE  (IN_HIVE)
	#						# 5th element = totalTables_TABLENOTFOUND_IN_RM  (IN RoadMaster)

	print (itab3 + "Total Input Tables  = " + str(totalInputTables));
	print (itab3 + "Total SUCCESS		= " + str(LIST_OF_TABLE_STATUS_COUNTS[0]) + "\t\t(Tables EMPTY = " + str(LIST_OF_TABLE_STATUS_COUNTS[0]) +")");
	print (itab3 + "Total FAILED		= " + str(LIST_OF_TABLE_STATUS_COUNTS[1]) + "\n");
	print (itab4 + "FAILURE Reason:");
	print (itab5 + "Tables NOTFOUND IN_HIVE		= " + str(LIST_OF_TABLE_STATUS_COUNTS[3]));
	print (itab5 + "Tables NOTFOUND IN_RoadMaster	= " + str(LIST_OF_TABLE_STATUS_COUNTS[4]));
	print (itab1 + "<== Function DisplayLIST_OF_TABLES_COUNT;\t\t" + getLocalTimeOfCity() )
#--}
#---------------------------------------------------------------------------------------------------------------
#--{
def  GetOutBoundAccessToken (URL_AUTHENTICATE , URL_MESSAGING_SECRET_KEY, debugVerbose=False, leadingTabs=""):
	itab1=leadingTabs;
	itab2=itab1 + "\t";

	print (itab1 + "==> Function GetOutBoundAccessToken;\t\t" + getLocalTimeOfCity() )
	auth = {'SECRET_ID': URL_MESSAGING_SECRET_KEY}
	print (itab2 + "1) Authenticating ...\t\t" + "auth="+str(auth) );
	try:
		response = requests.post(URL_AUTHENTICATE ,  json=auth)

		if response.status_code==200:
			print (itab3 + "Successfull ... response.status_code=" + str(response.status_code)  +
				"\t\t" + getLocalTimeOfCity() );
			print (itab2 + "Type of response       = "+ str(type(response)) )	# <class 'requests.models.Response'>
			print (itab3 + "response.status_code   = " + str(response.status_code))	# 200
			print (itab3 + "response.content       = " + str(response.content))	# It's a dictionary, but with b', not u':
						# b'{\n  "access_token": "eyJ0eXAiOiJKV1QiL...."\n}\n'

			print (itab2 + "2) Requesting access_token:");
			access_token = json.loads(response.content.decode('utf-8'))['access_token']
			print (itab3 + "Type  of access_token = " + str( type(access_token) ) );	# <class 'str'>
			print (itab3 + "Value of access_token = " + str( access_token ) ); # "eyJ0eXAiOiJKV1QiLCJhbGciOi ...."
			print ("\n" + itab3 + "==> MicroService Return Status: SUCCESSFUL;   status_code=" +	\
					str(response.status_code) +"\t\t(at GetOutBoundAccessToken () )\n")
		else:
			print ("\n\n\n"+itab1 + "********* ERROR (1): Cannot get authenticated   due to   response.status_code=" + \
					str(response.status_code) + "\t\t(at GetOutBoundAccessToken () )\n");
			print (itab3 + "==> MicroService Return Status (1): FAILED;\t\tstatus_code=" + str(response.status_code) + \
					"\t\t(at GetOutBoundAccessToken () )\n")
			raise EXCEPTION_CANNOT_GET_AUTHENTICATION_KEY;
	except:
		print ("\n\n\n"+itab1 + "********* ERROR (2): Cannot get authenticated: response.status_code=" +	\
				str(response.status_code) + "\t\t(at GetOutBoundAccessToken () )\n");
		print (itab3 + "==> MicroService Return Status (2): FAILED\t\t(GetOutBoundAccessToken () )\n")
		print (itab1 + "<== Function GetOutBoundAccessToken;\t\t" + getLocalTimeOfCity() )
		raise EXCEPTION_CANNOT_GET_AUTHENTICATION_KEY;
	print (itab1 + "<== Function GetOutBoundAccessToken;\t\t" + getLocalTimeOfCity() +"\n")
	return access_token
#---------------------------------------------------------------------------------------------------------------
def ConvertTaskStatCodeToString (TaskStatCode):
	str="?????";
	if TaskStatCode==RoadMaster_AVAIL_TaskCD:   str="AVAILABLE";
	if TaskStatCode==RoadMaster_RUNNING_TaskCD: str="RUNNING";
	if TaskStatCode==RoadMaster_SUCCESS_TaskCD: str="SUCCESS";
	if TaskStatCode==RoadMaster_FAIL_TaskCD:    str="FAIL";
	return str;
#---------------------------------------------------------------------------------------------------------------
#--{
def SendToMessageToOutBound (	tableNum, totalTables, schema, table, tbl_nm, tableID, BATCH_ID,	\
								taskCD, errorMsg, OutBoundAccessToken, URL_OUTBOUND,				\
								debugVerbose=False,   leadingTabs=""):
	itab1=leadingTabs;
	itab2=itab1 + "\t";
	itab3=itab2 + "\t";

	print (itab1 + "==> Function SendToMessageToOutBound;\t\t" + getLocalTimeOfCity() )
	print (itab2 + "TABLE_ID     = " + tableID);
	print (itab2 + "BATCH_ID     = " + str(BATCH_ID));
	print (itab2 + "schema       = " + schema);
	print (itab2 + "table        = " + table);
	print (itab2 + "taskCD       = " + str(taskCD) + "\t==> " + ConvertTaskStatCodeToString (taskCD));
	print (itab2 + "errorMsg     = \"" + errorMsg      + "\""  );
	print (itab2 + "URL_OUTBOUND = \"" + URL_OUTBOUND  + "\"\n");

	header	=	{	"Content-Type": "application/json",	\
					"Cache-Control": "no-cache",		\
					"Authorization": "Bearer {token}".format(token=OutBoundAccessToken)	\
			}
	message	=	{"hive_schema":schema, "tableId":tableID,   "task_stat_cd":taskCD,  "error":errorMsg }

	if debugVerbose:
		print (itab2 + "Type  of access_token = "	+ str( type(OutBoundAccessToken) ) );
		print (itab2 + "Value of access_token = "	+ str(      OutBoundAccessToken) );
		print (itab2 + "header  = "			+ str( header  ) );
		print (itab2 + "message = "			+ str( message ) );
		print (itab2 + "Type of message = "		+ str( type(message) ) );	# <class 'dict'>

	print ("\n" + itab2 + "Send OutBound message to " +  URL_OUTBOUND + "\t\t" + getLocalTimeOfCity() );
	try:
		### response = requests.post( URL_OUTBOUND, headers=header, json=json.dumps(message))
			#Need to apply json.dumps (message).  Otherwise it gives this error:
			#	<p class="errormsg">TypeError: the JSON object must be str, bytes or bytearray, not dict

		response = requests.post( URL_OUTBOUND, headers=header, json=message)

		if debugVerbose:
			print (itab3 + "URL_OUTBOUND   = " + URL_OUTBOUND);
			print (itab3 + "Type  of response               = " + str(type(response)) );			#<class 'requests.models.Response'>
			print (itab3 + "Type  of response.status_code   = " + str(type(response.status_code)) );	#<class 'int'>
			print (itab3 + "Value of response.status_code   = " + str(response.status_code) );

		if response.status_code==200:
			#print (response.status_code, response.reason)
			print ("\n" + itab3 + "==> MicroService Return Status: SUCCESSFUL  (at SendToMessageToOutBound())\t\tstatus_code=" +str(response.status_code) +"\n")
		else:
			print (itab3 + "response.status_code     = " + str(response.status_code) );
			print (itab3 + "response.status_reason   = " + response.reason)
			print (itab3 + "response.content         = " + str(response.content))

			print (itab3 + "Type of response.content = " + str(type(response.content)))
			print ("\n\n********** ERROR: at SendToMessageToOutBound(): MicroService Return Status: FAIL\t\tstatus_code=" + str(response.status_code)+ "\n\n");
			print (itab1 + "<== Function SendToMessageToOutBound;\t\t" + getLocalTimeOfCity() )
			raise EXCEPTION_OUTBOUND_MSG_FAIL;

	except Exception as EXCEPTION_OUTBOUND_MSG_FAIL:
		print ("\n********** ERROR: at SendToMessageToOutBound(): Exception: EXCEPTION_OUTBOUND_MSG_FAIL\n\n\n" );
		print (itab1 + "<== Function SendToMessageToOutBound;\t\t" + getLocalTimeOfCity() )

	except:
		print ("\n********** ERROR: at SendToMessageToOutBound(): Exception: " + str(sys.exc_info()[0])  + "\n\n\n" );
		print (itab1 + "<== Function SendToMessageToOutBound;\t\t" + getLocalTimeOfCity() )
		raise EXCEPTION_OUTBOUND_MSG_FAIL;

	print (itab1 + "<== Function SendToMessageToOutBound;\t\t" + getLocalTimeOfCity() )
#--}
#---------------------------------------------------------------------------------------------------------------


#---------------------------------------------------------------------------------------------------------------
#---------------------------------------- MAIN MAIN MAIN MAIN MAIN MAIN ----------------------------------------
#---------------------------------------------------------------------------------------------------------------
TIMEZONE= "US/Central"
CITY	= "Austin"
SESSION_ID = TIME1.strftime("%Y/%m/%d %H:%M:%S");

jobStart     = TIME1.strftime("%Y%m%d %H:%M:%S")
jobStartTime = TIME2();	#time()

print ("\n\n===== Python Job starts at: " + SESSION_ID + "\t\t" + getLocalTimeOfCity() + "\n")
spark = SparkSession.builder.appName("Chevelle_Statistics").enableHiveSupport().getOrCreate()
		# Note: The parameter at Spark-Submit, if specified, will override this appName 
spark.sparkContext.setLogLevel("ERROR")

itab1="\t";
itab2=itab1+"\t";
itab3=itab2+"\t";
itab4=itab3+"\t";
itab5=itab4+"\t";
itab6=itab5+"\t";

file_name						= sys.argv[1];
URL_GetTableByID				= sys.argv[2];	# MS=MicroService 1
URL_GetWorkUnit					= sys.argv[3];	# 
URL_Write1TableToOutput			= sys.argv[4];	# 
URL_UpdateQueue					= sys.argv[5];	#
URL_AUTHENTICATE				= sys.argv[6];
URL_OUTBOUND					= sys.argv[7];
URL_MESSAGING_SECRET_KEY		= sys.argv[8];
SESSION_DATE_STR				= sys.argv[9];	#	$(date +"%Y-%m-%d %H:%M:%S")"  => 2018-09-29 09:01:02
orig_file_name					= sys.argv[10];
hdfs_input_dir					= sys.argv[11];
hdfs_output_dir					= sys.argv[12];
hdfs_checkpoint_dir				= sys.argv[13];
hdfs_checkpoint_SUCCESS_dir			= sys.argv[14];
hdfs_checkpoint_ATTEMPTED_dir		= sys.argv[15];
hdfs_checkpoint_ROWSDATA_dir		= sys.argv[16];
hdfs_HIVE_Column_Types_dir			= sys.argv[17];
numExecutors						= sys.argv[18];
max_retry_threshold					= sys.argv[19];
DFColumnsBatchSizeStr				= sys.argv[20];
Must_Have_HIVE_DataTypes			= sys.argv[21];
Include_SKIP_DATA_In_Output			= sys.argv[22];
str_BIG_TABLE_ROWS_TO_SWITCH		= sys.argv[23];
str_BIG_TABLE_TABLECOLS_BATCH_SIZE	= sys.argv[24];
str_BIG_TABLE_STATCOLS_BATCH_SIZE	= sys.argv[25];
BATCH_ID							= sys.argv[26];
SRVR_NM								= sys.argv[27];
G_MICROSERVICE_AUTHORIZATION		= sys.argv[28];
debugStr							= sys.argv[29];

timestr  = TIME1.strftime("%Y%m%d%H%M%S");	# This format matches Old JSON output format 
DFColumnsBatchSize  = int(DFColumnsBatchSizeStr)
debugVerbose		= False;
if (debugStr=="YES"): debugVerbose	= True;

Check_MUST_HAVE_HIVE_DATATYPES	= True;
if (Must_Have_HIVE_DataTypes=="YES"):
	Check_MUST_HAVE_HIVE_DATATYPES	= True;
else:
	Check_MUST_HAVE_HIVE_DATATYPES	= False;

ip_dir				= hdfs_input_dir + "/" + file_name;	# ip_dir = input_dir (HDFS)
op_dir				= hdfs_output_dir;			# op_dir = output_dir (HDFS)
hdfs_file_path			= hdfs_input_dir + "/" + file_name	#display for OP people to use
MAX_RETRY_THRESHOLD		= int(max_retry_threshold)
BIG_TABLE_ROWS_TO_SWITCH	= int(str_BIG_TABLE_ROWS_TO_SWITCH)
BIG_TABLE_TABLECOLS_BATCH_SIZE	= int(str_BIG_TABLE_TABLECOLS_BATCH_SIZE)
BIG_TABLE_STATCOLS_BATCH_SIZE	= int(str_BIG_TABLE_STATCOLS_BATCH_SIZE)


#####################################################################################################
# TEMPORARY: this HDFS directory to save output as \x1c Delimiter, not JSON, 
#            Need to disable later
op_dir2 = op_dir + "2";
#####################################################################################################

print ("\n\n");
print ("Parameters:\t\t\t" + getLocalTimeOfCity() + "\n")
print ("\tfile_name					= " + file_name)
print ("\tURL_GetTableByID			= " + URL_GetTableByID)		# MS=MicroService 1
print ("\tURL_GetWorkUnit			= " + URL_GetWorkUnit)		# MS=MicroService 2
print ("\tURL_Write1TableToOutput	= " + URL_Write1TableToOutput)	# MS=MicroService 3
print ("\tURL_UpdateQueue			= " + URL_UpdateQueue)		# MS=MicroService 4
print ("\tURL_AUTHENTICATE			= " + URL_AUTHENTICATE)		# MS=MicroService 5
print ("\tURL_OUTBOUND				= " + URL_OUTBOUND)		# MS=MicroService 6
print ("\tURL_MESSAGING_SECRET_KEY	= " + URL_MESSAGING_SECRET_KEY)	
print ("");
print ("\tSESSION_DATE_STR			= " + SESSION_DATE_STR)
print ("\ttimestr					= " + timestr)
print ("\torig_file_name			= " + orig_file_name)
print ("\thdfs_input_dir			= " + hdfs_input_dir)
print ("\thdfs_output_dir			= " + hdfs_output_dir)
print ("\thdfs_checkpoint_dir		= " + hdfs_checkpoint_dir)
print ("\thdfs_checkpoint_SUCCESS_dir	= " + hdfs_checkpoint_SUCCESS_dir)
print ("\thdfs_checkpoint_ATTEMPTED_dir	= " + hdfs_checkpoint_ATTEMPTED_dir)
print ("\thdfs_checkpoint_ROWSDATA_dir	= " + hdfs_checkpoint_ROWSDATA_dir)
print ("\thdfs_HIVE_Column_Types_dir	= " + hdfs_HIVE_Column_Types_dir)
print ("\tnumExecutors				= " + numExecutors)
print ("\tMAX_RETRY_THRESHOLD		= " + max_retry_threshold )
print ("\tDFColumnsBatchSizeStr		= " + DFColumnsBatchSizeStr )
print ("\tMust_Have_HIVE_DataTypes	= " + Must_Have_HIVE_DataTypes )
print ("\tInclude_SKIP_DATA_In_Output	= " + Include_SKIP_DATA_In_Output )
print ("");
print ("\tBIG_TABLE_ROWS_TO_SWITCH			= " + str(BIG_TABLE_ROWS_TO_SWITCH) )
print ("\tBIG_TABLE_TABLECOLS_BATCH_SIZE	= " + str(BIG_TABLE_TABLECOLS_BATCH_SIZE) )
print ("\tBIG_TABLE_STATCOLS_BATCH_SIZE		= " + str(BIG_TABLE_STATCOLS_BATCH_SIZE) )
print ("\tBATCH_ID			= " + str(BATCH_ID) )
print ("\tSRVR_NM			= " + SRVR_NM )
print ("");
print ("\tdebugStr			= " + debugStr)
print ("");
print ("Internal Variables:")
print ("\tip_dir			= " + ip_dir)
print ("\top_dir			= " + op_dir)
print ("\top_dir2			= " + op_dir2)
print ("\thdfs_file_path	= " + hdfs_file_path)
print ("\tdebugVerbose		= " + format(debugVerbose) );	#Format convert to string: True/False
print ("\tEncoding			= " + sys.stdout.encoding)
print ("\tMS Authorization	= \"" + G_MICROSERVICE_AUTHORIZATION + "\"")
print ("\n\n");

if debugVerbose:
	n=0;
	print ("===== List of SparkContext Config items:\t\t\t" + getLocalTimeOfCity() + "\n")
	for v in spark.sparkContext.getConf().getAll():
		n +=1;
		print("\t"+ str(n) + ")\t" + str(v))
	print ("\n\n");

dfSchemaForCheckpoint        = CreateSchemaForCheckpoint ();
dfSchemaForOutputTable       = CreateSchemaForOutputTable ();
dfSchemaForHiveColumnTypes   = CreateSchemaForHiveColumnTypes ();
dfSchemaForDWColumns         = CreateSchemaForDWColumns();
dfSchemaForORACLEOutputTable = CreateSchemaForORACLEOutputTable ();
dfSchemaForTableTimeSummary  = CreateSchemaForTableTimeSummary ();

topNrows=10;	#Display this many rows from DataFrame (to limit output)
print ("\n");

DW_COLS_TO_SKIP_LIST = [	"SRC_SYS_CRT_TS",    "SRC_SYS_ID",         "SRC_SYS_IUD_CD",			\
				"SRC_SYS_UPD_BY",    "SRC_SYS_UPD_GMT_TS", "SRC_SYS_UPD_TS",			\
				"DW_ANOMALY_FLG",    "DW_EXTRACT_TS",      "DW_INS_GMT_TS",			\
				"DW_JOB_ID",         "DW_MOD_TS",          "DW_SUM_SK",				\
				"GG_SRC_SYS_IUD_CD", "GG_SRC_SYS_UPD_TS",  "GG_TXN_CSN",  "GG_TXN_NUMERATOR" ]
TIME_SUMMARY_LIST  = []

file=1
if (file):	# --{

	STEP="STEP 0";
	print ("\n\nSTEP 0: Calling Microservice to get OutBound Access Token:")
	try:
		OutBoundAccessToken = GetOutBoundAccessToken (URL_AUTHENTICATE, URL_MESSAGING_SECRET_KEY, debugVerbose,itab1);
	except:
		print ("\n\n\n***** WARNING: OutBound Call failed at STEP 0:");
		print (itab2 + "Exception: " + str(sys.exc_info()[0])  );
		print (itab2 + "This is OK as it should not stop Profiling  ==> Continue to next profiling step\n\n");

	registeredDFname="TEMP_DF_TABLE";

	LIST_OF_TABLE_STATUS_COUNTS=[0,0,0,0,0];	# 1st element = totalTables_SUCCESS
							# 2nd element = totalTables_FAILED
							# 3rd element = totalTables_EMPTY
							# 4th element = totalTables_TABLENOTFOUND_IN_HIVE  (IN_HIVE)
							# 5th element = totalTables_TABLENOTFOUND_IN_RM  (IN RoadMaster)
							# See function DisplayLIST_OF_TABLES_COUNT()

	RoadMaster_AVAIL_TaskCD=0;		# AVAIL:= 0; RUNNING:= 1; SUCCESS:= 2; FAILED:= 3;
	RoadMaster_RUNNING_TaskCD=1;	# AVAIL:= 0; RUNNING:= 1; SUCCESS:= 2; FAILED:= 3;
	RoadMaster_SUCCESS_TaskCD=2;	# AVAIL:= 0; RUNNING:= 1; SUCCESS:= 2; FAILED:= 3;
	RoadMaster_FAIL_TaskCD=3;		# AVAIL:= 0; RUNNING:= 1; SUCCESS:= 2; FAILED:= 3;

	# Read input file into DataFrame; Each line has 2 columns, separated by tab; split into 2 DataFrame columns:
	inputTables_rdd                = spark.sparkContext.textFile(ip_dir).map(lambda x: x.split("\t"))
	inputTablesDF                  = inputTables_rdd.toDF(['schema','table', 'tableID', 'fileSize']);
															#Must also specify these columns in function
															#CreateInitialDataFrames, which uses this DataFrame definition

	totalTables_BEFORE_SUBTRACT    = inputTablesDF.count();
	totalTablesStr_BEFORE_SUBTRACT = "table";
	if (totalTables_BEFORE_SUBTRACT > 1): totalTablesStr_BEFORE_SUBTRACT="tables"
	inputTablesDF_Name="INPUT_TABLES_DF"
	inputTablesDF.createOrReplaceTempView (inputTablesDF_Name);
	if debugVerbose:
		print ("This input SplitFile \"" + file_name + "\" has: " + str(totalTables_BEFORE_SUBTRACT) +	\
			" " + totalTablesStr_BEFORE_SUBTRACT +"  (BEFORE Subtracting Checkpointed Tables)\n")
		DisplayTopNrows (inputTablesDF, topNrows, itab1, inputTablesDF_Name);

	TABLES_TO_RERUN_DF = CreateInitialDataFrames (	inputTablesDF_Name, debugVerbose, itab1);

	print ("\n========== MAIN(): This Dataframe contains FINAL LIST of tables to run (AFTER SUBTRACKING Checkpoint tables):")
	DisplayTopNrows (TABLES_TO_RERUN_DF, -1, itab1, "TABLES_TO_RERUN_DF")

	total_Table_Columns = 0;
	total_Table_Rows    = 0;
	total_JOB_Columns   = 0;	# All columns of all tables processed
	total_JOB_Rows      = 0;	# All rows   of all tables processed
	tableNum            = 0;
	totalTables         = TABLES_TO_RERUN_DF.count();

	for row in TABLES_TO_RERUN_DF.orderBy("schema","table", ascending=True).collect():	# --{
			#The schema for this row object is line above: "inputTablesDF = inputTables_rdd.toDF(['schema','table', 'tableID']);	"
			# it has 3 fields

		tableStartTimeT1	= TIME1.strftime("%Y%m%d %H:%M:%S")
		tableStartTimeT1_ForMS	= TIME1.strftime("%Y-%m-%d %H:%M:%S")
		tableStartTimeT2	= TIME2();	#time()		Type: Float
		tableNum      += 1;
		schema		= row.schema.upper();
		table		= row.table.upper();
		tableID		= row.tableID.upper();
		fileSize	= int(row.fileSize);		#HDFS filesize, not rowscounts
		tbl_nm		= row.schema+'.'+row.table;

		OutBoundMS1_Elapsed			= "";
		HiveGetColumns_Elapsed		= "";
		HiveRowsCount_Elapsed		= "";
		RoadMasterMS_Elapsed		= "";
		ColsFilter_Elapsed			= "";
		Profile_Elapsed				= "";
		InsertOutputMS_Elapsed		= "";
		UpdateQueueMS_Elapsed		= "";
		OutBoundMS2_Elapsed			= "";


		tableRowsCountStr="";		#How long it took RoadMaster MicroService to complete
		RM_TotalColumnsStr="";		#How many columns returned by RoadMaster for current table
		HIVE_TotalColumnsStr="";	#How long columns returned by Hive Desc command

		TABLE_STATUS="RUNNING"

		print ("\n"+"="*150);
		print (	"===== BEGIN TABLE: #"  + str(tableNum) + "/" + str(totalTables)    + ":	'"+ tbl_nm + "'" )
		print ( "===== TABLE_ID="+tableID  + ";  FileSize=" + str(fileSize) +	\
					"; ~~~~~ StartTime:  "+tableStartTimeT1  + ";  tableStartTimeT2=" + str(tableStartTimeT2)  )
		print ( "===== BATCH_ID="+BATCH_ID +"; ~~~~~ Split File: "+file_name   + ";  " + getLocalTimeOfCity() )
		print ("="*150);

		try:	#--{
			STEP="STEP 1";
			print (	"\n"+ itab1 + "STEP 1: Send OutBound Message with taskcode=1 (Running):\t\t" + getLocalTimeOfCity()  );
			print (       itab2 + "(TableNum=" + str(tableNum) + " of " + str(totalTables)	+	\
						"; BATCH_ID="+BATCH_ID+"; TABLE_ID="+tableID+")\n" );
			ERRMSG_IF_FAIL        ="Cannot Send OutBound Message1"
			OutBoundMS1_Elapsed   = "FAILED"	#In case this next block fails, the output string would just be  "failed"
			OutBoundMS1_StartTime = TIME2();	#time()		Type: Float
			SendToMessageToOutBound (	tableNum, totalTables, schema, table, tbl_nm, tableID, BATCH_ID,	\
										RoadMaster_RUNNING_TaskCD, "", OutBoundAccessToken, URL_OUTBOUND,	\
										debugVerbose,   itab2);
			OutBoundMS1_Elapsed = str(int(math.ceil(TIME2() - OutBoundMS1_StartTime)))+ " secs" ;
			if debugVerbose: print (itab2+ "OutBoundMS1_Elapsed     = " + OutBoundMS1_Elapsed ) 
		except:
			print ("\n" + itab2 + "***** WARNING: OutBound Call failed at STEP 1:");
			#print (itab1 + "Exception Type: " + str(type(sys.exc_info()[0])) );	# <class 'type'>
			print (itab1 + "Exception:      " + str(sys.exc_info()[0])       )
			print (itab1 + "But it should not stop Profiling ==> Continue to next step\n\n");
		#--}

		try:	#--{
			STEP="STEP 2";

			#DEBUG:
			#tbl_nm=tbl_nm + "_XXXXXXXXX";


			print ("\n" + itab1 + "STEP 2. Check If Table Exists in HIVE:\t\t"  + getLocalTimeOfCity());
			ERRMSG_IF_FAIL           = "Hive Table NOT Found"
			HiveCheckTable_Elapsed   = "TableNotFound"
			HiveCheckTable_StartTime = TIME2();	#time()		Type: Float
			tbl = CheckIfTableExistsInHIVE(	tableNum, totalTables, schema, table, tbl_nm, tableID, BATCH_ID,	\
											TIME_SUMMARY_LIST,  tableStartTimeT1_ForMS, tableStartTimeT1,		\
											tableStartTimeT2, LIST_OF_TABLE_STATUS_COUNTS,						\
											debugVerbose, topNrows, itab2)
			HiveCheckTable_Elapsed = str(int(math.ceil(TIME2() - HiveCheckTable_StartTime) ) )+ " secs" ;


			STEP="STEP 3";
			print ("\n" + itab1 + "STEP 3. Get HIVE Table Columns:\t\t"  + getLocalTimeOfCity());
			ERRMSG_IF_FAIL           = "Cannot Retrieve Hive Table Columns"
			HiveGetColumns_Elapsed   = "FAILED"
			HiveGetColumns_StartTime = TIME2();	#time()		Type: Float
			allHIVETableColumnsNoDW_LIST = GetHIVEColumns (	tableNum, totalTables, schema, table, tbl_nm, tableID,	\
															BATCH_ID, tbl , registeredDFname,  TIME_SUMMARY_LIST,	\
															tableStartTimeT1_ForMS,	tableStartTimeT1, tableStartTimeT2,		\
															LIST_OF_TABLE_STATUS_COUNTS, debugVerbose,   topNrows,    itab2);
			totalHIVEColumnsNoDWC  = len(allHIVETableColumnsNoDW_LIST);
			HIVE_TotalColumnsStr   = str(totalHIVEColumnsNoDWC);
			HiveGetColumns_Elapsed = str(int(math.ceil(TIME2() - HiveGetColumns_StartTime) ) )+ " secs" ;
			if debugVerbose:
				print ("\n"+itab2 + "Type of allHIVETableColumnsNoDW_LIST=" + str(type(allHIVETableColumnsNoDW_LIST)));#<class 'int'>
				print (     itab2 + "HIVE_TotalColumnsStr   = " + HIVE_TotalColumnsStr );
				print (     itab2 + "HiveGetColumns_Elapsed = " + HiveGetColumns_Elapsed+ "\n");



			STEP="STEP 4";
			print ("\n"+itab1 + "STEP 4. Get HIVE Table Rows:\t\t"  + getLocalTimeOfCity());
			print (		itab2 + "FileSize passed in for this table is: \"" + str(fileSize) + "\"");
			print (		itab2 + "Type of fileSize is: \"" + str(type(fileSize)) + "\"");
			ERRMSG_IF_FAIL  = "Cannot Calculate Hive Table RowsCount"
			HiveRowsCount_Elapsed   = "FAILED"
			HiveRowsCount_StartTime = TIME2();	#time()		Type: Float
			if fileSize == 0:
				totalHIVETableRowsCount = 0;
				tableRowsCountStr       = str(totalHIVETableRowsCount);
				HiveRowsCount_Elapsed	= str(int(math.ceil(TIME2() - HiveRowsCount_StartTime) ) )+ " secs" ;
				print (itab2 + "Skip calculating Hive TableRowsCount.  Set totalHIVETableRowsCount= " + str(totalHIVETableRowsCount));
			else:
				totalHIVETableRowsCount = GetHIVEtableRowsCount (	tableNum, totalTables, schema, table,tbl_nm,					\
																	tableID,BATCH_ID, tbl , registeredDFname,  TIME_SUMMARY_LIST,	\
																	tableStartTimeT1_ForMS,	tableStartTimeT1, tableStartTimeT2,		\
																	LIST_OF_TABLE_STATUS_COUNTS, debugVerbose, topNrows, itab2);
				tableRowsCountStr       = str(totalHIVETableRowsCount);
				HiveRowsCount_Elapsed   = str(int(math.ceil(TIME2() - HiveRowsCount_StartTime) ) )+ " secs" ;
				if debugVerbose:
					print ("\n"+itab2+ "Type of totalHIVETableRowsCount=" + str(type(totalHIVETableRowsCount)));	#<class 'int'>
					print (     itab2+ "TableRowsCountStr     = " + tableRowsCountStr );
					print (     itab2+ "HiveRowsCount_Elapsed = " + HiveRowsCount_Elapsed+ "\n");



			#NOTE: Even if Hive table has 0 row, still need to call RoadMaster to get ColId for each column to display in Oracl3 output table


			STEP="STEP 5";
			print ("\n" + itab1 + "STEP 5. Get Columns from RoadMaster MicroService:\t\t"  + getLocalTimeOfCity());
				# Need to get these RoadMaster columns even when Hive table is empty (so to provide COL_ID to write out
				# to Output Oracle table)
			ERRMSG_IF_FAIL         = "RoadMaster MicroService Failed"
			RoadMasterMS_Elapsed   = "FAILED";
			RoadMasterMS_StartTime = TIME2();	#time()		Type: Float

			ONE_ROADMASTER_TABLE_DF = GetColumnsFor1TableFromRoadMaster (	tableNum, totalTables, schema, table,	\
											tbl_nm, tableID, BATCH_ID, URL_GetTableByID, URL_UpdateQueue,			\
											TIME_SUMMARY_LIST, tableStartTimeT1_ForMS,  tableStartTimeT1,			\
											tableStartTimeT2, LIST_OF_TABLE_STATUS_COUNTS,							\
											debugVerbose, topNrows, itab2);
			if debugVerbose: print (itab2 + "Type of ONE_ROADMASTER_TABLE_DF=" + str(type(ONE_ROADMASTER_TABLE_DF))+ "\n");
			RM_TotalColumns    = ONE_ROADMASTER_TABLE_DF.count()
			RM_TotalColumnsStr = str(RM_TotalColumns);
			RoadMasterMS_Elapsed = str(int(math.ceil(TIME2() - RoadMasterMS_StartTime) ) )+ " secs" ;	#Seconds
			if debugVerbose:
				print (itab2 + "RM_TotalColumnsStr   = " + RM_TotalColumnsStr);
				print (itab2 + "RoadMasterMS_Elapsed = " + RoadMasterMS_Elapsed + "\n") ;


			STEP="STEP 6";
			print (itab1+"STEP 6. Filter Hive Table against RoadMaster DataFrame to get list of columns:\t\t"+ getLocalTimeOfCity());
			ERRMSG_IF_FAIL="Filter Hive Table against RoadMaster DataFrame Failed"
			ColsFilter_Elapsed   = "FAILED"
			ColsFilter_StartTime = TIME2();	#time()		Type: Float
			cond1 =  (col("Hive_Schema_Nm" ) == schema.lower() ) | (col("Hive_Schema_Nm" ) == schema.upper() );
			cond2 =  (col("Table_Nm")        == table.lower()  ) | (col("Table_Nm" )       == table.upper() );
				# No need to compare to Column yet.  Will do that in BuildSQL_1_Batch
			ONE_ROADMASTER_TABLE_LIST  = ONE_ROADMASTER_TABLE_DF.filter(cond1 & cond2).collect();
				# See function FilterHiveColumnsAgainstRoadMasterColumns 
				# Will incoporate that function once i incorporate the RoadMaster GetBATCH

			if ONE_ROADMASTER_TABLE_DF.count() == 0:
				#Here cannot find Hive columms in DataFrame for this table
				print ("\n"+itab2 + "********** ERROR: RoadMaster does NOT have this table: \"" + schema +"."+ table + "\" *****\n");
				if Check_MUST_HAVE_HIVE_DATATYPES:
					print (itab2 + "==> RAISE: EXCEPTION_HIVE_TABLE_NOTFOUND_IN_ROADMASTER");
					RM_TotalColumnsStr   = str(len(ONE_ROADMASTER_TABLE_LIST))
					raise EXCEPTION_HIVE_TABLE_NOTFOUND_IN_ROADMASTER("***** RoadMaster does not have this table: \"" + schema +"."+ table + "\" *****");

				else: print (itab2+ "Option \"Check_MUST_HAVE_HIVE_DATATYPES\" was specified ==> Ignore this error to let it go thru");

			RM_TotalColumnsStr = str(len(ONE_ROADMASTER_TABLE_LIST))
			ColsFilter_Elapsed = str(int(math.ceil(TIME2() - ColsFilter_StartTime) ) )+ " secs" ;
			if debugVerbose:
				print (itab2 + "ONE_ROADMASTER_TABLE_LIST        = " + str(ONE_ROADMASTER_TABLE_LIST));
				print (itab2 + "ONE_ROADMASTER_TABLE_LIST Length = " + str(len(ONE_ROADMASTER_TABLE_LIST))  +"\t\t(This is number of columns)" );
				print (itab2 + "RM_TotalColumnsStr               = " + RM_TotalColumnsStr)
				print (itab2 + "ColsFilter_Elapsed               = " + ColsFilter_Elapsed+ "\n") ;




			Profile_Elapsed   = "FAILED"	#In case this next block fails, the output string would just be  "failed"
			Profile_StartTime = TIME2();	#time()		Type: Float

			if totalHIVETableRowsCount == 0:	#--{
				# TABLE is EMPTY: 
				Increment_HIVE_TABLE_EMPTY_COUNTS (LIST_OF_TABLE_STATUS_COUNTS);

				STEP="STEP 7";
				print ("\n"+itab1+"STEP 7. Table is Empty:  Skip Profiling table\t\t"+ getLocalTimeOfCity());
				# This DF is for saving to ORIGINAL Jason Output Table: dfSchemaForOutputTable 
				#    DF has as many rows as there are columns

				STEP="STEP 8";
				print ("\n"+itab1+"STEP 8: Create EMPTY DataFrame for Hive table: '" + schema +"."+
						table +"'\t\t" +getLocalTimeOfCity());
				ERRMSG_IF_FAIL="Cannot Create DataFrame For Empty Hive Table";
				DataFrameOfOutputRows = CreateOutputForZeroRow   (tableNum, totalTables,schema,table,tbl_nm,			\
											tableID,BATCH_ID, allHIVETableColumnsNoDW_LIST,totalHIVETableRowsCount,		\
											tableStartTimeT1, orig_file_name, file_name,  LIST_OF_TABLE_STATUS_COUNTS,	\
											debugVerbose, topNrows, itab2);
				if debugVerbose: print ("\n" +itab2+"Type of DataFrameOfOutputRows="+str(type(DataFrameOfOutputRows)));	#<class 'list'>
				total_Table_Rows    = 0;	# Rows Count
				total_Table_Columns = len(DataFrameOfOutputRows);
				total_JOB_Rows     += total_Table_Rows;
				total_JOB_Columns  += total_Table_Columns;

			else:	#--}{ 
				# TABLE is NOT-EMPTY:

				print ("\n" + itab1 + "Table is NOT Empty ("     + tableRowsCountStr + " rows) " +	\
						"==> Need to profile this table\t\t"     + getLocalTimeOfCity());
				print (       itab2 + "TableNum=" + str(tableNum)+ " of "+ str(totalTables)     +	\
						"; BATCH_ID=" + BATCH_ID + "; TABLE_ID=" + tableID  + "\n" );


				STEP="STEP 7";
				print (itab2 + "STEP 7: Profiling: Loop thru ALL columns to calculate statistics, Columns Batch size: " +	\
					str(DFColumnsBatchSize) + "\t\t" + getLocalTimeOfCity() + "\n");
				ERRMSG_IF_FAIL="Cannot Profile Hive Table"
				statsForALLCol = Process1NewTable (	tableNum, schema, table, tableID, BATCH_ID, registeredDFname,						\
													totalHIVETableRowsCount, timestr, allHIVETableColumnsNoDW_LIST, DFColumnsBatchSize,	\
													"SUCCESS", BIG_TABLE_ROWS_TO_SWITCH,   BIG_TABLE_TABLECOLS_BATCH_SIZE,				\
													BIG_TABLE_STATCOLS_BATCH_SIZE,	URL_GetTableByID, ONE_ROADMASTER_TABLE_DF,			\
													ONE_ROADMASTER_TABLE_LIST, debugVerbose, itab3);

				STEP="STEP 8";
				print (itab2 + "STEP 8: Create DataFrame from List of Output:\t\t"+ getLocalTimeOfCity());
				ERRMSG_IF_FAIL="Cannot Create DataFrame from List of Output";
				DataFrameOfOutputRows = Create_DataFrame_From_List (statsForALLCol,dfSchemaForOutputTable,	\
																	"OUTPUT_1TABLE_DF", -1, debugVerbose,itab3);
				total_Table_Rows    = statsForALLCol[0][4] ;	#Rows Count
				total_Table_Columns = len(statsForALLCol);
				total_JOB_Rows     += int(total_Table_Rows);
				total_JOB_Columns  += total_Table_Columns
			#--} IF stmt: CHECK for table is EMPTY or NOT

			Profile_Elapsed = str(int(math.ceil(TIME2() - Profile_StartTime)) )+ " secs" ;	#Seconds
			if debugVerbose: print (itab1+ "Profile_Elapsed     = " + Profile_Elapsed );

			STEP="STEP 9";
			print (	"\n"+ itab1 + "STEP 9: Save Profiled Rows into Oracle OUTPUT Table:\t\t" + getLocalTimeOfCity()  );
			print (       itab2 + "(TableNum=" + str(tableNum) + " of " + str(totalTables) +	\
				"; BATCH_ID=" + BATCH_ID + "; TABLE_ID=" + tableID  + ")\n" );
			ERRMSG_IF_FAIL           = "Cannot Save Profiled Rows into Oracle OUTPUT Table"
			InsertOutputMS_Elapsed   = "FAILED"	#In case this function call fails, the output string would just be  "failed"
			InsertOutputMS_StartTime = TIME2();	#time()		Type: Float

			WriteRowsToOutput (     DataFrameOfOutputRows, dfSchemaForOutputTable, dfSchemaForORACLEOutputTable,   \
									file_name, orig_file_name, "", "", schema, table, tbl_nm, tableNum,				\
									BATCH_ID, "SUCCESS", URL_Write1TableToOutput, G_MICROSERVICE_AUTHORIZATION,     \
									ONE_ROADMASTER_TABLE_LIST,  tableStartTimeT1_ForMS,  debugVerbose, topNrows, itab2);

			InsertOutputMS_Elapsed = str(int(math.ceil(TIME2() - InsertOutputMS_StartTime)) ) + " secs";
			if debugVerbose: print (itab2+ "InsertOutputMS_Elapsed     = " + InsertOutputMS_Elapsed );

			# NOTE: These next 2 steps: UpdateRowInOracleQUEUEtable and AddTableStatus_to_TIME_SUMMARY_LIST
			#	can be moved into the FINALLY block of this TRY/EXCEPT
			#	But since the UpdateRowInOracleQUEUEtable itself might fail, and if that happens I want
			#	to capture that status in AddTableStatus_to_TIME_SUMMARY_LIST  and in the OUTBOUND call
			#	So it is better to leave these two here instead of the FINNALY block

			STEP="STEP 10";
			print (	"\n"+ itab1 +"STEP 10: Update Oracle QUEUE Table to SUCCESS using MicroService:\t\t" + getLocalTimeOfCity()  );
			print (       itab2 +	"(TableNum="+str(tableNum)+" of "+str(totalTables) +	\
						"; BATCH_ID="+BATCH_ID+"; TABLE_ID="+tableID+")\n" );
			ERRMSG_IF_FAIL          = "Cannot Update Status to 3 (SUCCESS) of Table in Oracle QUEUE table";
			UpdateQueueMS_Elapsed   = "FAILED"	#In case this function call fails, the output string would just be  "failed"
			UpdateQueueMS_StartTime = TIME2();	#time()		Type: Float
			UpdateQueueUsingMicroService (	tableNum, tbl_nm,  tableID, BATCH_ID, SRVR_NM,		\
							RoadMaster_SUCCESS_TaskCD,	tableStartTimeT1_ForMS,	\
							URL_UpdateQueue, G_MICROSERVICE_AUTHORIZATION,		\
							debugVerbose, itab3);
			UpdateQueueMS_Elapsed = str(int(math.ceil(TIME2() - UpdateQueueMS_StartTime)))+ " secs" ;
			if debugVerbose: print (itab2+ "UpdateQueueMS_Elapsed     = " + InsertOutputMS_Elapsed );

			TABLE_STATUS         = "SUCCESS"
			FINAL_TASK_STAT_CODE = RoadMaster_SUCCESS_TaskCD;
			ERRMSG_IF_FAIL       = "";
			Increment_SUCCESS_COUNTS (LIST_OF_TABLE_STATUS_COUNTS);

		except:	#--}{

			Increment_FAIL_COUNTS (LIST_OF_TABLE_STATUS_COUNTS);
			print ("\n\n");
			print ("********** EXCEPTION Block at MAIN():\t\t" + "; TableNum=" + str(tableNum) +	\
					";   BATCH_ID=" + str(BATCH_ID) +	";  TableID=" + tableID );
			print ("**********    ERRMSG:    " + ERRMSG_IF_FAIL + "    ("+STEP+")")
			print ("**********    Exception: " + str(sys.exc_info()[0]) )
			print ("**********");

			TABLE_STATUS         = "FAILED"
			FINAL_TASK_STAT_CODE = RoadMaster_FAIL_TaskCD;
			# Variable ERRMSG_IF_FAIL contains whatever is set at the appropriate STEP above

			print ("\n" +itab2 +	"Update QUEUE Table with FAIL taskCD=" + str(RoadMaster_FAIL_TaskCD) + \
						"\t== " + ConvertTaskStatCodeToString (RoadMaster_FAIL_TaskCD));
			try:
				ERRMSG_IF_FAIL          = "Cannot Update Status to 3 (SUCCESS) of Table in Oracle QUEUE table";
				UpdateQueueMS_Elapsed   = "FAILED"	#In case this function call fails, the output string would just be  "failed"
				UpdateQueueMS_StartTime = TIME2();	#time()		Type: Float
				UpdateQueueUsingMicroService (	tableNum, tbl_nm,  tableID, BATCH_ID, SRVR_NM,	\
												RoadMaster_FAIL_TaskCD,	tableStartTimeT1_ForMS,	\
												URL_UpdateQueue, G_MICROSERVICE_AUTHORIZATION,	\
												debugVerbose, itab3);
				UpdateQueueMS_Elapsed = str(int(math.ceil(TIME2() - UpdateQueueMS_StartTime)))+ " secs" ;
				if debugVerbose: print (itab2+ "UpdateQueueMS_Elapsed     = " + InsertOutputMS_Elapsed );
			except:
				print ("\n\n\n********** ERROR: Function UpdateQueueUsingMicroService() failed WHILE INSIDE EXCEPTION BLOCK!\n\n\n")
				print (itab1 + "==> IGNORE THIS EXCEPTION.  No need to reraise as it is at the last step!");
				print (itab1 + "At MAIN() Except Block: EXCEPTION: " + str(sys.exc_info()[0])  + "\n\n\n" );

		finally:
			STEP="STEP 11";
			print (	"\n"+ itab1 +	"STEP 11: Send OutBound Message with TaskCode=" + str(FINAL_TASK_STAT_CODE)	+ \
						"\t== " + ConvertTaskStatCodeToString (FINAL_TASK_STAT_CODE)			+ \
						";  ERRMSG IF FAIL=\"" + ERRMSG_IF_FAIL						+ \
						"\"\t\t" + getLocalTimeOfCity()  );
			print (       itab2 + "TableName=" + tbl_nm + ";     BATCH_ID=" + BATCH_ID + ";     TABLE_ID=" + tableID  + ")\n" );
			# This step has its own TRY block because whether it is successful or not it should not interfere 
			# with the main Profiling job
			try:
				OutBoundMS2_StartTime = TIME2();	#time()		Type: Float
				OutBoundMS2_Elapsed   = "FAILED"
				ERRMSG_IF_FAIL        ="Cannot Send OutBound Message2"
				SendToMessageToOutBound (tableNum, totalTables, schema, table, tbl_nm, tableID, BATCH_ID,	\
						FINAL_TASK_STAT_CODE, ERRMSG_IF_FAIL, OutBoundAccessToken, URL_OUTBOUND,	\
						debugVerbose,   itab2);
				OutBoundMS2_Elapsed = str(int(math.ceil(TIME2() - OutBoundMS2_StartTime)))+ " secs" ;
				if debugVerbose: print (itab2+ "OutBoundMS2_Elapsed     = " + OutBoundMS2_Elapsed ) 
			except:
				print ("\n" + itab1 + "********** ERROR: EXCEPTION: " + str(sys.exc_info()[0])  + "\n" );
				print (       itab2 + "Function SendToMessageToOutBound() failed!")
				print (       itab2 + "IGNORE THIS EXCEPTION as it is external\n\n")


			STEP="STEP 12";
			print ("\n"+itab1 + "STEP 12: Cleanup Memory Usage 1" + "\t\t" + getLocalTimeOfCity()  );
			try:
				cache_time=TIME2();
				spark.catalog.clearCache();
				if debugVerbose: print(itab3 + "Clear Cache:\t\tTook: "+str(TIME2() - cache_time));
			except: print ("\n\n\n********** WARNING: Cannot Clear Cache () **********\n\n\n");


			STEP="STEP 13";
			print ("\n"+itab1 + "STEP 13: Cleanup Memory Usage 2" + "\t\t" + getLocalTimeOfCity()  );
			try:
				gc_time=TIME2();
				gc.collect();
				if debugVerbose: print(itab3 + "Java Garbage Collect:\tTook: "+str(TIME2() - gc_time) );
			except: print ("\n\n\n********** WARNING: Cannot Garbage Collect() **********\n\n\n");


			STEP="STEP 14";
			print ("\n"+itab1 +"STEP 14: Insert TIME_SUMMARY_LIST\t" + getLocalTimeOfCity()  );
			AddTableStatus_to_TIME_SUMMARY_LIST (	TIME_SUMMARY_LIST, TABLE_STATUS,	\
					RM_TotalColumnsStr, HIVE_TotalColumnsStr, tableRowsCountStr,		\
					tableStartTimeT1, tableStartTimeT2,				\
					OutBoundMS1_Elapsed,		\
					HiveCheckTable_Elapsed,		\
					HiveRowsCount_Elapsed,		\
					HiveGetColumns_Elapsed,		\
					RoadMasterMS_Elapsed,		\
					ColsFilter_Elapsed,			\
					Profile_Elapsed,			\
					InsertOutputMS_Elapsed,		\
					UpdateQueueMS_Elapsed,		\
					OutBoundMS2_Elapsed,		\
					debugVerbose, topNrows, itab2);
			tableEndT1     = TIME1.strftime("%Y%m%d %H:%M:%S");
			tableEndTimeT2 = TIME2();	#time()

			print ("\n\n" + itab1+"~"*200);
			print (itab1+"TABLE "+ str(tableNum) + "/" + str(totalTables) + ":  STATUS=" + TABLE_STATUS + "; " + tbl_nm +\
				";  Rows: "    + str(total_Table_Rows)    +										\
				";  Columns: " + str(total_Table_Columns) +										\
				";  Took: "    + ElapsedTimeStrFrom(tableStartTimeT2,TIME2(), debugVerbose) +	\
				";  (Start: "  + tableStartTimeT1 +";  End: "+ tableEndT1 +	")"  )
			print (itab1+"~"*200);
			print ("\n\n");

		#--} TRY/EXCEPT block of Current Table 
	#--} FOR LOOP: each Table of input file


	print ("\n\n");
	print ("="*160);
	print ("END OF FOR LOOP  for  All Tables in SplitFile: \"" + file_name + "\"\t\t" + getLocalTimeOfCity());
	print ("\n\tFINAL TABLE STATUS: " + file_name  + "    (BATCH_ID=" + str(BATCH_ID) + ")" )
	DisplayLIST_OF_TABLES_COUNT (LIST_OF_TABLE_STATUS_COUNTS, totalTables);
	print ("="*160);

	jobEnd     = TIME1.strftime("%Y%m%d %H:%M:%S")
	jobEndTime = TIME2();	#time()
	print ("\n\n"+"~"*300);
				# This token "SPARK JOB COMPLETED" is being grep for in the shell script.
				# If change, must change in both place
	print ("SPARK JOB COMPLETED"								+ \
		"; SRVR_NM="       + SRVR_NM							+ \
		"; BATCH_ID="      + str(BATCH_ID)						+ \
		"; SplitFile:"     + file_name							+ \
		"; Total_Tables:"  + str(totalTables)					+ \
		"; Total_Columns:" + str(total_JOB_Columns)				+ \
		"; Total_Rows:"    + str(total_JOB_Rows)				+ \
		"; Total_Time:"    + ElapsedTimeStrFrom(jobStartTime,TIME2(), debugVerbose)	+	\
		"; (Local Start:"  + jobStart + " - " + jobEnd +	")\t"					+	\
		getLocalTimeOfCity() + "\n" )
	print ("\n\n"+"~"*300);

	#Convert TIME_SUMMARY_LIST  to Dataframe for better display:
	rdd = spark.sparkContext.parallelize(TIME_SUMMARY_LIST, 1).map (									\
				lambda x: Row(	x[0],  x[1],  x[2],  x[3],  x[4],  x[5],  x[6],  x[7],  x[8],  x[9],	\
								x[10], x[11], x[12], x[13], x[14], x[15], x[16], x[17], x[18], x[19],	\
								x[20], x[21]));
	TableSummary_DF=spark.createDataFrame (rdd, dfSchemaForTableTimeSummary);

	print ("BEGIN_FINAL_SUMMARY");
	DisplayTopNrows (TableSummary_DF, -1, "", "Final Summary")
	print ("END_FINAL_SUMMARY");

	print ("~"*260);

	sys.exit(0);	# Return Success as ALL tables have completed successfully



#	print ("\nTotal Input Tables="+str(totalTables) + ";  SUCCESS tables=" + str(LIST_OF_TABLE_STATUS_COUNTS[0]), end='');
#	if LIST_OF_TABLE_STATUS_COUNTS[0] == totalTables:		#if SUCCESS table count < total tables
#		print ("\t\t==> Spark Exits with code 0 -- JOB SUCCESS\t\t" + getLocalTimeOfCity() + "\n\n")
#		sys.exit(0);	# Return Success as ALL tables have completed successfully
#	else:
#		print ("\t\t==> Spark Exits with code 1 -- JOB FAILED\t\t" + getLocalTimeOfCity() + "\n\n")
#		sys.exit(1);	# Return Success as ALL tables have completed successfully


else:	#--} 
	print("\n\n********** ERROR: Cannot locate input file in HDFS:" + file + "\n\n")
#------------------------------------------- END END END END END END END ------------------------------------------
# https://hadoopist.wordpress.com/2016/05/24/generate-unique-ids-for-each-rows-in-a-spark-dataframe/




