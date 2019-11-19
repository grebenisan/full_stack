##################################################################
# Script: chevelle_sample_data.sh
# Description: called by the Chevelle GUI back-end - GetSampleDataServlet, fia SSH connection
# Parameters: $1 - the hadoop path of the data directory
#			  $2 - how many records to retrieve from the first file in that directory
# Author: Dan Grebenisan
# History: Feb 2018 - Created
##################################################################

hdfs_path=$1
sample_size=$2

hadoop fs -ls "$hdfs_path/*" | awk  '{print $8}' |
    while read i
	do 
		# test if this is a file, not a directory, or a file in a sub-directory: hdfs -test -f
		if $(hadoop fs -test -f $i) 
		then 
			# echo "filename: $i"
			hadoop fs -cat "$i" | head -"$sample_size" 2> /dev/null
			break
		fi
    done
