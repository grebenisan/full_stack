#!/bin/ksh

#############################################################
# Author: Dan Grebenisan
# Pupose: runs a regular expression against a sample data file
# Description: The regular expression in not encoded in this version of the script
#
# Examples:
# ./chevelle_validate_regexp.sh '^[a-zA-Z]{1,15}\s?[a-zA-Z]{1,15}$' sample_names.txt
# ./chevelle_validate_regexp.sh '^[a-zA-Z0-9._-]*@.*[^a-z0-9A-Z._-]*[@]*\..*' sample_emails.txt
#
##############################################################

regexp_definition=$1
file_name=$2

baseDir="/data/commonScripts/util/chevelle_gui"
dataDir="${baseDir}/user_data"

echo "$regexp_definition" > current_regexp.txt

# grep the regular expression for the given sample file
grep -E "$regexp_definition" "${dataDir}/$file_name"

