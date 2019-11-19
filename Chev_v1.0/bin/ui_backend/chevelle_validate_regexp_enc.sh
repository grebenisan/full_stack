#!/bin/ksh

#############################################################
# Author: Dan Grebenisan
# Pupose: runs a regular expression against a sample data file
# Description: The regular expression has the backslash character encoded with © to avoid processing issues
#
# Examples:
# ./chevelle_validate_regexp.sh '^[a-zA-Z]{1,15}©s?[a-zA-Z]{1,15}$' sample_names.txt
# ./chevelle_validate_regexp.sh '^[a-zA-Z0-9._-]*@.*[^a-z0-9A-Z._-]*[@]*©..*' sample_emails.txt
#
##############################################################

regexp_encoded=$1
file_name=$2

typeset -L1 one_char
integer char_counter
regexp_decoded=""

baseDir="/data/commonScripts/util/chevelle_gui"
dataDir="${baseDir}/user_data"

echo "$regexp_encoded" > encoded_regexp.txt

#decode the regexp definition
regexp_length=${#regexp_encoded}

char_counter=0
while ((char_counter < regexp_length))
do
	one_char=$regexp_encoded
	regexp_encoded=${regexp_encoded#?}
	if [[ "$one_char" = "©" ]]
	then
		regexp_decoded=$regexp_decoded"\\"   # it works with "\\" and with '\\' too
	else
		regexp_decoded="$regexp_decoded$one_char"
	fi
	regexp_length=${#regexp_encoded}
done

echo "$regexp_decoded" >> encoded_regexp.txt

# grep the decoded regular expression for the given sample file
grep -E "$regexp_decoded" "${dataDir}/$file_name"

