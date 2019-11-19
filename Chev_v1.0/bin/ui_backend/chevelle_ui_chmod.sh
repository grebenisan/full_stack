##################################################################
# Script: chevelle_ui_chmod.sh
# Description: Called by the TFS deployment task
# 				Set the permission of the Chevelle back-end scripts
# Author: Dan Grebenisan
# History: Apr 2018 - Created for the TFS deployer
##################################################################

if [ -f /data/commonScripts/util/chevelle_gui/chevelle_sample_data.sh ]
then
	chmod 775 /data/commonScripts/util/chevelle_gui/chevelle_sample_data.sh
	echo "chevelle_sample_data.sh: permission changed to 775"
else
	echo "chevelle_sample_data.sh: file does not exist"
fi

if [ -f /data/commonScripts/util/chevelle_gui/chevelle_validate_regexp.sh ]
then
	chmod 775 /data/commonScripts/util/chevelle_gui/chevelle_validate_regexp.sh
	echo "chevelle_validate_regexp.sh: permission changed to 775"
else
	echo "chevelle_validate_regexp.sh: file does not exist"
fi

if [ -f /data/commonScripts/util/chevelle_gui/chevelle_validate_regexp_enc.sh ]
then
	chmod 775 /data/commonScripts/util/chevelle_gui/chevelle_validate_regexp_enc.sh
	echo "chevelle_validate_regexp_enc.sh: permission changed to 775"
else
	echo "chevelle_validate_regexp_enc.sh: file does not exist"
fi

if [ -f /data/commonScripts/util/chevelle_gui/regex_validation.py ]
then
	chmod 775 /data/commonScripts/util/chevelle_gui/regex_validation.py
	echo "regex_validation.py: permission changed to 775"
else
	echo "regex_validation.py: file does not exist"	
fi

if [ -f /data/commonScripts/util/chevelle_gui/regex_validation.sh ]
then
	chmod 775 /data/commonScripts/util/chevelle_gui/regex_validation.sh
	echo "regex_validation.sh: permission changed to 775"
else
	echo "regex_validation.sh: file does not exist"	  
fi

if [ -f /data/commonScripts/util/chevelle_gui/regex_validation_enc.py ]
then
	chmod 775 /data/commonScripts/util/chevelle_gui/regex_validation_enc.py
	echo "regex_validation_enc.py: permission changed to 775"
else
	echo "regex_validation_enc.py: file does not exist"	    
fi

if [ -f /data/commonScripts/util/chevelle_gui/regex_validation_enc.sh ]
then
	chmod 775 /data/commonScripts/util/chevelle_gui/regex_validation_enc.sh
	echo "regex_validation_enc.sh: permission changed to 775"
else
	echo "regex_validation_enc.sh: file does not exist"	    	
fi

if [ -f /data/commonScripts/util/chevelle_gui/regex_validation_native.py ]
then
	chmod 775 /data/commonScripts/util/chevelle_gui/regex_validation_native.py
	echo "regex_validation_native.py: permission changed to 775"
else
	echo "regex_validation_native.py: file does not exist"	    		
fi

if [ -f /data/commonScripts/util/chevelle_gui/regex_validation_native.sh ]
then
	chmod 775 /data/commonScripts/util/chevelle_gui/regex_validation_native.sh
	echo "regex_validation_native.sh: permission changed to 775"
else
	echo "regex_validation_native.sh: file does not exist"	    			
fi

echo "Permission of the files in /data/commonScripts/util/chevelle_gui has changed."
echo "This is the list of EdgeNode loaded scripts supporting the Chevelle UI:"
ls -l /data/commonScripts/util/chevelle_gui

