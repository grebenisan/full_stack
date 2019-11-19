##################################################################
# Script: chevelle_ui_mk_dirs.sh
# Description: Called by the TFS deployment task
# 				Creates the the Chevelle UI back-end directories
# Author: Dan Grebenisan
# History: Apr 2018 - Created for the TFS deployer
##################################################################

# if [ ! -d /data/commonScripts/util ]
# then
#	mkdir /data/commonScripts/util
#	chmod 775 /data/commonScripts/util
#	echo "Directory /data/commonScripts/util has been created!"
# else
#	echo "Directory /data/commonScripts/util exists already! No action taken."
# fi

if [ ! -d /data/commonScripts/util/chevelle_gui ]
then
	mkdir /data/commonScripts/util/chevelle_gui
	chmod 775 /data/commonScripts/util/chevelle_gui
	echo "Directory /data/commonScripts/util/chevelle_gui has been created!"
else
	echo "Directory /data/commonScripts/util/chevelle_gui exists already! No action taken."	
fi

if [ ! -d /data/commonScripts/util/chevelle_gui/user_data ]
then
	mkdir /data/commonScripts/util/chevelle_gui/user_data
	chmod 775 /data/commonScripts/util/chevelle_gui/user_data
	echo "Directory /data/commonScripts/util/chevelle_gui/user_data has been created!"
else
	echo "Directory /data/commonScripts/util/chevelle_gui/user_data exists already! No action taken."		
fi

