#!/bin/sh

#Config file that launches profiler script for regexp validation#
#Dan Grebenisan#
#2/12/2018#

regexp=$1
file_name=$2

python regex_validation_enc.py "$regexp" "$file_name"
