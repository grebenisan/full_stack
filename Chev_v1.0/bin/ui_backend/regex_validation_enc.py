# coding=utf-8
# Date: 02/16/2018
# Author: Dan Grebenisan
# Chevelle
# Native Python Version
# Validates a regular expression against a sample dataset

import sys
import re

regex = sys.argv[1]
file_name = sys.argv[2]
file_dir = '/data/commonScripts/util/chevelle_gui/user_data/'

if '©' in regex:
    regex_decoded = regex.replace('©',chr(92))
else:
    regex_decoded = regex

with open('{0}/{1}'.format(file_dir, file_name)) as f:

    data = f.read().split('\n')
  
    for sample_text in data:
        matched = re.match(regex_decoded,sample_text)
        if matched:
            sys.stdout.write(matched.group()+ '\n')
        else:
            pass
