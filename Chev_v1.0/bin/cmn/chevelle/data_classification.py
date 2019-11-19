# -*- coding: utf-8 -*-

"""
Instantiate HyperProfiling class, and run data classification method from class.
Author: Dan Grebenisan
Updated on: 09/26/2018
"""

import sys
from hyper_profiling_1_1 import HyperProfiling

file_name = sys.argv[1]
classify = HyperProfiling()
print(repr(classify))
classify.classify_data(file_name)
