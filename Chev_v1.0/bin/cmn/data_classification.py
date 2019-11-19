# author: Dan Grebenisan
# Date: 09-14-2018
# Instantiate HyperProfiling class, and run data classification method from class

import sys
from hyper_profiling import HyperProfiling

file_name = sys.argv[1]
classify = HyperProfiling()
print(repr(classify))
classify.classify_data(file_name)
