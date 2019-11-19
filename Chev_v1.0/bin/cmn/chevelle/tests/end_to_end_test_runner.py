# -*- coding: utf-8 -*-

"""
End to End test runner.
Author: Dan Grebenisan
Updated on: 09/19/2018
"""

import unittest
from test_hyper_profiling import TestHPUnits, TestHPEndToEnd

def end_to_end_suite():
    suite = unittest.TestSuite()
    suite.addTest(TestHPEndToEnd('test_end_to_end'))
    return suite

test_end_to_end_suite = end_to_end_suite()
end_to_end_test_runner = unittest.TextTestRunner()
end_to_end_test_runner.run(test_end_to_end_suite)