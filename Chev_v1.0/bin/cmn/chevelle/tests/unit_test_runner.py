# -*- coding: utf-8 -*-

"""
Unit test runner for test suite.
Author: Dan Grebenisan
Updated on: 09/26/2018
"""

import unittest
from test_hyper_profiling import TestHPUnits, TestHPEndToEnd

def function_suite():
    suite = unittest.TestSuite()
    suite.addTest(TestHPUnits('test_instantiate_abstract_base_class'))
    suite.addTest(TestHPUnits('test_update_queue'))
    suite.addTest(TestHPUnits('test_save_output'))
    suite.addTest(TestHPUnits('test_protocol'))
    suite.addTest(TestHPUnits('test_input_file_extension'))
    suite.addTest(TestHPUnits('test_input_file_schema'))
    suite.addTest(TestHPUnits('test_get_jwt_token'))
    suite.addTest(TestHPUnits('test_update_outbound_queue'))

    return suite

test_function_suite = function_suite()
function_suite_test_runner = unittest.TextTestRunner()
function_suite_test_runner.run(test_function_suite)
