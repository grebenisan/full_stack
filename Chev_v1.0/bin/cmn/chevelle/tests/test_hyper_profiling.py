# -*- coding: utf-8 -*-

"""
Test cases for hyper_profiling_1_1.py.
Author: Dan Grebenisan
Updated on: 09/26/2018
"""

import unittest
import requests
import json
from hyper_profiling_1_1 import OraclePersistence, \
    SaveOutput, \
    UpdateQueue, \
    HyperProfiling, \
    InvalidSchemaException, \
    HDFSFileException, \
    InvalidFileExtension, \
    HttpProtocolException


from pyspark.sql import SparkSession

class TestHPUnits(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.appName("Chevelle Hyper Data Profiling").enableHiveSupport().getOrCreate()
        self.main_class = HyperProfiling()

        self.input_file_correct = '0180919_143055__j1__e010__t100__dc_list1.txt'
        self.input_file_bad_extension = '20180912_142538__j800__e010__t2__dc_list800_bad_ext_test.bap'
        self.input_file_bad_schema = '20180912_142538__j800__e010__t2__dc_list800__bad_schema_test.txt'

        self.url_protocol = HyperProfiling.get_url('/dataclass/que/update').get('url').split(':')[0]

        self.save = HyperProfiling.get_url('/dataclass/out/save').get('url')
        self.update = HyperProfiling.get_url('/dataclass/que/update').get('url')
        self.token_url = 'https://chevelle-messaging-dev.cp-epg2i.domain.com'
        self.messaging_url = HyperProfiling.get_url('/events/data_class/outbound/updateQueue').get('messaging_url')

    def test_instantiate_abstract_base_class(self):
        self.assertRaises(TypeError, OraclePersistence,{'url':self.save,'session': requests.session()})

    def test_save_output(self):
        data = {
            "col_id": "45EBB33440EEF40D80500AB5CF9624E01E16AE2B",
            "data_cls_nm": "Drivers License Number",
            "crt_ts": "2018-09-05 10:24:43",
            "prof_start_ts": "2018-09-14 03:32:10",
            "prof_end_ts": "2018-09-14 03:35:10",
            "table_id": "00010978278AA78B3466EA110A22DD719E134F85",
            "batch_id": 352,
            "tot_row_cnt": 1423484,
            "sample_row_cnt": 142181,
            "col_val_uniq_cnt": 7616,
            "col_val_data_cls_cnt": 15,
            "col_max_val": "9998",
            "col_min_val": "100",
            "col_avg_len": 4,
            "appl_regex_str": "^[0-9]{6,14}|[A-Z]{1,3}[0-9]{3,14}|[0-9]{2}[A-Z]{3}[0-9]{5}|[0-9]{3}[A-Z]{2}[0-9]{4}|[0-9]{7}[A-Z]{1}$",
            "crt_by": "chevelle"
            }

        data["appl_regex_str"] = data["appl_regex_str"].\
            replace(chr(92), chr(169)).\
            replace(chr(123), chr(171)). \
            replace(chr(125), chr(187))
        save_output = SaveOutput({'save': self.save,
                                  'session': requests.session()})
        output_response = save_output.save_results({'payload': data})

        self.assertEqual(output_response['status'], 200,
                         msg='{output_response_reason},'
                             '{output_response_content}'.format(output_response_reason=output_response['reason'],
                                                                output_response_content=output_response['content']))

    def test_update_queue(self):
        data = {
                "batch_id": 352,
                "col_id": "45EBB33440EEF40D80500AB5CF9624E01E16AE2B",
                "data_cls_nm": "Drivers License Number",
                "fail_cnt": 0,
                "task_stat_cd": 2
                }
        update_queue_table = UpdateQueue({'update': self.update, 'session': requests.session()})
        update_response = update_queue_table.save_results({'update_output': data})

        self.assertEqual(update_response['status'], 200,
                         msg='{update_response_reason},'
                             '{update_response_content}'.format(update_response_reason=update_response['reason'],
                                                                update_response_content=update_response['content']))

    def test_get_jwt_token(self):
        auth = {'SECRET_ID': '3YmCdvHBKX;UwXrn'}
        response = requests.post(self.token_url, json=auth)
        access_token = json.loads(response.content.decode('utf-8'))['access_token']
        self.assertEqual(response.status_code, 200)
        self.assertTrue(access_token)

    def test_update_outbound_queue(self):
        login = {'SECRET_ID': '3YmCdvHBKX;UwXrn'}
        auth = requests.post('https://chevelle-messaging.cp-epg2i.domain.com/authenticate', json=login)
        access_token = json.loads(auth.content.decode('utf-8'))['access_token']

        header = \
            {
                "Content-Type": "application/json",
                "Cache-Control": "no-cache",
                "Authorization": "Bearer {token}".format(token=access_token)
            }

        message = {
            'col_id': '00000',
            'data_cls_nm': 'NAme',
            'batch_id': 124,
            'task_stat_cd': 4,
            'fail_cnt': 0
        }

        r = requests.post(self.messaging_url,
                          json=json.dumps(message),
                          headers=header)
        self.assertEqual(r.status_code, 200)

    def test_input_file_extension(self):

        self.assertRaises(SystemExit, self.main_class.classify_data, self.input_file_bad_extension)

    def test_input_file_schema(self):

        self.assertRaises(SystemExit, self.main_class.classify_data, self.input_file_bad_schema)

    def test_protocol(self):
        self.assertEqual(self.url_protocol, 'https')


class TestHPEndToEnd(unittest.TestCase):

    def setUp(self):
        self.input_file_correct = '0180919_143055__j1__e010__t100__dc_list1.txt'
        self.main_class = HyperProfiling()

    def test_end_to_end(self):

        self.main_class.classify_data(self.input_file_correct)
