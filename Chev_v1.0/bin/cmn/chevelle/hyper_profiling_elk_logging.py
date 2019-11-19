# -*- coding: utf-8 -*-

"""
Find suspected columns to encrypt.
Author: Dan Grebenisan
Updated on: 09/21/2018
"""

import sys
import subprocess
import datetime
import time
import traceback
import json
from abc import ABCMeta, abstractmethod
import pytz
from collections import OrderedDict
import requests

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql.functions import lower, trim, lit, regexp_replace

timezone = pytz.timezone("US/Central")


class Error(Exception):
    """User defined base exception class."""
    pass


class HDFSFileException(Error):
    """Exception if file doesn't exist in HDFS."""
    def __init__(self, file_):
        self.file_ = file_
        super(Exception, self).__init__('{file} does not exist. '
                                        'Exiting....'.format(file=self.file_))


class InvalidSchemaException(Error):
    """Exception if file has more or less than the required number of columns."""
    def __init__(self, file_, col_cnt):
        self.file_ = file_
        super(Exception, self).__init__('{file} has {col_cnt} '
                                        'instead of 9 columns. '
                                        'Invalid schema'.format(file=self.file_, col_cnt=col_cnt))


class InvalidFileExtension(Error):
    """Exception if file doesn't have extenstions txt, csv, or dat."""
    def __init__(self, file_, file_ext):
        self.file_ = file_
        self.file_ext = file_ext
        super(Exception, self).__init__('{file} must have '
                                        'txt extension. '
                                        'Your file has *.{ext}. Invalid file'.format(file=self.file_, ext=file_ext))


class HttpProtocolException(Error):
    """Exception if url doesn't have https protocol."""
    def __init__(self, url1, url2, protocol1, protocol2):
        self.url1 = url1
        self.protocol1 = protocol1
        self.url2 = url2
        self.protocol2 = protocol2
        super(Exception, self).__init__('{protocol1} and {protocol2} methods not allowed '
                                        '{url1} and {url2} are invalid'.format(url1=self.url1,
                                                                               url2=self.url2,
                                                                               protocol1=self.protocol1,
                                                                               protocol2=self.protocol2))


class OraclePersistence(metaclass=ABCMeta):
    """Abstract Base Class for saving to, and updating Oracle tables"""
    header = {"Content-Type": "application/json", "Cache-Control": "no-cache",
              "Authorization": "Bearer abc123ABC123 ...."}

    def __init__(self, session_config):
        self.session_config = session_config

    @abstractmethod
    def save_results(self, persistence_configuration):
        """Output abstract base class."""
        pass


class SaveOutput(OraclePersistence):
    """Subclass for saving to Oracle output table."""
    def __init__(self, session_config):
        super().__init__(session_config)

    def save_results(self, persistence_configuration):
        """Save profile results to output table in Oracle."""
        url = self.session_config['save']
        session = self.session_config['session']
        with session as session_object:
            payload = persistence_configuration['payload']
            session_object.keep_alive = False
            session_object.headers.update(self.header)
            response = session_object.post(url, data=json.dumps(payload, separators=(',', ':')))
        return {'status': response.status_code, 'reason': response.reason, 'content': response.content}


class UpdateQueue(OraclePersistence):
    """Subclass for updating Oracle queue table."""
    def __init__(self, session_config):
        super().__init__(session_config)

    def save_results(self, persistence_configuration):
        """Update queue table in Oracle."""
        url = self.session_config['update']
        session = self.session_config['session']
        with session as session_object:
            payload = persistence_configuration['update_output']
            session_object.keep_alive = False
            session_object.headers.update(self.header)
            response = session_object.put(url, json=payload)
        return {'status': response.status_code, 'reason': response.reason, 'content': response.content}


class HyperProfiling:
    """Main class for data profiling."""
    _user_id = subprocess.Popen('whoami', stdout=subprocess.PIPE).communicate()[0].decode('utf-8').upper().strip()
    _hostname = subprocess.Popen('hostname', stdout=subprocess.PIPE).communicate()[0].decode('utf-8').upper().strip()
    _spark = SparkSession.builder.appName("Chevelle Hyper Data Profiling").enableHiveSupport().getOrCreate()
    _env = _spark._jsc.hadoopConfiguration().get('dfs.internal.nameservices')[5:-3].upper()
    _endpoint_env = _env.lower()

    def __init__(self):
        self.app_name = 'Chevelle Hyper Data Profiling'

    def __repr__(self):
        repr_ = {'class': self.__class__.__name__, 'app_name': self.app_name}
        return '{class}({app_name!r})'.format(**repr_)

    @staticmethod
    def get_session():
        """Returns http session object."""
        return requests.session()

    @classmethod
    def get_url(cls, endpoint):
        """Returns url based on environment (dev, test, prod)."""
        if cls._endpoint_env == 'dev':
            url = 'https://chevelle-micro-dev-enc.cp-epg2i.domain.com' + endpoint
            return url
        elif cls._endpoint_env == 'tst':
            url = 'https://chevelle-micro-test.cp-epg2i.domain.com' + endpoint
            return url
        elif cls._endpoint_env == 'crt':
            url = 'https://chevelle-micro-qa.cp-epg2i.domain.com' + endpoint
            return url
        else:
            url = 'https://chevelle-micro.cp-epg2i.domain.com' + endpoint
            return url

    def classify_data(self, file_name):
        """Profiles columns from input file."""
        try:
            session_id = datetime.datetime.now(timezone).strftime("%Y-%m-%d %H:%M:%S")
            print("\n\n=====Python Job starts at {session_id}\n".format(session_id=session_id))

            save = HyperProfiling.get_url('/dataclass/out/save')
            update = HyperProfiling.get_url('/dataclass/que/update')

            if save.split(':')[0] and update.split(':')[0] != 'https':
                raise HttpProtocolException(save, update, save.split(':')[0], update.split(':')[0])

            session = HyperProfiling().get_session()

            hdfs_output_dir = '/{env}/EDW/DSODB/OPS/CHEVELLE/HYPER_PROF_RSLT/Output'.format(env=self._env)
            input_path = '/{env}/EDW/DSODB/OPS/CHEVELLE/HYPER_PROF_RSLT/Input/{file_name}'

            base_query = \
                """
                select 
                cast('{COL_ID}' as string) as col_id,
                cast('{DATA_CLS_NM}' as string) as data_cls_nm,
                cast('{CRT_TS}' as string) as crt_ts,
                cast('{PROF_START_TS}' as string) as prof_start_ts,
                cast('{PROF_END_TS}' as string) as prof_end_ts,
                cast('{TABLE_ID}' as string) as table_id,
                cast('{BATCH_ID}' as int) as batch_id,
                cast('{TOTAL_ROW_COUNT}' as int) as tot_row_cnt,
                cast('{SAMPLE_ROW_COUNT}' as int) as sample_row_cnt,
                cast(count(distinct({column})) as int) as col_val_uniq_cnt, 
                cast(sum(case when {column} REGEXP '{REGEX_STR}' then 1 else 0 end) as int) as col_val_data_cls_cnt,
                cast(max({column}) as string) as col_max_val,
                cast(min({column}) as string) as col_min_val, 
                cast(avg(length({column})) as int) as col_avg_len,
                cast('{REGEX_STR}' as string) as appl_regex_str,
                cast('{CRT_BY}' as string) as crt_by 
                from {temp_table}
                """

            file_schema = StructType([
                    StructField("hive_schema", StringType()),
                    StructField("table_name", StringType()),
                    StructField("col_name", StringType()),
                    StructField("data_cls_nm", StringType()),
                    StructField("regex_str", StringType()),
                    StructField("table_id", StringType()),
                    StructField("col_id", StringType()),
                    StructField("batch_id", StringType()),
                ])

            empty_schema = StructType([
                StructField("col_id", StringType()),
                StructField("data_cls_nm", StringType()),
                StructField("crt_ts", StringType()),
                StructField("prof_start_ts", StringType()),
                StructField("prof_end_ts", StringType()),
                StructField("table_id", StringType()),
                StructField("batch_id", StringType()),
                StructField("tot_row_cnt", StringType()),
                StructField("sample_row_cnt", StringType()),
                StructField("col_val_uniq_cnt", StringType()),
                StructField("col_val_data_cls_cnt", StringType()),
                StructField("col_max_val", StringType()),
                StructField("col_min_val", StringType()),
                StructField("col_avg_len", StringType()),
                StructField("appl_regex_str", StringType()),
                StructField("crt_by", StringType())
            ])

            file_ = input_path.format(env=self._env, file_name=file_name)
            print(file_)

            schema_check = self._spark.read.csv(file_, sep='\x1c')
            schema_length = len(schema_check.columns)

            file_extension = file_name.split('.')[1]
            if file_extension not in ('txt', 'csv', 'dat'):
                raise InvalidFileExtension(file_name, file_extension)
            elif schema_length < 8:
                raise InvalidSchemaException(file_name, schema_length)
            else:
                print("File is ok to process")

            input_file_base = self._spark.read.csv(file_, sep='\x1c', schema=file_schema)
            input_file = input_file_base.withColumn('hive_schema', regexp_replace('hive_schema', "\-", "_"))
            input_restructured = input_file. \
                select('table_id',
                       'col_id',
                       'batch_id',
                       'data_cls_nm',
                       'col_name',
                       'regex_str',
                       F.concat_ws('.', 'hive_schema', 'table_name').alias('table')). \
                orderBy('table', ascending=False)
            input_tables = input_restructured.select(input_restructured['table']).dropDuplicates()
            input_tables_broadcast = self._spark.sparkContext.broadcast(input_tables.rdd.collect())

            application_start = time.time()

            for tables in input_tables_broadcast.value:

                rows_to_profile = \
                    input_restructured.select('table_id',
                                              'col_id',
                                              'batch_id',
                                              'col_name',
                                              'data_cls_nm',
                                              'regex_str'). \
                        where(input_restructured['table'] == '{table}'.format(table=tables.table)).toLocalIterator()

                for row in list(rows_to_profile):
                    col_id = row.col_id
                    table_id = row.table_id
                    batch_id = int(row.batch_id)
                    data_cls_nm = row.data_cls_nm
                    col_name = row.col_name
                    regex_str = row.regex_str. \
                        replace(chr(169), chr(92)). \
                        replace(chr(171), chr(123)). \
                        replace(chr(187), chr(125))

                    empty_ts = datetime.datetime.now(timezone).strftime("%Y-%m-%d %H:%M:%S")

                    failed_update_schema = \
                        {
                            'col_id': col_id,
                            'data_cls_nm': data_cls_nm,
                            'batch_id': batch_id,
                            'task_stat_cd': 3,
                            'fail_cnt': 0
                        }

                    success_update_schema = \
                        {
                            'col_id': col_id,
                            'data_cls_nm': data_cls_nm,
                            'batch_id': batch_id,
                            'task_stat_cd': 2,
                            'fail_cnt': 0
                        }

                    empty_update_schema = \
                        {
                            'col_id': col_id,
                            'data_cls_nm': data_cls_nm,
                            'batch_id': batch_id,
                            'task_stat_cd': 4,
                            'fail_cnt': 0
                        }

                    empty_record = (
                        col_id,
                        data_cls_nm,
                        empty_ts,
                        empty_ts,
                        empty_ts,
                        table_id,
                        batch_id,
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        regex_str.replace(chr(92), chr(169)).replace(chr(123), chr(171)).replace(chr(125), chr(187)),
                        "chevelle"
                    )

                    try:
                        table_data = self._spark.table(tables.table)
                        data_empty = table_data.rdd.isEmpty()
                    except:

                        try:
                            update_output = UpdateQueue({'update': update, 'session': session})
                            update_response = update_output.save_results({'update_output': empty_update_schema})
                            empty_df = self._spark.createDataFrame([empty_record], empty_schema)
                            empty_df.write.json(hdfs_output_dir, mode='ignore')
                            payload = json.loads(empty_df.toJSON().collect()[0], object_pairs_hook=OrderedDict)
                            output = SaveOutput({'save': save, 'session': session})
                            output_response = output.save_results({'payload': payload})
                            if update_response['status'] != 200:
                                summary_msg = '{output}\tCRITICAL FAIL\tCheck Update Endpoint'.format(
                                    output=json.dumps(failed_update_schema))
                                print(summary_msg)
                                sys.exit(0)
                            elif output_response['status'] != 200:
                                raise Exception
                        except Exception:
                            update_output = UpdateQueue({'update': update, 'session': session})
                            failed_update_response = update_output.save_results({'update_output': failed_update_schema})
                            exc_type, exc_value, exc_tb = sys.exc_info()
                            print(traceback.format_exception(exc_type, exc_value, exc_tb))
                            if failed_update_response['status'] != 200:
                                summary_msg = '{output}\tCRITICAL FAIL\tCheck Update Endpoint'.format(
                                    output=json.dumps(failed_update_schema))
                                print(summary_msg)
                                sys.exit(0)
                            else:
                                failed_log = {'content': json.dumps(failed_update_schema),'status': failed_update_response['status']}
                                r = requests.post(
                                    'https://chevelle-elk-logger.cp-epg2i.domain.com/logging/chevelle_dc/stdout',
                                    json=failed_log)
                                r.close()
                                continue

                        update_log = {'content': json.dumps(empty_update_schema), 'status':update_response['status']}
                        r = requests.post('https://chevelle-elk-logger.cp-epg2i.domain.com/logging/chevelle_dc/stdout',
                                          json=update_log)
                        r.close()
                        continue

                    else:

                        if not data_empty:

                            try:
                                table_data_column = table_data.select(col_name). \
                                        where(F.length(col_name) > 0). \
                                        where(lower(trim(table_data[col_name])).
                                              isin(' ', 'null', 'n/a', 'unknown', 'unk', 'unspecified', 'no match row id',
                                                   '__not_applicable__') == False)
                            except Exception:

                                try:
                                    update_output = UpdateQueue({'update': update, 'session': session})
                                    update_response = update_output.save_results({'update_output': empty_update_schema})
                                    empty_df = self._spark.createDataFrame([empty_record], empty_schema)
                                    empty_df.write.json(hdfs_output_dir, mode='ignore')
                                    payload = json.loads(empty_df.toJSON().collect()[0], object_pairs_hook=OrderedDict)
                                    output = SaveOutput({'save': save, 'session': session})
                                    output_response = output.save_results({'payload': payload})
                                    if output_response['status'] != 200:
                                        raise Exception
                                except Exception:
                                    update_output = UpdateQueue({'update': update, 'session': session})
                                    update_response = update_output.save_results({'update_output': failed_update_schema})
                                    exc_type, exc_value, exc_tb = sys.exc_info()
                                    print(traceback.format_exception(exc_type, exc_value, exc_tb))
                                    if update_response['status'] != 200:
                                        summary_msg = '{output}\tCRITICAL FAIL\tCheck Update Endpoint'.format(
                                            output=json.dumps(failed_update_schema))
                                        print(summary_msg)
                                        sys.exit(0)
                                    else:
                                        print(failed_update_schema, update_response['status'])
                                        continue

                                print(empty_update_schema, update_response['status'])
                                continue

                            else:

                                column_row_count_full = table_data_column.count()
                                output_strings = \
                                    {
                                        'COL_ID': col_id,
                                        'DATA_CLS_NM': data_cls_nm,
                                        'PROF_START_TS': 'START',
                                        'PROF_END_TS': 'END',
                                        'BATCH_ID': batch_id,
                                        'TABLE_ID': table_id,
                                        'TOTAL_ROW_COUNT': column_row_count_full,
                                        'SAMPLE_ROW_COUNT': column_row_count_full,
                                        'CRT_BY': 'chevelle',
                                        'CRT_TS': 'CRT',
                                        'REGEX_STR': regex_str,
                                        'column': col_name,
                                        'temp_table': 'temp_'
                                    }

                                if column_row_count_full >= 1000000:

                                    column_sample = table_data_column.sample(False, 0.1)
                                    column_sample.createOrReplaceTempView('temp_sample')
                                    column_sample_count = column_sample.count()

                                    output_strings['SAMPLE_ROW_COUNT'] = column_sample_count
                                    output_strings['temp_table'] = 'temp_sample'

                                    try:
                                        profile_start = datetime.datetime.now(timezone).strftime("%Y-%m-%d %H:%M:%S")

                                        output_strings['PROF_START_TS'] = profile_start
                                        results = self._spark.sql(base_query.format(**output_strings))

                                        profile_end = datetime.datetime.now(timezone).strftime("%Y-%m-%d %H:%M:%S")

                                        final_results = \
                                            results.\
                                                replace('END', profile_end, 'PROF_END_TS').\
                                                replace('CRT', profile_end, 'CRT_TS')
                                        update_output = UpdateQueue({'update': update, 'session': session})
                                        update_response = update_output.save_results({'update_output': success_update_schema})
                                        final_results.write.json(hdfs_output_dir, mode='ignore')
                                        payload = json.loads(final_results.toJSON().collect()[0], object_pairs_hook=OrderedDict)
                                        payload['appl_regex_str'] =  \
                                            payload['appl_regex_str'].\
                                                replace(chr(92), chr(169)).\
                                                replace(chr(123), chr(171)).\
                                                replace(chr(125), chr(187))

                                        output = SaveOutput({'save': save, 'session': session})
                                        output_response = output.save_results({'payload': payload})
                                        if output_response['status'] != 200:
                                            raise Exception
                                    except Exception:
                                        update_output = UpdateQueue({'update': update, 'session': session})
                                        update_response = update_output.save_results({'update_output': failed_update_schema})
                                        exc_type, exc_value, exc_tb = sys.exc_info()
                                        print(traceback.format_exception(exc_type, exc_value, exc_tb))
                                        if update_response['status'] != 200:
                                            summary_msg = '{output}\tCRITICAL FAIL\tCheck Update Endpoint'.format(
                                                output=json.dumps(failed_update_schema))
                                            print(summary_msg)
                                            sys.exit(0)
                                        else:
                                            print(failed_update_schema, update_response['status'])
                                            continue
                                    print(success_update_schema, update_response['status'])

                                elif 0 < column_row_count_full < 1000000:

                                    table_data_column.createOrReplaceTempView('temp_')

                                    try:
                                        profile_start = datetime.datetime.now(timezone).strftime("%Y-%m-%d %H:%M:%S")
                                        output_strings['PROF_START_TS'] = profile_start
                                        results = self._spark.sql(base_query.format(**output_strings))
                                        profile_end = datetime.datetime.now(timezone).strftime("%Y-%m-%d %H:%M:%S")
                                        final_results = \
                                            results.\
                                                replace('END', profile_end, 'PROF_END_TS').\
                                                replace('CRT', profile_end, 'CRT_TS')
                                        update_output = UpdateQueue({'update': update, 'session': session})
                                        update_response = update_output.save_results({'update_output': success_update_schema})
                                        final_results.write.json(hdfs_output_dir, mode='ignore')
                                        payload = json.loads(final_results.toJSON().collect()[0], object_pairs_hook=OrderedDict)
                                        payload['appl_regex_str'] = \
                                            payload['appl_regex_str']. \
                                                replace(chr(92), chr(169)). \
                                                replace(chr(123), chr(171)). \
                                                replace(chr(125), chr(187))
                                        output = SaveOutput({'save': save, 'session': session})
                                        output_response = output.save_results({'payload': payload})
                                        if output_response['status'] != 200:
                                            raise Exception
                                    except Exception:

                                        update_output = UpdateQueue({'update': update, 'session': session})
                                        update_response = update_output.save_results({'update_output': failed_update_schema})
                                        exc_type, exc_value, exc_tb = sys.exc_info()
                                        print(traceback.format_exception(exc_type, exc_value, exc_tb))
                                        if update_response['status'] != 200:
                                            summary_msg = '{output}\tCRITICAL FAIL\tCheck Update Endpoint'.format(
                                                output=json.dumps(failed_update_schema))
                                            print(summary_msg)
                                            sys.exit(0)
                                        else:
                                            print(failed_update_schema, update_response['status'])
                                            continue
                                    print(success_update_schema, update_response['status'])

                        else:

                            try:
                                update_output = UpdateQueue({'update': update, 'session': session})
                                update_response = update_output.save_results({'update_output': empty_update_schema})
                                empty_df = self._spark.createDataFrame([empty_record], empty_schema)
                                empty_df.write.json(hdfs_output_dir, mode='ignore')
                                payload = json.loads(empty_df.toJSON().collect()[0], object_pairs_hook=OrderedDict)
                                output = SaveOutput({'save': save, 'session': session})
                                output_response = output.save_results({'payload': payload})
                                if output_response['status'] != 200:
                                    raise Exception
                            except Exception:
                                update_output = UpdateQueue({'update': update, 'session': session})
                                update_response = update_output.save_results({'update_output': failed_update_schema})
                                exc_type, exc_value, exc_tb = sys.exc_info()
                                print(traceback.format_exception(exc_type, exc_value, exc_tb))
                                if update_response['status'] != 200:
                                    summary_msg = '{output}\tCRITICAL FAIL\tCheck Update Endpoint'.format(
                                        output=json.dumps(failed_update_schema))
                                    print(summary_msg)
                                    sys.exit(0)
                                else:
                                    print(failed_update_schema, update_response['status'])
                                    continue

                            print(empty_update_schema, update_response['status'])
                            continue

        except:
            sys.exit(0)

        application_end_time = time.time()
        print('-- Application Run time --')
        print(str(application_end_time - application_start))
        sys.exit(0)


