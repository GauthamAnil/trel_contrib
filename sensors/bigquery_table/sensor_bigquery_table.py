#!/usr/bin/env python3
'''
This sensor can monitor a given dataset in BigQuery to look for tables with an optional prefix and a date/time component. The time component is used to file the table appropriately.

E.g., table abc_20210201 will be cataloged as <dataset_class>,,20210201,<label>,<repository>

bq_table_prefix has to contain the full prefix. E.g. abc\_ for abc_20210201. Just abc or ab will not be enough.

TODO: Add option to query using table metadata and not API. This would scale better.

'''


import argparse, os, sys, datetime
import treldev.gcputils
from os import listdir
from os.path import isfile, join, isdir

class BigQueryTableSensor(treldev.Sensor):

    def __init__(self, config, credentials, *args, **kwargs):
        super().__init__(config, credentials, *args, **kwargs)
        
        self.bq_project = self.config['bq_project']
        self.bq_dataset = self.config['bq_dataset']
        self.bq_table_prefix = self.config.get('bq_table_prefix')
        self.instance_ts_precision = self.config['instance_ts_precision']
        self.instance_ts_format = self.config.get('instance_ts_format',"%Y%m%d")
        
        self.locking_seconds = self.config.get('locking_seconds',30)
        self.credentials = credentials
        self.known_contents = set([])
        self.bq_client = treldev.gcputils.BigQuery.get_client()

    def find_monitored_tables(self):
        tables = self.bq_client.list_tables(f"{self.bq_project}.{self.bq_dataset}")
        monitored = set([])
        for table in tables:
            if self.bq_table_prefix is None or table.table_id.startswith(self.bq_table_prefix):
                monitored.add(table.table_id)
        return monitored

    def get_new_datasetspecs(self, datasets):
        ''' If there is data ready to be inserted, this should return a datasetspec. Else, return None '''
        tables = self.find_monitored_tables()
        existing_tss = set([ ds['instance_ts'] for ds in datasets ])
        new_tables = tables.difference(self.known_contents)
        skipped_tables = []
        for table in new_tables:
            self.known_contents.add(table)
            try:
                ts_portion = table[(0 if self.bq_table_prefix is None else len(self.bq_table_prefix)):]
                instance_ts = datetime.datetime.strptime(ts_portion, self.instance_ts_format)
            except:
                print(f"Unable to parse ts from table {table}",file=sys.stderr)
                continue

            if self.max_instance_age_seconds is not None and \
               instance_ts < self.round_now - datetime.timedelta(seconds=self.max_instance_age_seconds):
                continue
                
            yield table, { 'instance_prefix':None,
                           'instance_ts':str(instance_ts),
                           'instance_ts_precision':self.instance_ts_precision,
                           'locking_seconds': self.locking_seconds,
                           'alt_uri':f'bq://{self.bq_project}/{self.bq_dataset}/{table}'
            }

    def save_data_to_path(self, load_info, uri):
        ''' Nothing to do, as this sensor only registers. '''
        pass
        
if __name__ == '__main__':
    treldev.Sensor.init_and_run(BigQueryTableSensor)
    
