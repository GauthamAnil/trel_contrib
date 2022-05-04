#!/usr/bin/env python3

'''
This sensor loads from ODBC data sources. See the config.yml file below for possible options.

.. literalinclude:: ../../GIT/trel_contrib/sensors/odbc_table_load/config.yml
   :language: yaml


The behavior is as follows:

1. The sensor queries the relevant datasets are already exist in the catalog. It uses the following parameters for this:

 * dataset_class
 * instance_ts_precision
 * label
 * repository
 * instance_prefix
 * max_instance_age_seconds
    
2. The sensor looks at the clock and identifies which datasets are relevant to load. The parameters of relevance are,

 * cron_constraint
 * delay_seconds
 * max_instance_age_seconds

3. Once a dataset that should be created is identified, a connection is established to the ODBC data source

 * driver
 * server
 * port
 * username
 * password
 * credentials.requested_name
 * credentials.source_key -> odbc.<server_name>

4. Then the relevant data is downloaded in pieces and uploaded to the destination

 * database
 * table
 * custom_sql
 * repository
 * credentials.requested_name

.. admonition:: Warning
    :class: warning

    When using ``custom_sql`` is used, make sure that for tabular destinations such as BigQuery, the result has the same schema as the table. 

    This restriction does not apply to object store destinations such as S3 and Google Storage.

Please take a look at :ref:`odbc_data_sources` for a list of pre-installed ODBC drivers in the Trel instance for the ``driver`` parameter.

Supported repository classes as destination:

1. ``s3``
2. ``bigquery``


'''


import argparse, os, sys
import treldev, pyodbc, tempfile, json, datetime, subprocess
from os import listdir
from os.path import isfile, join, isdir

class ODBCSensor(treldev.Sensor):


    
    def __init__(self, config, credentials, *args, **kwargs):
        super().__init__(config, credentials, *args, **kwargs)
        
        self.instance_ts_precision = self.config['instance_ts_precision']
        self.credentials = credentials
        
        if 'credentials.source_key' in self.config:
            odbc_creds = json.loads(self.credentials[self.config['credentials.source_key']])
            for k in ['driver','server', 'port','username','password']:
                setattr(self, k, odbc_creds[k])
        else:
            self.driver = self.config['driver']
            self.server = self.config['server']
            self.port = self.config['port']
            self.username = self.config['username']
            self.password = self.config['password']
            
        self.database = self.config['database']
        self.table = self.config['table']
        self.custom_sql = self.config.get('custom_sql')
        self.batch_rows = self.config.get('batch_rows',100000)
        self.output_format = self.config.get('output_format','json')
        self.compression = self.config.get('compression','gz')
        self.known_contents = set([])
        self.cron_constraint = self.config['cron_constraint']
        self.lookback_seconds = self.config['max_instance_age_seconds'] - 1 # how far we should backfill missing datasets
        self.locking_seconds = self.config.get('locking_seconds',600)
    
    def get_new_datasetspecs(self, datasets, **kwargs):
        # res = list( self.get_new_datasetspecs_with_cron_and_precision(datasets) )
        # self.logger.debug(res)
        # return res
        return self.get_new_datasetspecs_with_cron_and_precision(datasets)
    
    def save_data_to_path(self, load_info, uri, dataset=None, **kwargs):
        ''' if the previous call to get_new_datasetspecs returned a (load_info, datasetspec) tuple, then this call should save the data to the provided path, given the corresponding (load_info, path). '''
        minute = load_info

        cnxn = pyodbc.connect('DRIVER={'+self.driver+'};SERVER='+self.server+';DATABASE='+self.database+';UID='+self.username+';PWD='+ self.password)
        cursor = cnxn.cursor()

        self.columns = list(cursor.columns(table=self.table))
        
        print(f"Table {uri} columns: ",file=sys.stderr)
        for col in self.columns:
            print(f"  {col}", file=sys.stderr)
        
        destination = DestinationProtocol.get_object_from_uri(uri, self)
        destination.prepare()
            
        fetch_rows = self.batch_rows // 20
        sql = ("select * from `{table}`"
               if self.custom_sql is None
               else self.custom_sql).format(
                       table=self.table,
                       instance_ts=minute,
                       instance_ts_precision=self.instance_ts_precision)
        print(f"Executing:\n{sql}", file=sys.stderr)
        cursor.execute(sql)
        done = False
        while not done:
            with tempfile.NamedTemporaryFile('w+', delete=False) as f:
                for batch_i in range(20):
                    done = True
                    for row in cursor.fetchmany(fetch_rows):
                        done = False
                        destination.write_row_to_file(row, f)
                    if done:
                        break
            print(f"Uploading batch {destination.get_next_batch_num()} data from {f.name} to {uri}",file=sys.stderr)
            destination.append_data(f.name)
            sys.stderr.flush()
        destination.finish()

class DestinationProtocol(object):

    registered = {}
    
    @classmethod
    def register(cls):
        cls.registered[cls.protocol] = cls
    
    @classmethod
    def get_class_from_protocol(cls, protocol):
        try:
            return cls.registered[protocol]
        except KeyError:
            logger.debug(f"{protocol} is not a registered destination protocol. Only found {cls.registered}")
            raise
        
    @classmethod
    def get_object_from_uri(cls, uri, sensor):
        try:
            uri_protocol,_ = uri.split(':',1)
            return cls.registered[uri_protocol](uri, sensor)
        except KeyError as ex:
            msg = (f"{uri_protocol} is not a registered destination protocol. Only found {sorted(cls.registered)}")
            sensor.logger.error(msg)
            raise Exception(msg) from ex
        
    def __init__(self, uri, sensor):
        self.uri = uri
        self.sensor = sensor
    
    def prepare(self):
        self.batch_num = 0
        self.col_names = []
        for col in self.sensor.columns:
            self.col_names.append( col.column_name )
        self.prepare_inner()

    def append_data(self, file_name):
        self.append_data_inner(file_name)
        self.batch_num += 1

    def finish(self):
        self.finish_inner()

    def write_row_to_file(self, row, f):
        for i in range(len(row)):
            if type(row[i]) is datetime.datetime:
                row[i] = str(row[i])
        json.dump(dict(filter((lambda x: x[1] is not None), zip(self.col_names, row))), f)
        f.write('\n')
    
    def get_next_batch_num(self):
        return self.batch_num

class S3Destination(DestinationProtocol):

    protocol = 's3'

    def prepare_inner(self):
        self.s3_commands = treldev.S3Commands(credentials=self.sensor.credentials)

    
    def append_data_inner(self, filename):
        if self.sensor.compression == 'gz':
            subprocess.check_call(f"gzip {filename}", shell=True)
            filename = filename + '.gz'
            file_uri = self.uri + f"part-{self.batch_num:>05}.gz"
        else:
            file_uri = self.uri + f"part-{self.batch_num:>05}"
        self.s3_commands.upload_file(filename, file_uri)
        os.remove(filename)

    def finish_inner(self):
        with tempfile.NamedTemporaryFile('w') as f:
            self.s3_commands.upload_file(f.name, self.uri+'_SUCCESS')
            f.close()
S3Destination.register()

            
class BigQueryDestination(DestinationProtocol):

    protocol = 'bq'
    type_mapping = {
        pyodbc.SQL_CHAR: 'STRING',
        pyodbc.SQL_VARCHAR: 'STRING',
        pyodbc.SQL_LONGVARCHAR: 'STRING',
        pyodbc.SQL_WCHAR: 'STRING',
        pyodbc.SQL_WVARCHAR: 'STRING',
        pyodbc.SQL_WLONGVARCHAR: 'STRING',
        pyodbc.SQL_GUID: 'STRING',
        pyodbc.SQL_TYPE_DATE: 'DATE',
        pyodbc.SQL_TYPE_TIME: 'TIME',
        pyodbc.SQL_TYPE_TIMESTAMP: 'DATETIME',
        #pyodbc.SQL_TYPE_UTCDATETIME: 'DATETIME',
        #pyodbc.SQL_TYPE_UTCTIME: 'DATETIME',
        pyodbc.SQL_BINARY: 'BYTES',
        pyodbc.SQL_VARBINARY: 'BYTES',
        pyodbc.SQL_DECIMAL: 'DECIMAL',
        pyodbc.SQL_NUMERIC: 'DECIMAL',
        pyodbc.SQL_SMALLINT: 'INTEGER',
        pyodbc.SQL_INTEGER: 'INTEGER',
        pyodbc.SQL_BIT: 'INTEGER', # test
        pyodbc.SQL_TINYINT: 'INTEGER',
        pyodbc.SQL_BIGINT: 'INTEGER',
        pyodbc.SQL_REAL: 'FLOAT64',
        pyodbc.SQL_FLOAT: 'FLOAT64',
        pyodbc.SQL_DOUBLE: 'FLOAT64',
        pyodbc.SQL_INTERVAL_MONTH: 'INTERVAL',
        pyodbc.SQL_INTERVAL_YEAR: 'INTERVAL',
        pyodbc.SQL_INTERVAL_YEAR_TO_MONTH: 'INTERVAL',
        pyodbc.SQL_INTERVAL_DAY: 'INTERVAL',
        pyodbc.SQL_INTERVAL_HOUR: 'INTERVAL',
        pyodbc.SQL_INTERVAL_MINUTE: 'INTERVAL',
        pyodbc.SQL_INTERVAL_SECOND: 'INTERVAL',
        pyodbc.SQL_INTERVAL_DAY_TO_HOUR: 'INTERVAL',
        pyodbc.SQL_INTERVAL_DAY_TO_MINUTE: 'INTERVAL',
        pyodbc.SQL_INTERVAL_DAY_TO_SECOND: 'INTERVAL',
        pyodbc.SQL_INTERVAL_HOUR_TO_MINUTE: 'INTERVAL',
        pyodbc.SQL_INTERVAL_HOUR_TO_SECOND: 'INTERVAL',
        pyodbc.SQL_INTERVAL_MINUTE_TO_SECOND: 'INTERVAL',
    }
    
    def prepare_inner(self):
        global bigquery, BigQuery, BigQueryURI, base64

        assert self.sensor.output_format == 'json'
        
        from treldev.gcputils import BigQuery, BigQueryURI
        import treldev.gcputils
        from google.cloud import bigquery
        import base64

        self.client = treldev.gcputils.BigQuery.get_client()
        for col in self.sensor.columns:
            bq_type = self.type_mapping[col.data_type]
            print(f"{col.column_name}:{bq_type},", file=sys.stderr,end='')
        print("", file=sys.stderr)

        self.schema = []
        self.bquri = BigQueryURI(self.uri)
        for col in self.sensor.columns:
            bq_type = self.type_mapping[col.data_type]
            self.schema.append( bigquery.SchemaField(col.column_name, bq_type, mode=("NULLABLE" if col.nullable else "REQUIRED")) )
        self.client.delete_table(self.bquri.path, not_found_ok=True)
        table = bigquery.Table(self.bquri.path, schema=self.schema)
        
        table = self.client.create_table(table)
        print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id), file=sys.stderr)
        
    def write_row_to_file(self, row, f):
        for i in range(len(row)):
            bq_type = self.type_mapping[self.sensor.columns[i].data_type]
            if bq_type in ('DATE','TIME','DATETIME','INTERVAL'):
                if row[i] is not None:
                    row[i] = str(row[i])
            elif bq_type == 'BYTES':
                if row[i] is not None:
                    row[i] = base64.b64encode(row[i]).decode('utf-8')
        json.dump(dict(filter((lambda x: x[1] is not None), zip(self.col_names, row))), f)
        f.write('\n')
    
    def append_data_inner(self, filename):
        loadjob_config_dict = {
            'write_disposition': bigquery.WriteDisposition.WRITE_APPEND,
            'source_format': bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            }
        self.bquri.load_file(filename, loadjob_config_dict)
        os.remove(filename)
        
    def finish_inner(self):
        pass
BigQueryDestination.register()
        
if __name__ == '__main__':
    treldev.Sensor.init_and_run(ODBCSensor)
    
