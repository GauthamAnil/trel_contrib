#!/usr/bin/env python3
'''
This sensor can monitor a given folder within the trel instance for new files and upload them to S3 as well as register them in the catalog.

Each file noticed, such as a.txt will be registered as: <dataset_class>,a.txt,<optional instance_ts_str>,<label>,<repository>

This sensor has limited practical value in production, but is good for learning about sensors.
'''


import argparse, os, sys
import treldev
from os import listdir
from os.path import isfile, join, isdir

class LocalFileSensor(treldev.Sensor):

    def __init__(self, config, credentials, *args, **kwargs):
        super().__init__(config, credentials, *args, **kwargs)
        
        self.folder_to_monitor = os.path.expanduser(self.config['folder_to_monitor'])
        self.insert_instance_ts = self.config['insert_instance_ts']
        self.instance_ts_precision = self.config['instance_ts_precision']
        self.locking_seconds = self.config.get('locking_seconds',86400)
        self.credentials = credentials
        self.known_contents = set([])
        self.s3_commands = treldev.S3Commands(credentials=self.credentials)
    
    def find_monitored_files(self):
        only_files = [f for f in listdir(self.folder_to_monitor) if isfile(os.path.join(self.folder_to_monitor, f))]
        return only_files

    def get_new_datasetspecs(self, datasets):
        ''' If there is data ready to be inserted, this should return a datasetspec. Else, return None '''
        only_files = self.find_monitored_files()
        matching_prefixes = set([ ds['instance_prefix'] for ds in datasets ])
        for filename in only_files:
            if filename in self.known_contents:
                continue
            if filename in matching_prefixes:
                self.known_contents.add(filename)
                self.logger.debug(f"file {filename} has a matching dataset")
                continue
            instance_ts = datetime.datetime.now() if self.insert_instance_ts else None
            yield filename, { 'instance_prefix':filename,
                              'instance_ts':instance_ts,
                              'instance_ts_precision':self.instance_ts_precision,
                              'locking_seconds': self.locking_seconds }

    def save_data_to_path(self, load_info, uri):
        ''' if the previous call to get_new_datasetspecs returned a (load_info, datasetspec) tuple, then this call should save the data to the provided path, given the corresponding (load_info, path). '''
        filename = load_info
        print(f"Uploading {filename} to {uri}",file=sys.stderr)
        self.s3_commands.upload_file(self.folder_to_monitor+'/'+filename, uri+filename)
        sys.stderr.flush()
        
if __name__ == '__main__':
    treldev.Sensor.init_and_run(LocalFileSensor)
    
