'''  Queries Finnhub API. Uploads stock ticks to BigQuery.

Credentials required: 
- finnhub: A JSON dict containing at least api_key

Packages required:
- finnhub
'''

import json, yaml, os, unittest, croniter, time, datetime, tempfile, sys
import finnhub
import treldev
from treldev import gcputils, S3Commands

def crawl(ticker, min_ts, max_ts, credentials, logger=None, debug=False):
    ''' Yields ticks '''
    finnhub_client = finnhub.Client(api_key=credentials['api_key'])
    if debug:
        logger.debug("Opened finnhub client")
    res = finnhub_client.stock_candles(ticker, 1, min_ts, max_ts)
    if debug:
        logger.debug("Got data")
    if res['s'] == 'no_data':
        if debug:
            logger.debug("Closing finnhub client (no_data)")
        finnhub_client.close()
        return []
    if res['s'] != 'ok':
        if debug:
            logger.debug("Closing finnhub client (not ok)")
        finnhub_client.close()
        raise Exception(f"Finnhub has error with message {res['s']}")
    keys = res.keys() - {'s'}
    for i in range(len(res['t'])):
        d = { k:res[k][i] for k in keys }
        d['ticker'] = ticker
        yield d
    if debug:
        logger.debug("Closing finnhub client")
    finnhub_client.close()

class Test(unittest.TestCase):

    def test_save_data(self):
        import logging
        with open("credentials.yml") as f:
            credentials = yaml.safe_load(f)
        with open("trel_sensor_finnhub.yml") as f:
            config = yaml.safe_load(f)
            
        root = logging.getLogger()
        #root.setLevel(logging.DEBUG)
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        root.addHandler(handler)
        
        now = datetime.datetime.now()
        sensor = FinnhubSensor(config, credentials, root, True)
        path = os.environ['BIGQUERY_TEMP_PATH']+"_finnhub_"+str(time.time()).replace(".","_")
        sensor.save_data_to_path(datetime.datetime(2021,8,26,20), path)
        bquri = gcputils.BigQueryURI(path)
        table = bquri.get_table()
        self.assertGreater(table.num_rows, 0)
        
class FinnhubSensor(treldev.Sensor):

    def __init__(self, config, credentials, *args, **kwargs):
        super().__init__(config, credentials, *args, **kwargs)
        
        self.instance_ts_precision = self.config['instance_ts_precision']
        self.cron_constraint = self.config['cron_constraint']
        self.tickers = self.config['tickers']
        self.lookback_seconds = self.config['max_instance_age_seconds'] - 1 # how far we should backfill missing datasets
        self.locking_seconds = self.config.get('locking_seconds',600)
        self.delay_seconds = 30
    
    def get_new_datasetspecs(self, datasets):
        ''' If there is data ready to be inserted, this should return a datasetspec. Else, return None '''
        return self.get_new_datasetspecs_with_cron_and_precision(datasets)

    def save_data_to_path(self, load_info, uri):
        ''' if the previous call to get_new_datasetspecs returned a (load_info, datasetspec) tuple, then this call should save the data to the provided path, given the corresponding (load_info, path). '''
        ts = load_info
        ts_next = croniter.croniter(self.cron_constraint, ts).get_next(datetime.datetime)
        if self.debug:
            self.logger.debug(f"ts {ts} ts_next {ts_next}")
            
        with tempfile.NamedTemporaryFile('w+t') as f:
            if self.debug:
                self.logger.debug(f"filename: {f.name}")
            min_ts = int(time.mktime(ts.timetuple()))
            max_ts = int(time.mktime((ts_next - datetime.timedelta(seconds=1)).timetuple()))
            for ticker in self.tickers:
                if self.debug:
                    self.logger.debug(f"Processing ticker {ticker}")
                for e in crawl(ticker, min_ts, max_ts, json.loads(self.credentials['finnhub']), self.logger, self.debug):
                    json.dump(e,f)
                    f.write('\n')
                if self.debug:
                    self.logger.debug(f"Done processing ticker {ticker}")
            f.flush()

            bquri = gcputils.BigQueryURI(uri) # wraps credential management and improves readability
            from google.cloud import bigquery
            schema = [
                bigquery.SchemaField("ticker","string", mode="REQUIRED"),
                bigquery.SchemaField("t","int64", mode="REQUIRED"),
                bigquery.SchemaField("v","int64", mode="NULLABLE"),
                bigquery.SchemaField("h","float64", mode="NULLABLE"),
                bigquery.SchemaField("l","float64", mode="NULLABLE"),
                bigquery.SchemaField("o","float64", mode="NULLABLE"),
                bigquery.SchemaField("c","float64", mode="NULLABLE"),
            ]
            bquri.load_file(f.name, {"source_format":bigquery.job.SourceFormat.NEWLINE_DELIMITED_JSON,
                                        "schema":schema})

if __name__ == '__main__':
    treldev.Sensor.init_and_run(FinnhubSensor)
    
