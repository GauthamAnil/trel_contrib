# Trel community code base

Here you will find a number of jobs, sensors and their registrations that you can use in your pipeline / warehouse.

## Sensors

| sensor | group | description |
| ------ | ------ | ------------ |
| bigquery_table | bigquery | Can monitor a dataset in bigquery for new tables and add them to the catalog. |
| local_file | test | Can detect new files within the VM running Trel. Only useful for testing. |

## Jobs

| job | group | description |
| ------ | ------ | ------------ |
| append_day | bigquery, demo | A bigquery code part of the Trel demo |
| report_summary | bigquery, demo | A bigquery code part of the Trel demo |
| lifecycle/s3_python (python/pyspark) | lifecycle | This job will perform lifecycle on S3 type repositories |
| lifecycle/gcp (python/pyspark) | lifecycle | This job will perform lifecycle on Google Storage and BigQuery repositories |
