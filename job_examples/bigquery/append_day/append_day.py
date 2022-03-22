import argparse, yaml
from treldev.gcputils import BigQueryURI

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--transactions")
    parser.add_argument("--transactions_30day", dest="t30")
    parser.add_argument("--output")
    parser.add_argument("--_schedule_instance_ts", dest="ts")
    args, _ = parser.parse_known_args()

    output_bq = BigQueryURI(args.output)
    transactions_bq = BigQueryURI(args.transactions)
    t30_bq = BigQueryURI(args.t30)

    output_bq.save_sql_results(f"""
with 

transactions as (SELECT * FROM `{transactions_bq.path}`)
,t30 as (SELECT * FROM `{t30_bq.path}`)
,max_ as (SELECT cast("{args.ts}" AS datetime) max_ts)

SELECT * FROM transactions 
UNION ALL 
SELECT t30.* FROM t30 
CROSS JOIN max_ 
WHERE ts >= DATETIME_SUB(max_ts, INTERVAL 29 DAY)
""")
