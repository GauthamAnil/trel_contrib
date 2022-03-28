import argparse, yaml
from treldev.gcputils import BigQueryURI

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--transactions_30day", dest="t30")
    parser.add_argument("--report_summary", dest="output")
    parser.add_argument("--_schedule_instance_ts", dest="ts")
    args, _ = parser.parse_known_args()

    output_bq = BigQueryURI(args.output)
    t30_bq = BigQueryURI(args.t30)

    output_bq.save_sql_results(f"""
with 
t30 as (SELECT * FROM `{t30_bq.path}`)

select 
    count(distinct ts) num_days,
    count(distinct user_id) users,
    count(distinct product_id) products,
    avg(amount) average_amount
from t30""")
