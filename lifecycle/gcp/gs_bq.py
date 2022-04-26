'''
This file is able to handle Google Storage and BigQuery lifecycle actions. Use the registration file provided.

Testing steps:

1. Provide a credentials.yml file with the appropriate credentials for google.
2. Execute the following commands::

  python3 -c "from  gs_bq import test_gs as t; t('gs://trel/unittest/lifecycle_tmp/', num_paths=2)"
  python3 -c "from  gs_bq import test_bq as t; t('gs://trel/unittest/lifecycle_tmp/','bq://trel-main/tmp/lifecycle_unittest', num_tables=20)"

'''

import json, time, sys, yaml, boto3, tempfile, os, datetime, subprocess
import multiprocessing.pool
import treldev.gcputils

def parse_args():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--lifecycle.actions", dest='input_path')
    parser.add_argument("--lifecycle.actions.complete", dest='output_path')
    parser.add_argument("--threads", type=int, default=100)
    parser.add_argument("--_args")
    args, _ = parser.parse_known_args()
    return args

def do_action(action):
    # fill in before_state, after_state, action_completed_ts (if SUCCESS) , error_message (if FAILED)
    if action['action_requested'] == 'delete':
        protocol,_ = action['uri'].split(':',1)
        if protocol == 'bq':
            bquri = treldev.gcputils.BigQueryURI(action['uri'])
            try:
                bqclient.delete_table(bquri.path)
                print(f"Deleting {bquri.uri}")
                empty = False
            except:
                empty = True
        elif protocol == 'gs':
            _, _, bucket, prefix = action['uri'].split('/',3)
            bucket_obj = None
            try:
                bucket_obj = sclient.bucket(bucket)
            except:
                print(f"Unable to find google storage bucket {bucket}", out=sys.stderr)
            if bucket_obj:
                empty = True
                for blob in bucket_obj.list_blobs(prefix=prefix):
                    blob.delete()
                    print(f"Deleting gs://{bucket}/{blob.name}")
                    empty=False
        action['action_completed_ts'] = str(datetime.datetime.utcnow())
        action['before_state'] = [ ('empty' if empty else 'not_empty') ]
        action['after_state'] = ['empty']
    else:
        action['before_state'] = []
        action['after_state'] = []
        action['error_message'] = s3_action['action_requested'] + " is not a valid action"
    return action
        
def main(_args, input_path, output_path, threads):
    global bqclient, sclient
    bqclient = treldev.gcputils.BigQuery.get_client()
    sclient = treldev.gcputils.Storage.get_client()
    
    start_time = time.time()
    pool = multiprocessing.pool.Pool(processes=threads)
    fd, filename = tempfile.mkstemp()
    output_folder = tempfile.mkdtemp()


    input_protocol,_,bucket, prefix = input_path.split('/',3)
    output_protocol,_,output_bucket, output_prefix = output_path.split('/',3)
    assert input_protocol == 'gs:'
    assert output_protocol == 'gs:'
    res = []
    i = 0
    output_bucket_obj = sclient.bucket(output_bucket)
    for blob in sclient.list_blobs(bucket,prefix=prefix):
        print("Processing action file w prefix "+ blob.name)
        with open(filename,'wb') as f:
            sclient.download_blob_to_file(blob,f)
        subset = []
        with open(filename) as f:
            for line in f:
                subset.append( json.loads(line) )
                if len(subset) == 10000:
                    print("Asking pool to process a subset of length {}".format(len(subset)))
                    res += pool.map(do_action, subset)
                    subset.clear()
        print("Asking pool to process a subset of length {}".format(len(subset)))
        res += pool.map(do_action, subset)
        output_filename = 'part-{:05d}'.format(i)
        full_output_filename = os.path.join(output_folder, output_filename)
        with open(full_output_filename,'w') as f:
            for v in res:
                f.write(json.dumps(v))
                f.write('\n')
        output_blob = output_bucket_obj.blob(output_prefix+output_filename)
        output_blob.upload_from_filename(full_output_filename)
        os.system("rm {0}".format(full_output_filename))
        i += 1
    os.close(fd)
    print("Execution took {} seconds".format(time.time() - start_time))

def test_gs(temp_gs_path, num_paths=20, num_files=10):
    assert 'tmp/' in temp_gs_path
    subprocess.call('gsutil -m rm -r gs://trel/unittest/lifecycle_tmp/',shell=True)
    temp_folder = tempfile.mkdtemp()
    # make some files
    subprocess.check_call("""for fname in `echo | py -l '[ ("part_"+str(i)) for i in range({num_files})]'`; do touch {temp_folder}/$fname; done""".format(**locals()), shell=True)
    # upload them in 10 places
    res = []
    for i in range(num_paths):
        prefix = "{temp_gs_path}data/_{i}".format(**locals())
        subprocess.check_call("gsutil -m cp -r {temp_folder} {prefix}".format(**locals()), shell=True)
        res.append({'uri':prefix,'action_requested':'delete'})
        
    temp_folder2 = tempfile.mkdtemp()
    #put the places in a file
    with open(os.path.join(temp_folder2,'part-00000'),'w') as f:
        for d in res:
            json.dump(d,f)
            f.write('\n')
    # upload the file
    subprocess.check_call("gsutil -m cp -r {temp_folder2} {temp_gs_path}input".format(**locals()), shell=True)
    # call the script
    cmd = ("python3 gs_bq.py --lifecycle.actions {temp_gs_path}input/ --lifecycle.actions.complete {temp_gs_path}output/ --_args 1".format(**locals()))
    print(cmd)
    subprocess.check_call(cmd, shell=True)
    assert 1 == subprocess.call(f"gsutil ls {temp_gs_path}data/ 2> devnull",shell=True)

    
        
def test_bq(temp_gs_path, temp_bq_path, num_tables=5):
    assert 'tmp' in temp_bq_path
    assert 'tmp/' in temp_gs_path
    subprocess.call('gsutil -m rm -r gs://trel/unittest/lifecycle_tmp/',shell=True)

    res = []
    tmp = treldev.gcputils.BigQueryURI(temp_bq_path)
    for i in range(num_tables):
        subprocess.check_call(f"bq mk -f --schema a:string --project_id {tmp.project} -t {tmp.dataset}.{tmp.table}_data{i}", shell=True)
        res.append({'uri':f"bq://{tmp.project}/{tmp.dataset}/{tmp.table}_data{i}",'action_requested':'delete'})
        
    temp_folder2 = tempfile.mkdtemp()
    #put the places in a file
    with open(os.path.join(temp_folder2,'part-00000'),'w') as f:
        for d in res:
            json.dump(d,f)
            f.write('\n')
            
    # upload the file
    subprocess.check_call("gsutil -m cp -r {temp_folder2} {temp_gs_path}input".format(**locals()), shell=True)
    # call the script
    cmd = ("python3 gs_bq.py --lifecycle.actions {temp_gs_path}input/ --lifecycle.actions.complete {temp_gs_path}output/ --_args 1".format(**locals()))
    print(cmd)
    subprocess.check_call(cmd, shell=True)
        
if __name__ == '__main__':
    args = parse_args()
    main(**args.__dict__)

'''
'''
