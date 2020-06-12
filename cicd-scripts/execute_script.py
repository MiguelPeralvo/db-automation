#!/usr/bin/python3
import json
import requests
import os
import sys
import argparse
import time

def main():
    shard = ''
    token = ''
    cluster = ''
    localpath = ''
    workspacepath = ''
    outfilepath = ''
    params=''
    parser = argparse.ArgumentParser(description="Execute python scripts in Databricks")
    parser.add_argument("-s", "--shard", help="Databricks workspace", required=True)
    parser.add_argument("-t", "--token", help="Databricks token", required=True)
    parser.add_argument("-c", "--cluster", help="Databricks cluster id", required=True)
    parser.add_argument("-l", "--localpath", help="Localpath", required=True)
    parser.add_argument("-w", "--workspacepath", help="DBFS workspace", required=True)
    parser.add_argument("-o", "--outfilepath", help="Logging output path", required=True)
    parser.add_argument("-p", "--params", help="Params", required=True)

    args = parser.parse_args()
    shard = args.shard
    token = args.token
    cluster = args.cluster
    localpath = args.localpath
    workspacepath = args.workspacepath
    outfilepath = args.outfilepath
    params = args.params

    print('-s is ' + shard)
    #print('-t is ' + token)
    print('-c is ' + cluster)
    print('-l is ' + localpath)
    print('-w is ' + workspacepath)
    print('-o is ' + outfilepath)
    print('-p is ' + params)
    # Generate array from walking local path

    scripts = []
    for path, subdirs, files in os.walk(localpath):
        for name in files:
            fullpath = path + '/' + name
            # removes localpath to repo but keeps workspace path
            fullworkspacepath = workspacepath + path.replace(localpath, '')

            name, file_extension = os.path.splitext(fullpath)
            if file_extension.lower() in ['.scala', '.sql', '.r', '.py']:
                row = [fullpath, fullworkspacepath, 1]
                scripts.append(row)
    print('Number of scripts to process: '+str(len(scripts)))
    print(f'scripts to process: {scripts}')

    # run each element in array
    for script in scripts:
        nameonly = os.path.basename(script[0])
        workspacepath = script[1]

        name, file_extension = os.path.splitext(nameonly)

        # workpath removes extension
        fullworkspacepath = workspacepath + '/' + name

        print('Running job for:' + fullworkspacepath)
        
        #Create json from inout parameter list
        paramList = params.split(',')
        jsonString = '{'
        for param in paramList:
            if jsonString != '{':
                jsonString=jsonString+','
            paramElement = param.split('=')
            jsonString = jsonString +'"' + paramElement[0]+'":"'+paramElement[1]+'"'
        jsonString = jsonString + '}'
        pyJsonString = json.loads(jsonString)

        values = {'name': name, 'existing_cluster_id': cluster, 'timeout_seconds': 3600, 'notebook_task': {'notebook_path': fullworkspacepath}}
        #values = {'run_name': name, 'existing_cluster_id': cluster, 'timeout_seconds': 3600, 'notebook_task': {'notebook_path': fullworkspacepath}}
        #Create DB Job
        print('Job Create Request URL: '+ shard + '/api/2.0/jobs/create')
        print('Job Create Request Data:' + json.dumps(values))
        resp = requests.post(shard + '/api/2.0/jobs/create',
                             data=json.dumps(values), auth=("token", token))
        createjson = resp.text
        print("createson response:" + createjson)
        d = json.loads(createjson)
        jobid = d['job_id']
        #Run Job
        print('Run Request URL: '+ shard + '/api/2.0/jobs/run-now')
        values={'job_id': jobid,'notebook_params':pyJsonString}
        print('Run Request Data:' + json.dumps(values))
        resp = requests.post(shard + '/api/2.0/jobs/run-now',
                             data=json.dumps(values), auth=("token", token))
        runjson = resp.text
        print("runjson response:" + runjson)
        d = json.loads(runjson)
        runid = d['run_id']
        i=0
        waiting = True
        while waiting:
            time.sleep(10)
            jobresp = requests.get(shard + '/api/2.0/jobs/runs/get?run_id='+str(runid), auth=("token", token))
            jobjson = jobresp.text
            print("jobjson:" + jobjson)
            j = json.loads(jobjson)            
            current_state = j['state']['life_cycle_state']
            runid = j['run_id']
            if current_state in ['INTERNAL_ERROR', 'SKIPPED']:
                sys.exit("script run did not complete. Status is "+current_state)
                break
            else: 
                if current_state in ['TERMINATED']:
                    result_state = j['state']['result_state']
                    if result_state in ['FAILED']:
                        sys.exit("script run did not complete. Status is "+result_state)
                    else:
                        break
            i=i+1

        jobresp = requests.get(shard + '/api/2.0/jobs/runs/get-output?run_id='+str(runid),auth=("token", token))
        jobjson = jobresp.text
        print("Final response:" + jobjson)
        j = json.loads(jobjson)  
        script_output= j["notebook_output"]
        response=script_output["result"]
        print ("Return value is:"+response)
        print('##vso[task.setvariable variable=response;]%s' % (response))
        if outfilepath != '':
            file = open(outfilepath + '/' +  str(runid) + '.json', 'w')
            file.write(json.dumps(j))
            file.close()

if __name__ == '__main__':
    main()