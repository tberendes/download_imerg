import sys
import json
from urllib.parse import unquote_plus

import urllib3
import certifi
import requests
from time import sleep
import boto3 as boto3

data_bucket = "mosquito-data"

auth = ('mosquito2019', 'Malafr#1')

s3 = boto3.resource(
    's3')

# Create a urllib PoolManager instance to make requests.
http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())
#http = urllib3.PoolManager()
# Set the URL for the GES DISC subset service endpoint
url = 'https://disc.gsfc.nasa.gov/service/subset/jsonwsp'

# This method POSTs formatted JSON WSP requests to the GES DISC endpoint URL
# It is created for convenience since this task will be repeated more than once
def get_http_data(request):
    hdrs = {'Content-Type': 'application/json',
            'Accept': 'application/json'}
    data = json.dumps(request)
    r = http.request('POST', url, body=data, headers=hdrs)
    response = json.loads(r.data)
    print('response ', response)
    # Check for errors
    if response['type'] == 'jsonwsp/fault':
        print('API Error: faulty %s request' % response['methodname'])
        sys.exit(1)
    return response

def download_imerg(subset_request):

    # Define the parameters for the data subset
    download_results = []
    # Submit the subset request to the GES DISC Server
    response = get_http_data(subset_request)
    # Report the JobID and initial status
    myJobId = response['result']['jobId']
    print('Job ID: ' + myJobId)
    print('Job status: ' + response['result']['Status'])

    # Construct JSON WSP request for API method: GetStatus
    status_request = {
        'methodname': 'GetStatus',
        'version': '1.0',
        'type': 'jsonwsp/request',
        'args': {'jobId': myJobId}
    }
    # Check on the job status after a brief nap
    while response['result']['Status'] in ['Accepted', 'Running']:
        sleep(2)
        response = get_http_data(status_request)
        status = response['result']['Status']
        percent = response['result']['PercentCompleted']
        print('Job status: %s (%d%c complete)' % (status, percent, '%'))
    if response['result']['Status'] == 'Succeeded':
        print('Job Finished:  %s' % response['result']['message'])
    else:
    #    print('Job Failed: %s' % response['fault']['code'])
        print('Job Failed: %s' % response['result']['message'])
        sys.exit(1)

    # Retrieve a plain-text list of results in a single shot using the saved JobID
    result = requests.get('https://disc.gsfc.nasa.gov/api/jobs/results/' + myJobId)
    try:
        result.raise_for_status()
        print(result.text)
    #    urls = result.text.split('\n')
        urls = result.text.splitlines()
        for i in urls: print('%s' % i)
    except:
        print('Request returned error code %d' % result.status_code)

    # Use the requests library to submit the HTTP_Services URLs and write out the results.
    print('\nHTTP_services output:')
    for item in urls:
        outfn = item.split('/')
        if len(outfn) <= 0:
            print('skipping unknown file '+outfn)
            continue
        outfn = outfn[len(outfn) - 1].split('?')[0]
        # skip pdf documentation files staged automatically by request
        if not outfn.endswith('.pdf'):
            download_results.append("imerg/"+outfn)
            print('outfile %s ' % outfn)
            URL = item
            print("item " + item)
            s=requests.Session()
            s.auth = auth
            r1 = s.request('get', URL)

            result = s.get(r1.url)
            try:
                result.raise_for_status()
                tmpfn = '/tmp/' + outfn
                f = open(tmpfn, 'wb')
                f.write(result.content)
                f.close()
                print(outfn)

                s3.Bucket(data_bucket).upload_file(tmpfn, "imerg/"+outfn)
            except:
                print('Error! Status code is %d for this URL:\n%s' % (result.status.code, URL))
                print('Help for downloading data is at https://disc.gsfc.nasa.gov/data-access')
        else:
            print('skipping documentation file '+outfn)
    return download_results

def load_json(bucket, key):

    print("event key " + key)
    # strip off directory from key for temp file
    key_split = key.split('/')
    download_fn=key_split[len(key_split) - 1]
    file = "/tmp/" + download_fn
    s3.Bucket(data_bucket).download_file(key, file)

    try:
        with open(file) as f:
            jsonData = json.load(f)
        f.close()
    except IOError:
        print("Could not read file:" + file)
        jsonData = {"message": "Error reading json file"}

    return jsonData

def lambda_handler(event, context):
    #    product = 'GPM_3IMERGDE_06'
    # product = 'GPM_3IMERGDF_06'
    # use "Late" product
    product = 'GPM_3IMERGDL_06'
    varName = 'HQprecipitation'
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])

        input_json = load_json(bucket, key)


        dataset = input_json["dataset"]
        org_unit = input_json["org_unit"]
        agg_period = input_json["agg_period"]
        request_id = input_json["request_id"]
        print("request_id ", request_id)

        start_date = input_json['start_date']
        end_date = input_json['end_date']
        #begTime = '2015-08-01T00:00:00.000Z'
        #endTime = '2015-08-01T23:59:59.999Z'

        minlon = input_json['min_lon']
        maxlon = input_json['max_lon']
        minlat = input_json['min_lat']
        maxlat = input_json['max_lat']

        data_element_id = input_json['data_element_id']

    #    varName = event['variable']
        # Construct JSON WSP request for API method: subset
        subset_request = {
            'methodname': 'subset',
            'type': 'jsonwsp/request',
            'version': '1.0',
            'args': {
                'role': 'subset',
                'start': start_date,
                'end': end_date,
                'box': [minlon, minlat, maxlon, maxlat],
                'extent': [minlon, minlat, maxlon, maxlat],
                'data': [{'datasetId': product,
                          'variable': varName
                          }]
            }
        }

        download_results=download_imerg(subset_request)

        # need error check on download_imerg

        # write out file list as json file into monitored s3 bucket to trigger aggregation
        # format new json structure
        aggregateJson = {"request_id": request_id, "data_element_id": data_element_id, "variable": varName,
                         "dataset": dataset, "org_unit": org_unit, "agg_period": agg_period,
                         "s3bucket": data_bucket, "files": download_results}

        aggregate_pathname = "requests/aggregate/precipitation/"

        with open("/tmp/" + request_id + "_aggregate.json", 'w') as aggregate_file:
            json.dump(aggregateJson, aggregate_file)
        #        json.dump(districtPrecipStats, json_file)
        aggregate_file.close()

        s3.Bucket(data_bucket).upload_file("/tmp/" + request_id + "_aggregate.json",
                                           aggregate_pathname + request_id + "_aggregate.json")
