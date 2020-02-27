import sys
import json
import urllib3
import certifi
import requests
from time import sleep
import boto3 as boto3

data_bucket = "mosquito-data"
json_bucket = "mosquito-json"
polyfilename = "district_bnds_geojson.geojson"
polybucket = 'mosquito-dev'

auth = ('mosquito2019', 'Malafr#1')

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
    s3 = boto3.resource(
        's3')
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
            download_results.append(outfn)
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

def main():

#    product = 'GPM_3IMERGDE_06'
#    product = 'GPM_3IMERGDL_06'
    product = 'GPM_3IMERGDF_06'
    begTime = '2015-08-01T00:00:00.000Z'
    endTime = '2015-08-01T23:59:59.999Z'

    minlon = -13.6
    maxlon = -10.1
    minlat = 6.8
    maxlat = 10.1
    varNames = ['HQprecipitation']
    # The dimension slice will be for pressure levels between 1000 and 100 hPa
    # dimName = '/HDFEOS/SWATHS/Temperature/nLevels'
    # dimVals = [1,2,3,4,5,6,7,8,9,10,11,12,13]
    # dimSlice = []
    # for i in range(len(dimVals)) :
    #    dimSlice.append({'dimensionId': dimName, 'dimensionValue': dimVals[i]})
    # Construct JSON WSP request for API method: subset
    subset_request = {
        'methodname': 'subset',
        'type': 'jsonwsp/request',
        'version': '1.0',
        'args': {
            'role': 'subset',
            'start': begTime,
            'end': endTime,
            'box': [minlon, minlat, maxlon, maxlat],
            'extent': [minlon, minlat, maxlon, maxlat],
            'data': [{'datasetId': product,
                      'variable': varNames[0]
                      }]
        }
    }

    download_results = download_imerg(subset_request)

def lambda_handler(event, context):

    print("event ", event)

    if 'body' in event:
        event = json.loads(event['body'])

    product = event['product']
    start_date = event['start_date']
    end_date = event['end_date']
    #begTime = '2015-08-01T00:00:00.000Z'
    #endTime = '2015-08-01T23:59:59.999Z'

    minlon = event['min_lon']
    maxlon = event['max_lon']
    minlat = event['min_lat']
    maxlat = event['max_lat']
    varName = event['variable']
    # extract polygon json struct and upload to S3 bucket
    # set up for passing polygons as json input

   # with open("/tmp/" + download_fn+ ".json", 'w') as json_file:
    #    json.dump(outputJson, json_file)
    #    s3 = boto3.resource('s3')
    #polyfile = '/tmp/'+polyfilename
    #s3.Bucket(polybucket).download_file(polyfilename, polyfile)
    #s3.Bucket(output_bucket).upload_file("/tmp/" + download_fn+ ".json", "imerg/"+ download_fn+".json")


    # The dimension slice will be for pressure levels between 1000 and 100 hPa
    # dimName = '/HDFEOS/SWATHS/Temperature/nLevels'
    # dimVals = [1,2,3,4,5,6,7,8,9,10,11,12,13]
    # dimSlice = []
    # for i in range(len(dimVals)) :
    #    dimSlice.append({'dimensionId': dimName, 'dimensionValue': dimVals[i]})
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

    return dict(statusCode='200', headers={'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                body=json.dumps({'files': download_results}), isBase64Encoded='false')


#return dict(statusCode='200', body={'files': download_results}, isBase64Encoded='false')
#    return dict(body={'files': download_results}, isBase64Encoded='false')

    # return {
    #     'files': download_results
    # }

if __name__ == '__main__':
   main()
