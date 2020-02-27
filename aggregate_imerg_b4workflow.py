# ---------------------------------------------------------------------------------------------
#
#  aggregate_data_orig.py
#
#  Description: as the file name suggests this script reads data from an subsetted IMERG Day granule, formatted in netCDF
#               and parses out the values based on geographic polygons (districts) and generates a JSON return
#               of mean and median values at a district level
#
#  Syntax: currently no input parameters
#
#  To Do: modify to accept input parameters, such as filename and maybe the location of district coordinates
#
# ---------------------------------------------------------------------------------------------


# --Do all the necessary imports
import statistics

import boto3 as boto3
from netCDF4 import Dataset as NetCDFFile
import json
from matplotlib.patches import Polygon
import matplotlib.path as mpltPath
import uuid
from urllib.parse import unquote_plus
import datetime
from datetime import date
from datetime import timedelta

s3 = boto3.resource(
    's3')

#polyfilename = 'organisationunit.MultiPolygon.geojsonlines'
polyfilename = "district_bnds_geojson.geojson"
polybucket = 'mosquito-dev'
precipVar = 'HQprecipitation'

data_bucket = "mosquito-data"
output_bucket = "mosquito-json"
data_type = "imerg"

def accumPrecipByDistrict(polylist, precip, lat, lon, districtPrecip):
    #    print('calc stats')
    #    districtPrecip={}
    for poly in polylist:
        if poly.get_label() not in districtPrecip.keys():
            districtPrecip[poly.get_label()] = []
        #        for ptLat,ptLon,val in lat,lon,precip:
        #        print("poly ", poly.get_label())
        for i in range(lon.shape[0]):
            #            print("i ",i)
            for j in range(lat.shape[0]):
                #                print("j ",j)
                #                print("lat ", lat[i], " lon ", lon[j], " poly ", poly.get_label())
                path = mpltPath.Path(poly.xy)
                inside = path.contains_point((lon[i], lat[j]))
                if inside:
                    # add precip value to district
                    if precip[i][j] >= 0.0:
                        districtPrecip[poly.get_label()].append(float(precip[i][j]))
                    else:
                        districtPrecip[poly.get_label()].append(0.0)


#                    print("lat ", lat[j], " lon ", lon[i], " precip ", precip[i][j], " inside ", poly.get_label())

def calcDistrictStats(districtPrecip, districtPrecipStats):
    for dist in districtPrecip.keys():
        if dist not in districtPrecipStats.keys():
            districtPrecipStats[dist] = {}
        if len(districtPrecip[dist]) > 0:
            #            print('len ',len(districtPrecip[dist]))
            #            print('points ',districtPrecip[dist])
            mean = statistics.mean(districtPrecip[dist])
            median = statistics.median(districtPrecip[dist])
        else:
            mean = 0.0
            median = 0.0
        #        meadian_high = statistics.median_high(districtPrecip[dist])
        #        meadian_low = statistics.median_low(districtPrecip[dist])
        #        std_dev = statistics.stdev(districtPrecip[dist])
        #        variance = statistics.variance(districtPrecip[dist])
        districtPrecipStats[dist] = dict([
            ('mean', mean),
            ('median', median)
        ])


def process_file(event_s3_bucket, event_key, output_s3_bucket):
    # open the JSON administrative districts files and load into a JSON object
    # polyfile=(
    #    r"C:\Users\tberendes\MOSQUITO\organisationunit.MultiPolygon.geojsonlines")
    #    polyfile=(
    #        r"/media/sf_MOSQUITO/organisationunit.MultiPolygon.geojsonlines")
    

#    sts_client = boto3.client('sts')

# use the assumed roll stuff for running the script locally, don't use it when
# running as a lambda on AWS
    # Call the assume_role method of the STSConnection object and pass the role
    # ARN and a role session name.
#    assumed_role_object = sts_client.assume_role(
#        RoleArn="arn:aws:iam::197443901397:role/services-developer",
#        RoleSessionName="AssumeRoleSession1"
#    )

    # From the response that contains the assumed role, get the temporary
    # credentials that can be used to make subsequent API calls
#    credentials = assumed_role_object['Credentials']

    # Use the temporary credentials that AssumeRole returns to make a
    # connection to Amazon S3
#    s3 = boto3.resource(
#        's3',
#        aws_access_key_id=credentials['AccessKeyId'],
#        aws_secret_access_key=credentials['SecretAccessKey'],
#        aws_session_token=credentials['SessionToken'],
#    )

    #    s3 = boto3.resource('s3')
    polyfile = '/tmp/'+polyfilename

    s3.Bucket(polybucket).download_file(polyfilename, polyfile)
# need to figure out new json structure
    with open(polyfile) as f:
        districts = json.load(f)
    f.close()
   # print(districts)

    # original format code
    # with open(polyfile) as f:
    #     content = f.readlines()
    # districts = []
    # for line in content:
    #     data = json.loads(line)
    #     districts.append(data)
#    print(districts[0])

#    print(districts[0]['properties'])
#    print(districts[0]['geometry'])

    numDists = len(districts)
#    print(numDists, ' districts')

    # nc = NetCDFFile('C:\\Users\\tberendes\\MOSQUITO\\Ken\\IMERG Daily Subsets\\3B-DAY.MS.MRG.3IMERG.20190228-S000000-E235959.V06.nc4.nc4')

    #    nc = NetCDFFile('/media/sf_MOSQUITO/Ken/IMERG Daily Subsets/3B-DAY.MS.MRG.3IMERG.20190228-S000000-E235959.V06.nc4.nc4')
#    s3.Bucket(data_bucket).download_file("imerg/3B-DAY.MS.MRG.3IMERG.20190228-S000000-E235959.V06.nc4.nc4",
 #                                            "/tmp/3B-DAY.MS.MRG.3IMERG.20190228-S000000-E235959.V06.nc4.nc4")
 #   nc = NetCDFFile('/tmp/3B-DAY.MS.MRG.3IMERG.20190228-S000000-E235959.V06.nc4.nc4')

    print("event key " + event_key)
    # strip off directory from key for temp file
    key_split = event_key.split('/')
    download_fn=key_split[len(key_split) - 1]
    s3.Bucket(data_bucket).download_file(event_key, "/tmp/" + download_fn)
    nc = NetCDFFile("/tmp/" +download_fn)

    # --Pull out the needed variables, lat/lon, time and precipitation.  These subsetted files only have precip param.
    lat = nc.variables['lat'][:]
    lon = nc.variables['lon'][:]
    dayssince1970 = nc.variables['time'][...]
    print("dayssince1970 ", dayssince1970[0])

    StartDate = "1/1/70"
    date_1 = datetime.datetime.strptime(StartDate, "%m/%d/%y")
    end_date = date_1 + datetime.timedelta(days=dayssince1970[0])

    print(end_date)
    print(end_date.strftime("%Y%m%d"))

    dateStr = end_date.strftime("%Y%m%d")

    # precipVar = 'precipitationCal'
    # precip = nc.variables['HQprecipitation'][:]
    precip = nc.variables[precipVar][:]

    # -- eliminate unnecessary time dimension from precip variable in IMERG
    # dims are lon,lat
    precip = precip.reshape(precip.shape[1], precip.shape[2])

    # globals for precip values and stats by district
    districtPrecip = {}
    districtPrecipStats = {}
    districtPolygons = {}

    #added for new json format
    districts = districts['features']

    for district in districts:
        shape = district['geometry']
        coords = district['geometry']['coordinates']
 #       name = district['properties']['name']
        name = district['Properties']

        def handle_subregion(subregion):
            poly = Polygon(subregion, edgecolor='k', linewidth=1., zorder=2, label=name)
            return poly

        distPoly = []
        if shape["type"] == "Polygon":
            for subregion in coords:
                distPoly.append(handle_subregion(subregion))
        elif shape["type"] == "MultiPolygon":
            for subregion in coords:
                #            print("subregion")
                for sub1 in subregion:
                    #                print("sub-subregion")
                    distPoly.append(handle_subregion(sub1))
        else:
            print
            "Skipping", name, \
            "because of unknown type", shape["type"]
        # compute statisics
        accumPrecipByDistrict(distPoly, precip, lat, lon, districtPrecip)
        districtPolygons[name] = distPoly

    calcDistrictStats(districtPrecip, districtPrecipStats)
    for district in districts:
       # name = district['properties']['name']
        name = district['Properties']
        print("district ", name)
        print("mean precip ", districtPrecipStats[name]['mean'])
        print("median precip ", districtPrecipStats[name]['median'])


    #    with open('test_dist_poly.json', 'w') as json_file:
    #      json.dump(districtPrecipStats, json_file)
    #    print(json.dumps(districtPrecipStats))
    print("aggregate_imerg done")
    nc.close()

    #    statType='mean'
    statType = 'median'
    # reformat new json structure
    outputJson = {'dataValues' : []}
    for key in districtPrecipStats.keys():
        value = districtPrecipStats[key][statType]
        jsonRecord = {'dataElement':'DATA_ELEMENT_VALUE','period':dateStr,'orgUnit':key,'value':value}
        outputJson['dataValues'].append(jsonRecord)


    with open("/tmp/" + download_fn+ ".json", 'w') as json_file:
        json.dump(outputJson, json_file)
#        json.dump(districtPrecipStats, json_file)
    json_file.close()

    s3.Bucket(output_bucket).upload_file("/tmp/" + download_fn+ ".json", "imerg/"+ download_fn+".json"
                                         ,ExtraArgs={"ACL": "public-read"})


#    return json.dumps(districtPrecipStats)

def main():
  #  polyfilename = 'organisationunit.MultiPolygon.geojsonlines'
  #  polybucket = 'mosquito-dev'
  #  precipVar = 'HQprecipitation'

 #   data_bucket = "mosquito-data"
  #  output_bucket = "mosquito-json"
    #keyvalue = "imerg/3B-DAY.MS.MRG.3IMERG.20190228-S000000-E235959.V06.nc4.nc4"
    keyvalue = "imerg/3B-DAY.MS.MRG.3IMERG.20150801-S000000-E235959.V06.nc4.nc"
   # data_type = "imerg"

    # set test variables
    process_file (data_bucket, keyvalue,
                  output_bucket)
    # need to dump districtPrecipStats into a file
    #dumpStats(districtPrecipStats)

def lambda_handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        process_file (bucket, key,
                  output_bucket)

# def s3_lambda_handler(event, context):
#     # use event context to set variables and loop through events
#     for record in event['Records']:
#         s3_filename
#         tmp_filename
#         out_filename
#         key = unquote_plus(record['s3']['object']['key'])
#         download_path = '/tmp/{}{}'.format(uuid.uuid4(), key)
#         upload_path = '/tmp/resized-{}'.format(key)
#         s3_client.download_file(bucket, key, download_path)
#         resize_image(download_path, upload_path)
#         s3_client.upload_file(upload_path, '{}resized'.format(bucket), key)
#
#
#     main()

if __name__ == '__main__':
    main()
