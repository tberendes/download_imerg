#---------------------------------------------------------------------------------------------
#
#  aggregate_imerg_orig.py
#
#  Description: as the file name suggests this script reads data from an subsetted IMERG Day granule, formatted in netCDF
#               and parses out the values based on geographic polygons (districts) and generates a JSON return
#               of mean and median values at a district level
#
#  Syntax: currently no input parameters
#
#  To Do: modify to accept input parameters, such as filename and maybe the location of district coordinates
#
#---------------------------------------------------------------------------------------------


#--Do all the necessary imports
import statistics
from netCDF4 import Dataset as NetCDFFile
import json
from matplotlib.patches import Polygon
import matplotlib.path as mpltPath

# open the JSON administrative districts files and load into a JSON object
#jsonfile=(
#    r"C:\Users\tberendes\MOSQUITO\organisationunit.MultiPolygon.geojsonlines")
jsonfile=(
    r"/media/sf_MOSQUITO/organisationunit.MultiPolygon.geojsonlines")
with open(jsonfile) as f:
    content = f.readlines()
districts=[]
for line in content:
    data = json.loads(line)
    districts.append(data)
print(districts[0])

print(districts[0]['properties'])
print(districts[0]['geometry'])

numDists=len(districts)
print(numDists,' districts')

#nc = NetCDFFile('C:\\Users\\tberendes\\MOSQUITO\\Ken\\IMERG Daily Subsets\\3B-DAY.MS.MRG.3IMERG.20190228-S000000-E235959.V06.nc4.nc4')
nc = NetCDFFile('/media/sf_MOSQUITO/Ken/IMERG Daily Subsets/3B-DAY.MS.MRG.3IMERG.20190228-S000000-E235959.V06.nc4.nc4')

#--Pull out the needed variables, lat/lon, time and precipitation.  These subsetted files only have precip param.
lat = nc.variables['lat'][:]
lon = nc.variables['lon'][:]
time = nc.variables['time'][:]
precipVar = 'HQprecipitation'
#precipVar = 'precipitationCal'
#precip = nc.variables['HQprecipitation'][:]
precip = nc.variables[precipVar][:]

#-- eliminate unnecessary time dimension from precip variable in IMERG
# dims are lon,lat
precip = precip.reshape(precip.shape[1], precip.shape[2])

# globals for precip values and stats by district
districtPrecip={}
districtPrecipStats = {}
districtPolygons = {}

def accumPrecipByDistrict(polylist):
#    print('calc stats')
#    districtPrecip={}
    for poly in polylist:
        if poly.get_label() not in districtPrecip.keys():
            districtPrecip[poly.get_label()]=[]
        #        for ptLat,ptLon,val in lat,lon,precip:
#        print("poly ", poly.get_label())
        for i in range(lon.shape[0]):
#            print("i ",i)
            for j in range(lat.shape[0]):
#                print("j ",j)
#                print("lat ", lat[i], " lon ", lon[j], " poly ", poly.get_label())
                path = mpltPath.Path(poly.xy)
                inside = path.contains_point((lon[i],lat[j]))
                if inside:
                    # add precip value to district
                    if precip[i][j] >= 0.0:
                        districtPrecip[poly.get_label()].append(float(precip[i][j]))
                    else:
                        districtPrecip[poly.get_label()].append(0.0)
#                    print("lat ", lat[j], " lon ", lon[i], " precip ", precip[i][j], " inside ", poly.get_label())

def calcDistrictStats():
    for dist in districtPrecip.keys():
        if dist not in districtPrecipStats.keys():
            districtPrecipStats[dist] = {}
        if len(districtPrecip[dist]) > 0:
#            print('len ',len(districtPrecip[dist]))
#            print('points ',districtPrecip[dist])
            mean=statistics.mean(districtPrecip[dist])
            median = statistics.median(districtPrecip[dist])
        else:
            mean=0.0
            median=0.0
#        meadian_high = statistics.median_high(districtPrecip[dist])
#        meadian_low = statistics.median_low(districtPrecip[dist])
#        std_dev = statistics.stdev(districtPrecip[dist])
#        variance = statistics.variance(districtPrecip[dist])
        districtPrecipStats[dist] = dict([
            ('mean', mean),
            ('median', median)
        ])
for district in districts:
    shape=district['geometry']
    coords=district['geometry']['coordinates']
    name=district['properties']['name']

    def handle_subregion(subregion):
        poly = Polygon(subregion, edgecolor='k',linewidth=1., zorder=2, label=name)
        return poly

    distPoly=[]
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
    accumPrecipByDistrict(distPoly)
    districtPolygons[name]=distPoly

calcDistrictStats()
for district in districts:
    name=district['properties']['name']
    print("district ", name)
    print("mean precip ", districtPrecipStats[name]['mean'])
    print("median precip ", districtPrecipStats[name]['median'])

#    statType='mean'
    statType='median'
    value = districtPrecipStats[name][statType]

with open('test_dist_poly.json', 'w') as json_file:
  json.dump(districtPrecipStats, json_file)

nc.close()
