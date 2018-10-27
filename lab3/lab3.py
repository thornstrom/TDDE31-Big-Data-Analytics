#from future import division
import datetime
from math import radians, cos, sin, asin, sqrt, exp 
from datetime import date
from pyspark import SparkContext
sc = SparkContext(appName="lab_kernel") 

from pyspark.sql import SQLContext, Row
sqlContext = SQLContext(sc)

def haversine(lon1, lat1, lon2, lat2):
	"""
	Calculate the great circle distance between two points 
	on the earth (specified in decimal degrees)
	"""
	# convert decimal degrees to radians
	lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2]) 
	# haversine formula
	dlon = lon2 -lon1 
	dlat = lat2 -lat1
	a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2 
	c = 2 * asin(sqrt(a))
	km = 6367 * c 
	return km
	
def date_difference(a,b):
	d1 = datetime.date(int(a.split("-")[0]), int(a.split("-")[1]), int(a.split("-")[2]))
	d2 = datetime.date(int(b.split("-")[0]), int(b.split("-")[1]), int(b.split("-")[2]))
	
	temp1 = d1 - d2
	temp2 = temp1.days
	temp3 = temp2%365
	temp4 = 365-temp3
	
	if(temp3 < temp4):
		return temp3
	else:
		return temp4
	
def time_difference(a,b):
	a_hour = a.split(":")[0]
	b_hour = b.split(":")[0]	
	
	return abs(int(a_hour)-int(b_hour))
	
#weights taken from my solution in TDDE01
h_distance = 75# Up to you 
h_date  =  15#  Up  to  you 
h_time = 4# Up to you
a = 58.4030713 # Up to you 
b = 15.5970199 # Up to you
my_date = "1962-07-14" # Up to you

#Gaussian kernel
def gaus(date, time, distance):
	res_date=exp(-(date/h_date)**2)
	res_time=exp(-(time/h_time)**2)
	res_dist=exp(-(distance/h_distance)**2)
	return(res_date+res_time+res_dist)
	#return(res_date*res_time*res_dist)

# Your code here
temp = sc.textFile("/user/x_johth/data/temperature-readings.csv")
#102170;2013-11-29;18:00:00;-2.9;G
parts_t = temp.map(lambda l: l.split(";"))
#102170 2013-11-29 18:00:00 -2.9 G
previous_temp = parts_t.filter(lambda x: x[1] < my_date)
date_dist = previous_temp.map(lambda x: (x[0], (date_difference(my_date, str(x[1])), x[2], x[3])))
#(u'102190', (49, u'06:00:00', u'-2.9'))

all_stations = sc.textFile("data/stations.csv")
#84260;orberga;2.0;58.4274;14.826;1990-11-01 00:00:00;1993-03-31 23:59:59;105.0
parts_s = all_stations.map(lambda l: l.split(";"))
#84260 orberga 2.0 58.4274 14.826 1990-11-01 00:00:00 1993-03-31 23:59:59 105.0
dist = parts_s.map(lambda x: (x[0], haversine(float(a),float(b),float(x[3]),float(x[4]))))
#(u'102170', 365.7076656680558)

one_rdd = date_dist.join(dist)
#(u'147460', ((171, u'06:00:00', u'-2.9'), 701.3632321376383))
all_distances = one_rdd.map(lambda x: (x[0], (x[1][0][0],x[1][0][1],x[1][1], float(x[1][0][2]))))
#(u'147460', (171, u'06:00:00', 701.3632321376383, u'-2.9'))

for i in range(4,25,2):
	weighted_sums = all_distances.map(lambda x: (x[1][3], gaus(x[1][0], int(i)-int(x[1][1].split(":")[0]) , x[1][2])))
	#(u'-6.2', 2.3678794411714423)
	weighted_sum = weighted_sums.map(lambda x: (x[0]*x[1], x[1]))
	total_sum = weighted_sum.reduce(lambda a,b: (a[0]+b[0], a[1]+b[1]))
	prediction = total_sum[0]/total_sum[1]
	print(str(i) + ":00:00 \t" + str(prediction))
	
