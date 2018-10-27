from  pyspark  import  SparkContext
sc  = SparkContext(appName = "q4")

def max_val(a,b):
	if a > b:
		return a
	else:
		return b

temperature_file = sc.textFile("/user/x_johth/data/temperature-readings.csv")
precipitation_file = sc.textFile("/user/x_johth/data/precipitation-readings.csv")

#102170;2013-11-29;18:00:00;-2.9;G
lines = temperature_file.map(lambda line: line.split(";"))
#102170 2013-11-29 18:00:00 -2.9 G
station_temperature = lines.map(lambda x: ((x[1], x[0]), float(x[3])))
#((u'2013-11-29', u'102170'), u'-2.9')
max_temperature = station_temperature.reduceByKey(max_val)
max_temperature = max_temperature.filter(lambda x: int(x[1])>=25 and int(x[1])<=30)

#102170;2013-11-29;18:00:00;-2.9;G
lines = precipitation_file.map(lambda line: line.split(";"))
#102170 2013-11-29 18:00:00 -2.9 G
station_precipitation = lines.map(lambda x: ((x[1], x[0]), float(x[3])))
station_precipitation = station_precipitation.reduceByKey(lambda a,b: a+b)
max_precipitation = station_precipitation.reduceByKey(max_val)
max_precipitation = max_precipitation.filter(lambda x: int(x[1])>= 100 and int(x[1])<= 200)

total = max_temperature.join(max_precipitation)

total.saveAsTextFile("lab1_q4")
