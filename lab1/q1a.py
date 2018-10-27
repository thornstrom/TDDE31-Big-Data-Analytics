from  pyspark  import  SparkContext
sc  = SparkContext(appName = "BDA1:1")

def max_temp(a,b):
	if a[1] > b[1]:
		return a
	else:
		return b
		
		
def min_temp(a,b):
	if a[1] < b[1]:
		return a
	else:
		return b


temperature_file = sc.textFile("/user/x_johth/data/temperature-readings.csv")

lines = temperature_file.map(lambda line: line.split(";"))

year_temperature = lines.map(lambda x: (x[1][0:4], (float(x[3]), x[0])))

year_temperature = year_temperature.filter(lambda x: int(x[0])>=1950 and int(x[0])<=2014)


min_temperatures = year_temperature.reduceByKey(min)
min_temperaturesSorted = min_temperatures.sortBy(ascending = False, keyfunc=lambda k: k[1][0])
min_temperaturesSorted.saveAsTextFile("results/q1a_mintemp")

max_temperatures = year_temperature.reduceByKey(max)
max_temperaturesSorted = max_temperatures.sortBy(ascending = False, keyfunc=lambda k: k[1][0])
max_temperaturesSorted.saveAsTextFile("results/q1a_maxtemp")
