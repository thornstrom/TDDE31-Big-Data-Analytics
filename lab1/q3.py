from  pyspark  import  SparkContext
sc  = SparkContext(appName = "q3")

temperature_file = sc.textFile("/user/x_johth/data/temperature-readings.csv")
#102170;2013-11-29;18:00:00;-2.9;G
lines = temperature_file.map(lambda line: line.split(";"))
#102170 2013-11-29 18:00:00 -2.9 G
valid_years = lines.filter(lambda x: int(x[1][0:4])>=1960 and int(x[1][0:4])<=2014)
year_temperature = valid_years.map(lambda x: ((x[1][0:7], x[0]), (float(x[3]), 1)))
#((u'2013-11', u'102170'), (u'-2.9', u'1'))
#year_temperature = year_temperature.filter(lambda x: int(x[0][0][0:4])>=1950 and int(x[0][0][0:4])<=2014)

average_temperature = year_temperature.reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1]))
average_temperature = average_temperature.map(lambda x: (x[0][0][0:4], x[0][0][4:7], x[0][1], (x[1][0]/x[1][1])))

average_temperature.saveAsTextFile("lab1_q3")
