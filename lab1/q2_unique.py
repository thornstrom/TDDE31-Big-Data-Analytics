from  pyspark  import  SparkContext
sc  = SparkContext(appName = "q2_unique")

temperature_file = sc.textFile("/user/x_johth/data/temperature-readings.csv")
#102170;2013-11-29;18:00:00;-2.9;G
lines = temperature_file.map(lambda line: line.split(";"))
#102170 2013-11-29 18:00:00 -2.9 G
year_temperature = lines.map(lambda x: (x[1][0:7], (x[0], float(x[3]))))
#(u'2013-11', (u'102170', u'-2.9'))
year_temperature = year_temperature.filter(lambda x: int(x[0][0:4])>=1950 and int(x[0][0:4])<=2014)
year_temperature = year_temperature.filter(lambda x: float(x[1][1]) >= 10)

month_id = year_temperature.map(lambda x: (x[0], x[1][0]))
#(u'2013-11', u'102170')
month_unique_id = month_id.distinct()
unique_each_month = month_unique_id.map(lambda x: x[0])
#(u'2013-11')
month_count = unique_each_month.map(lambda s: (s,1))
counts = month_count.reduceByKey(lambda a,b: a+b)

counts.saveAsTextFile("lab1_q2_unique")
