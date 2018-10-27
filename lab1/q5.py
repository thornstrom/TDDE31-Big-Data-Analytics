from  pyspark  import  SparkContext
sc  = SparkContext(appName = "lab1_q5")

ostergotland_file = sc.textFile("/user/x_johth/data/stations-Ostergotland.csv")
precipitation_file = sc.textFile("/user/x_johth/data/precipitation-readings.csv")


#stations_ostergotland
#84260;orberga;2.0;58.4274;14.826;1990-11-01 00:00:00;1993-03-31 23:59:59;105.0
stations = ostergotland_file.map(lambda line: line.split(";")[0]).collect()
#84260 

#precipitation
#102170;2013-11-29;18:00:00;0.0;G
p_lines = precipitation_file.map(lambda line: line.split(";"))
#102170 2013-11-29 18:00:00 0.0 G
valid_years = p_lines.filter(lambda x: int(x[1][0:4])>=1993 and int(x[1][0:4])<=2016)
valid_readings = valid_years.filter(lambda x: x[0] in stations)

formatted_readings = valid_readings.map(lambda x: ((x[1][0:7], x[0]), float(x[3])))

#((u'2013-11', u'102170'), 0.0)
monthly_prec_per_station = formatted_readings.reduceByKey(lambda a,b: a+b)

average_monthly_prec = monthly_prec_per_station.map(lambda x: (x[0][0],  (x[1], 1)))
#(u'2013-11', ('0.0', 1))
monthly_prec = average_monthly_prec.reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1]))
monthly_prec = monthly_prec.map(lambda x: (x[0][0:4], x[0][5:7], x[1][0]/x[1][1]))
#(u'1996', u'11', 67.11666666666665)


monthly_prec.saveAsTextFile("lab1_q5")


