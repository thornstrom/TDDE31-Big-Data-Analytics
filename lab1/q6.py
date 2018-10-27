from  pyspark  import  SparkContext
sc  = SparkContext(appName = "lab1_q6")

ostergotland_file = sc.textFile("/user/x_johth/data/stations-Ostergotland.csv")
temperature_file = sc.textFile("/user/x_johth/data/temperature-readings.csv")

#84260;orberga;2.0;58.4274;14.826;1990-11-01 00:00:00;1993-03-31 23:59:59;105.0
stations = ostergotland_file.map(lambda line: line.split(";")[0]).collect()
#84260 


#total average monthly temperature from 1950-1980
#102170;2013-11-29;18:00:00;0.0;G
t_lines = temperature_file.map(lambda line: line.split(";"))
#102170 2013-11-29 18:00:00 0.0 G
valid_years = t_lines.filter(lambda x: int(x[1][0:4])>=1950 and int(x[1][0:4])<=2014)
valid_readings = valid_years.filter(lambda x: x[0] in stations)

long_term_average = valid_readings.filter(lambda x: int(x[1][0:4])<=1980)
long_term_average = long_term_average.map(lambda x: ((x[0], x[1][0:7]), (float(x[3]), 1)))
long_term_average = long_term_average.reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1]))
long_term_average = long_term_average.map(lambda x: (x[0][1][5:7], (x[1][0]/x[1][1], 1)))
long_term_average = long_term_average.reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1]))
long_term_average = long_term_average.map(lambda x: (x[0], x[1][0]/x[1][1]))


avg_temp = valid_readings.map(lambda x: ((x[0], x[1][0:7]), (float(x[3]), 1)))
avg_temp = avg_temp.reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1]))
avg_temp = avg_temp.map(lambda x: (x[0][1], (x[1][0]/x[1][1], 1)))
avg_temp = avg_temp.reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1]))
avg_temp = avg_temp.map(lambda x: (x[0][5:7], (x[0][0:4], x[1][0]/x[1][1])))


final = avg_temp.join(long_term_average)
final = final.map(lambda x: (str(x[1][0][0])+"-"+str(x[0]), x[1][0][1]-x[1][1]))
final.saveAsTextFile("lab1_q6")


