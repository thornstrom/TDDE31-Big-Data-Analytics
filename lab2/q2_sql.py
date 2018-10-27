from pyspark import SparkContext 
sc  = SparkContext(appName = "lab2_q2")
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F
sqlContext = SQLContext(sc)

temp = sc.textFile("/user/x_johth/data/temperature-readings.csv")
parts = temp.map(lambda l: l.split(";"))
tempReadings = parts.map(lambda p: Row(station=p[0], date=p[1], year=p[1].split("-")[0], month=p[1].split("-")[1],  time=p[2], value=float(p[3]), quality=p[4] ))

schemaTempReadings = sqlContext.createDataFrame(tempReadings)
schemaTempReadings.registerTempTable("tempReadings")

all_readings = sqlContext.sql("SELECT year, month, count(station) as all_readings FROM tempReadings WHERE year>=1950 and year <=2014 and value>=10.0 GROUP BY year, month ORDER BY all_readings DESC").show()
distinct_readings = sqlContext.sql("SELECT year, month, count(DISTINCT station) as distinct_readings FROM tempReadings WHERE year>=1950 and year <=2014 and value>10 GROUP BY year, month ORDER BY distinct_readings DESC").show()
