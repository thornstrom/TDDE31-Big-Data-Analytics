from pyspark import SparkContext 
sc  = SparkContext(appName = "lab2_q3")
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F
sqlContext = SQLContext(sc)

temp = sc.textFile("/user/x_johth/data/temperature-readings.csv")
parts = temp.map(lambda l: l.split(";"))
tempReadings = parts.map(lambda p: Row(station=p[0], year=p[1].split("-")[0], month=p[1].split("-")[1], value=float(p[3]) ))

schemaTempReadings = sqlContext.createDataFrame(tempReadings)
schemaTempReadings = schemaTempReadings.filter( (schemaTempReadings['year'] >= 1960) & (schemaTempReadings['year'] <= 2014) )

avgTemperatures = schemaTempReadings.groupBy(['year', 'month', 'station']).agg(F.avg('value').alias('average_temperature')).orderBy('average_temperature', ascending=False).show()
