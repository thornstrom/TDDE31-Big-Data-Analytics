from pyspark import SparkContext 
sc  = SparkContext(appName = "lab2_q5")
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F
sqlContext = SQLContext(sc)
from pyspark.sql.types import *

stations = sc.textFile("/user/x_johth/data/stations-Ostergotland.csv")
parts = stations.map(lambda l: l.split(";"))
stations = parts.map(lambda p: Row(station=p[0]))
stations_df = sqlContext.createDataFrame(stations)

prec = sc.textFile("/user/x_johth/data/precipitation-readings.csv")
parts_p = prec.map(lambda l: l.split(";"))
precReadings = parts_p.map(lambda p: Row(station=p[0], year=p[1].split("-")[0], month=p[1].split("-")[1], value=float(p[3]) ))

schemaPrecReadings = sqlContext.createDataFrame(precReadings)
schemaPrecReadings = schemaPrecReadings.filter( (schemaPrecReadings['year'] >= 1993) & (schemaPrecReadings['year'] <= 2016) )

precipiation = schemaPrecReadings.join(stations_df, 'station', 'inner')


precipiation = precipiation.groupBy('year','month','station').agg(F.sum('value').alias('value'))
precipiation = precipiation.groupBy('year','month').agg(F.avg('value').alias('avgMonthlyPrecipitation')).orderBy(['year','month'], ascending=False).show()
