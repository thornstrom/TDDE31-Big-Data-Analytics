from pyspark import SparkContext 
sc  = SparkContext(appName = "lab2_q2")
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F
sqlContext = SQLContext(sc)


temp = sc.textFile("/user/x_johth/data/temperature-readings.csv")
parts = temp.map(lambda l: l.split(";"))
tempReadings = parts.map(lambda p: Row(station=p[0], year=p[1].split("-")[0], month=p[1].split("-")[1], value=float(p[3]) ))

schemaTempReadings = sqlContext.createDataFrame(tempReadings)
schemaTempReadings = schemaTempReadings.filter( (schemaTempReadings['year'] >= 1950) & (schemaTempReadings['year'] <= 2014) &( schemaTempReadings['value'] > 10 ) )

allReadings = schemaTempReadings.groupBy(['year', 'month']).count().orderBy('count', ascending=False).show()

uniqueReadings = schemaTempReadings.select(['year', 'month', 'station']).distinct().groupBy(['year','month']).count().orderBy('count', ascending=False).show()
