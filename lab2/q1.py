from pyspark import SparkContext 
sc  = SparkContext(appName = "lab2_q1")
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F
sqlContext = SQLContext(sc)


temp = sc.textFile("/user/x_johth/data/temperature-readings.csv")
parts = temp.map(lambda l: l.split(";"))
tempReadings = parts.map(lambda p: Row(station=p[0], year=p[1].split("-")[0], value=float(p[3]) ))

schemaTempReadings = sqlContext.createDataFrame(tempReadings)
schemaTempReadings = schemaTempReadings.filter( (schemaTempReadings['year'] >= 1950) & (schemaTempReadings['year'] <= 2014) )

maxReadings = schemaTempReadings.groupBy('year').agg(F.max('value').alias('value'))
maxReadings = maxReadings.join(schemaTempReadings, ['year', 'value'], 'inner')
#remove duplicates if multiple stations had the highest reading
maxReadings = maxReadings.select('year', 'station', 'value').groupBy('year').agg(F.first('station').alias('station'), F.first('value').alias('max_temperature')).orderBy('max_temperature', ascending=False).show()

minReadings = schemaTempReadings.groupBy('year').agg(F.min('value').alias('value'))
minReadings = minReadings.join(schemaTempReadings, ['year', 'value'], 'inner')
minReadings = minReadings.select('year', 'station', 'value').groupBy('year').agg(F.first('station').alias('station'), F.first('value').alias('min_temperature')).orderBy('min_temperature', ascending=True).show()
