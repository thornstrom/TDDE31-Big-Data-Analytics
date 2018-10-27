from pyspark import SparkContext 
sc  = SparkContext(appName = "lab2_q4")
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F
sqlContext = SQLContext(sc)


temp = sc.textFile("/user/x_johth/data/temperature-readings.csv")
parts = temp.map(lambda l: l.split(";"))
tempReadings = parts.map(lambda p: Row(station=p[0], date=p[1], value=float(p[3]) ))

schemaTempReadings = sqlContext.createDataFrame(tempReadings)
tempReadings = schemaTempReadings.groupBy(['station', 'date']).agg(F.max('value').alias('temperature'))
tempReadings = tempReadings.filter( (tempReadings['temperature'] >= 25) & (tempReadings['temperature'] <= 30) )


prec = sc.textFile("/user/x_johth/data/precipitation-readings.csv")
parts = prec.map(lambda l: l.split(";"))
precReadings = parts.map(lambda p: Row(station=p[0], date=p[1], value=float(p[3]) ))

schemaPrecReadings = sqlContext.createDataFrame(precReadings)
precReadings = schemaPrecReadings.groupBy(['station', 'date']).agg(F.sum('value').alias('precipitation'))
precReadings = precReadings.filter( (precReadings['precipitation'] >= 100) & (precReadings['precipitation'] <= 200) )

final = tempReadings.join(precReadings, ['station', 'date'], 'inner').select('station', 'temperature', 'precipitation').show()
