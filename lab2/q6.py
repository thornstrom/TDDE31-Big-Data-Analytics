from pyspark import SparkContext 
sc  = SparkContext(appName = "lab2_q6")
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F
sqlContext = SQLContext(sc)
from pyspark.sql.types import *

stations = sc.textFile("/user/x_johth/data/stations-Ostergotland.csv")
parts = stations.map(lambda l: l.split(";"))
stations = parts.map(lambda p: Row(station=p[0]))
stations_df = sqlContext.createDataFrame(stations)

temp = sc.textFile("/user/x_johth/data/temperature-readings.csv")
parts_t = temp.map(lambda l: l.split(";"))
tempReadings = parts_t.map(lambda p: Row(station=p[0], year=p[1].split("-")[0], month=p[1].split("-")[1], value=float(p[3]) ))

schemaTempReadings = sqlContext.createDataFrame(tempReadings)
schemaTempReadings = schemaTempReadings.filter( (schemaTempReadings['year'] >= 1950) & (schemaTempReadings['year'] <= 2014) )
temperature = schemaTempReadings.join(stations_df, 'station', 'inner')

long_term_average = temperature.filter( temperature['year'] <= 1980 )
long_term_average = long_term_average.groupBy('year','month','station').agg(F.avg('value').alias('value'))
long_term_average = long_term_average.groupBy('month').agg(F.avg('value').alias('long_term'))

monthly_average = temperature.groupBy('year','month','station').agg(F.avg('value').alias('value'))
monthly_average = monthly_average.groupBy('year','month').agg(F.avg('value').alias('value'))

final = long_term_average.join(monthly_average, 'month', 'inner')
final = final.withColumn('difference', final['long_term']-final['value']).select('year','month','difference').orderBy(['year','month'], ascending=False).show()


