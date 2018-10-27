import csv
import time


start = time.time()


#r = csv.reader(open("temperature-readings.csv"), delimiter=";")
#r = open("/nfshome/hadoop_examples/shared_data/")


maxTemp = {}
minTemp = {}

# Expand the list.
for i in range(1950,2015):
	maxTemp[i] = (-100, "")
	minTemp[i] = (+100, "")


with open('/nfshome/hadoop_examples/shared_data/temperatures-big.csv') as f:
	
	for line in f:
		values = line.split(";")
		year = int(values[1][:4])
		
		if year >= 1950 and year <= 2014:
			if maxTemp[year][0] < float(values[3]):
				maxTemp[year] = (float(values[3]), values[0])
			if minTemp[year][0] > float(values[3]):
				minTemp[year] = (float(values[3]), values[0])
			
			
maxTempSorted = sorted(maxTemp.items(), key = lambda tup: tup[1][0], reverse=True)
minTempSorted = sorted(minTemp.items(), key = lambda tup: tup[1][0], reverse=True)


print("Max Temp")
print("\n".join(map(str, maxTempSorted)))

print("Min Temp")
print("\n".join(map(str, minTempSorted)))


print("Time:")
print(time.time()-start)

