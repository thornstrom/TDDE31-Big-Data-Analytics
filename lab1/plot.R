rm(list=ls())
library(stringi)
library(zoo)
library(anytime)
library(ggplot2)
data0 <- read.csv2("part-00000.csv", dec=".", sep=",")
data1 <- read.csv2("part-00001.csv", dec=".", sep=",")
data3 <- read.csv2("part-00003.csv", dec=".", sep=",")
data4 <- read.csv2("part-00004.csv", dec=".", sep=",")
data5 <- read.csv2("part-00005.csv", dec=".", sep=",")
data6 <- read.csv2("part-00006.csv", dec=".", sep=",")
data7 <- read.csv2("part-00007.csv", dec=".", sep=",")
data10 <- read.csv2("part-00010.csv", dec=".", sep=",")
data11 <- read.csv2("part-00011.csv", dec=".", sep=",")
names(data0) = c('dates', 'value')
names(data1) = c('dates', 'value')
names(data3) = c('dates', 'value')
names(data4) = c('dates', 'value')
names(data5) = c('dates', 'value')
names(data6) = c('dates', 'value')
names(data7) = c('dates', 'value')
names(data10) = c('dates', 'value')
names(data11) = c('dates', 'value')

test = rbind(data0,data1,data3,data4,data5,data6,data7,data10,data11)
test$value <- substr(as.character(test$value),start= 1, stop= nchar(as.character(test$value) )-1 )
test$dates <- substr(as.character(test$dates),start= 3, stop= nchar(as.character(test$dates) )-1 )
test$dates<-as.Date(as.yearmon(test$dates))
n=dim(test)[1]
test$mean = c(1:n)
test=test[order(test$dates),]

for (i in 1:n){
  temp = 0
  test$mean[i] = 0
  for (j in 1:i){
    temp = temp+1
    test$mean[i] = test$mean[i]+as.numeric(test$value[j])
  }
  test$mean[i] = test$mean[i]/temp
}
#361 = 1980-01
for (i in 1:360){
  test$mean[i] = 0
}

plot(test$dates,test$value, type="l", col="lightblue", xlab="dates", ylab="Temperature anomaly")
lines(test$dates,test$mean, col="red")
abline(h=0)


