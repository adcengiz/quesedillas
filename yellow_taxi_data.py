from pyspark.sql.functions import concat
from pyspark.sql.functions import lit
from pyspark.sql.functions import col
from pyspark.sql.functions import year, month, dayofmonth, hour
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import udf
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import to_date
from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()
sc = spark.sparkContext

## If you will take files from a different directory 
## please change the corresponding paths below
sys.argv = ["/user/adc563/weather-2011-2017.csv","/user/adc563/yellow_tripdata_2011.csv",\
"/user/adc563/yellow_tripdata_2012.csv","/user/adc563/yellow_tripdata_2013.csv",\
"/user/adc563/yellow_tripdata_2014.csv","/user/adc563/yellow_tripdata_2015-01-06.csv",\
"/user/adc563/2016_Yellow_Taxi_Trip_Data.csv"]

## some pickup and dropoff dates come as string 
## instead of datetime. Get the dates to group by
## by using these udfs
def taxi_month(x):
    if len(str(int(x[:2]))) == 1:
        return "0" + str(int(x[:2]))
    else: 
        return str(int(x[:2]))

def taxi_day(x):
    if len(str(int(x[3:5]))) == 1:
        return "0" + str(int(x[3:5]))
    else:
        return str(int(x[3:5]))

def taxi_year(x):
    return x[6:10]

taxi_month = udf(taxi_month)
taxi_day = udf(taxi_day)
taxi_year = udf(taxi_year)

## Yellow Taxi Data 2011

## read data and register as sql df
taxi_2011 =  spark.read.format("csv").options(header="true",\
inferschema="true").load(sys.argv[1])
taxi_2011.createOrReplaceTempView("taxi_2011")

## breakdown timestamp
taxi_2011 = taxi_2011.withColumn("tripyear",year(taxi_2011.Trip_Pickup_DateTime))
taxi_2011 = taxi_2011.withColumn("tripmonth",month(taxi_2011.Trip_Pickup_DateTime))
taxi_2011 = taxi_2011.withColumn("tripday",dayofmonth(taxi_2011.Trip_Pickup_DateTime))
taxi_2011.createOrReplaceTempView("taxi_2011")

## group by day
taxi_daygroups2011 = spark.sql("SELECT * FROM taxi_2011").\
groupby("tripyear","tripmonth","tripday").count()
taxi_daygroups2011.createOrReplaceTempView("taxi_daygroups2011")
## sort by day
taxi_daygroups2011 = spark.sql("SELECT * FROM taxi_daygroups2011").\
orderBy("tripyear","tripmonth","tripday")
taxi_daygroups2011.createOrReplaceTempView("taxi_daygroups2011")

## Yellow Taxi Data 2012

## read data and register as sql df
taxi_2012 =  spark.read.format("csv").options(header="true",\
inferschema="true").load(sys.argv[2])
taxi_2012.createOrReplaceTempView("taxi_2012")

## breakdown timestamp
taxi_2012 = taxi_2012.withColumn("tripyear",year(taxi_2012.Trip_Pickup_DateTime))
taxi_2012 = taxi_2012.withColumn("tripmonth",month(taxi_2012.Trip_Pickup_DateTime))
taxi_2012 = taxi_2012.withColumn("tripday",dayofmonth(taxi_2012.Trip_Pickup_DateTime))
taxi_2012.createOrReplaceTempView("taxi_2012")

## group by day
taxi_daygroups2012 = spark.sql("SELECT * FROM taxi_2012").\
groupby("tripyear","tripmonth","tripday").count()
taxi_daygroups2012.createOrReplaceTempView("taxi_daygroups2012")
## sort by day
taxi_daygroups2012 = spark.sql("SELECT * FROM taxi_daygroups2012").\
orderBy("tripyear","tripmonth","tripday")
taxi_daygroups2012.createOrReplaceTempView("taxi_daygroups2012")

## Yellow Taxi Data 2013

## read data and register as sql df
taxi_2013 =  spark.read.format("csv").options(header="true",\
inferschema="true").load(sys.argv[3])
taxi_2013.createOrReplaceTempView("taxi_2013")

## breakdown timestamp
taxi_2013 = taxi_2013.withColumn("tripyear",year(taxi_2013.Trip_Pickup_DateTime))
taxi_2013 = taxi_2013.withColumn("tripmonth",month(taxi_2013.Trip_Pickup_DateTime))
taxi_2013 = taxi_2013.withColumn("tripday",dayofmonth(taxi_2013.Trip_Pickup_DateTime))
taxi_2013.createOrReplaceTempView("taxi_2013")

## group by day
taxi_daygroups2013 = spark.sql("SELECT * FROM taxi_2013").\
groupby("tripyear","tripmonth","tripday").count()
taxi_daygroups2013.createOrReplaceTempView("taxi_daygroups2013")
## sort by day
taxi_daygroups2013 = spark.sql("SELECT * FROM taxi_daygroups2013").\
orderBy("tripyear","tripmonth","tripday")
taxi_daygroups2013.createOrReplaceTempView("taxi_daygroups2013")

## Yellow Taxi Data 2014

## read data and register as sql df
taxi_2014 = spark.read.format("csv").options(header="true",\
inferschema="true").load(sys.argv[4])
taxi_2014.createOrReplaceTempView("taxi_2014")

## breakdown timestamp
taxi_2014 = taxi_2014.withColumn("tripyear",year(taxi_2014.pickup_datetime))
taxi_2014 = taxi_2014.withColumn("tripmonth",month(taxi_2014.pickup_datetime))
taxi_2014 = taxi_2014.withColumn("tripday",dayofmonth(taxi_2014.pickup_datetime))
taxi_2014.createOrReplaceTempView("taxi_2014")

## group by day
taxi_daygroups2014 = spark.sql("SELECT * FROM taxi_2014").\
groupby("tripyear","tripmonth","tripday").count()
taxi_daygroups2014.createOrReplaceTempView("taxi_daygroups2014")
## sort by day
taxi_daygroups2014 = spark.sql("SELECT * FROM taxi_daygroups2014").\
orderBy("tripyear","tripmonth","tripday")
taxi_daygroups2014.createOrReplaceTempView("taxi_daygroups2014")

## Yellow Taxi Data 2015

## read data and register as sql df
taxi_2015 =  spark.read.format("csv").\
options(header="true",inferschema="true").load(sys.argv[5])
taxi_2015.createOrReplaceTempView("taxi_2015")

## breakdown timestamp
taxi_2015 = taxi_2015.withColumn("tripyear",year(taxi_2015.tpep_pickup_datetime))
taxi_2015 = taxi_2015.withColumn("tripmonth",month(taxi_2015.tpep_pickup_datetime))
taxi_2015 = taxi_2015.withColumn("tripday",dayofmonth(taxi_2015.tpep_pickup_datetime))
taxi_2015.createOrReplaceTempView("taxi_2015")

## group by day
taxi_daygroups2015 = spark.sql("SELECT * FROM taxi_2015").\
groupby("tripyear","tripmonth","tripday").count()
taxi_daygroups2015.createOrReplaceTempView("taxi_daygroups2015")
## sort by day
taxi_daygroups2015 = spark.sql("SELECT * FROM taxi_daygroups2015").\
orderBy("tripyear","tripmonth","tripday")
taxi_daygroups2015.createOrReplaceTempView("taxi_daygroups2015")

## get first union
taxi_union = spark.sql("SELECT * FROM taxi_daygroups2011\
 UNION ALL SELECT * FROM taxi_daygroups2012 UNION ALL SELECT * \
 FROM taxi_daygroups2013 UNION ALL SELECT * FROM taxi_daygroups2014\
  UNION ALL SELECT * FROM taxi_daygroups2015")
taxi_union.createOrReplaceTempView("taxi_union")

## Yellow Taxi Data 2016

## read data and register as sql df
taxi_2016 =  spark.read.format("csv").options(header="true",\
inferschema="true").load(sys.argv[6])
taxi_2016.createOrReplaceTempView("taxi_2016")

## breakdown timestamp
taxi_2016 = taxi_2016.withColumn("tripyear",taxi_year(taxi_2016.tpep_pickup_datetime))
taxi_2016 = taxi_2016.withColumn("tripmonth",taxi_month(taxi_2016.tpep_pickup_datetime))
taxi_2016 = taxi_2016.withColumn("tripday",taxi_day(taxi_2016.tpep_pickup_datetime))
taxi_2016.createOrReplaceTempView("taxi_2016")

## group by day
taxi_daygroups2016 = spark.sql("SELECT * FROM taxi_2016").\
groupby("tripyear","tripmonth","tripday").count()
taxi_daygroups2016.createOrReplaceTempView("taxi_daygroups2016")
## sort by day
taxi_daygroups2016 = spark.sql("SELECT * FROM taxi_daygroups2016").\
orderBy("tripyear","tripmonth","tripday")
taxi_daygroups2016.createOrReplaceTempView("taxi_daygroups2016")

## get second union
taxi_union_2011_2016 = spark.sql("SELECT * FROM taxi_union \
UNION ALL SELECT * FROM taxi_daygroups2016")
taxi_union_2011_2016.createOrReplaceTempView("taxi_union_2011_2016")
## write to csv
taxi_union_2011_2016.toPandas().to_csv("taxi_union_2011_2016.csv")
