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

## read the data and register as an sql df
weather_data = spark.read.format("csv").options(header="true",inferschema="true").load(sys.argv[0])
weather_data.createOrReplaceTempView("weather_data")
## pass column names
weather_data = weather_data.toDF("Date","Time","Spd","Visb","Temp","Prcp","SD","SDW","SA")
weather_data.createOrReplaceTempView("weather_data")
weather_data = weather_data.withColumn("Date",weather_data.Date.cast("string"))
weather_data = weather_data.withColumn("Time",weather_data.Time.cast("string"))

## zero-pad the time with a udf
def make_time(x):
    if len(x) == 1:
            return "000"+x
    elif len(x) == 2:
            return "00"+x
    elif len(x) == 3:
            return "0"+x
    else:
            return x

myudf = udf(make_time)
weather_data = weather_data.withColumn("newformattedtime",myudf("Time"))

## different measurements need different methods for cleaning

def clean_precipitation(x):
    if x != 0.0:
        return 1
    else: 
        return 0

def clean_sa(x):
    if x != 0:
        return 1
    else:
        return 0

def clean_sd(x):
    if x == 9999:
        return 0
    else:
        return x

def clean_sdw(x):
    if x != 0:
        return 1
    else:
        return x

def clean_speed(x):
    if x > 100:
        return 0
    else:
        return x

## clean data
speed_udf = udf(clean_speed)
precip_udf = udf(clean_precipitation)
sa_udf = udf(clean_sa)
sd_udf = udf(clean_sd)
sdw_udf = udf(clean_sdw)
weather_data = weather_data.withColumn("SDWclean",sdw_udf("SDW"))
weather_data.createOrReplaceTempView("weather_data")
weather_data = weather_data.withColumn("SDclean",sd_udf("SD"))
weather_data.createOrReplaceTempView("weather_data")
weather_data = weather_data.withColumn("SAclean",sa_udf("SA"))
weather_data.createOrReplaceTempView("weather_data")
weather_data = weather_data.withColumn("Precip",precip_udf("Prcp"))
weather_data.createOrReplaceTempView("weather_data")
weather_data = weather_data.withColumn("Spdclean",speed_udf("Spd"))
weather_data.createOrReplaceTempView("weather_data")
weather_data = weather_data.drop("Prcp")
weather_data.createOrReplaceTempView("weather_data")
weather_data = weather_data.drop("SDW","SD","Spd","SA")
weather_data.createOrReplaceTempView("weather_data")

## ignore the records where the temperature was recorded as 999C
weather_data = spark.sql("SELECT joined_date,Temp,SDWclean,\
SDclean,SAclean,Spdclean,Precip,Visb FROM weather_data \
WHERE weather_data.Temp != 999.9")
weather_data.createOrReplaceTempView("weather_data")

## zero-pad the dates where necasssary
def get_year(x):
    return int(x[:4])

def get_month(x):
    if len(str(int(x[4:6]))) == 1:
        return "0" + str(int(x[4:6]))
    else: 
        return str(int(x[4:6]))

def get_day(x):
    if len(str(int(x[6:8]))) == 1:
        return "0" + str(int(x[6:8]))
    else:
        return str(int(x[6:8]))

## register udfs
year_udf = udf(get_year)
month_udf = udf(get_month)
day_udf = udf(get_day)

weather_data = weather_data.withColumn("weatheryear",year_udf("joined_date"))
weather_data.createOrReplaceTempView("weather_data")
weather_data = weather_data.withColumn("weathermonth",month_udf("joined_date"))
weather_data.createOrReplaceTempView("weather_data")
weather_data = weather_data.withColumn("weatherday",day_udf("joined_date"))
weather_data.createOrReplaceTempView("weather_data")

## cast the columns where measurements are recorded as strings as float
weather_avg = spark.sql("SELECT weatheryear,weathermonth,\
weatherday,CAST(SDWclean AS INT),CAST(SDclean AS FLOAT),\
CAST(SAclean AS FLOAT),CAST(Spdclean AS FLOAT),CAST(Visb AS FLOAT),\
Temp FROM weather_data")
weather_avg.createOrReplaceTempView("weather_avg")

## group the data by days and get the daily averages
weather_avg = spark.sql("SELECT * FROM weather_avg").\
groupby("weatheryear","weathermonth","weatherday").mean()
weather_avg.createOrReplaceTempView("weather_avg")

## order the grouped data by dates
weather_avg = spark.sql("SELECT * FROM weather_avg").\
orderBy("weatheryear","weathermonth","weatherday")
weather_avg.createOrReplaceTempView("weather_avg")

## save the output as csv to dumbo 
weather_avg.toPandas().to_csv("weather_avg_data.csv")