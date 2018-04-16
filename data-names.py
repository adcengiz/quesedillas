## Configuration
module load python/gnu/3.4.4
module load spark/2.2.0
export PYSPARK_PYTHON=/share/apps/python/3.4.4/bin/python
export PYTHONHASHSEED=0
export SPARK_YARN_USER_ENV=PYTHONHASHSEED=0
pyspark 

## Which link corresponds to which dataset
datalinks = {"311requests": "user/bigdata/nyc_open_data/erm2-nwe9.json",\
"citibike": "user/bigdata/nyc_open_data/vsnr-94wk.json",\
"crime_data": "user/bigdata/nyc_open_data/qgea-i56i.json",\
"vehicle_collisions": "user/bigdata/nyc_open_data/h9gi-nx95.json",\
"weather2011": "user/bigdata/nyc_open_data/q39e-7gbs.json",\
"weather2012": "user/bigdata/nyc_open_data/5gde-fmj3.json",\
"weather2013": "user/bigdata/nyc_open_data/rgfe-8y2z.json",\
"weather2014": "user/bigdata/nyc_open_data/jzst-u7j8.json",\
"weather2015": "user/bigdata/nyc_open_data/hypw-js3b.json",\
"weather2016": "user/bigdata/nyc_open_data/8u86-bviy.json",\
"taxi2011":"user/bigdata/nyc_open_data/jr6k-xwua.json",\
"taxi2012":"user/bigdata/nyc_open_data/fd5y-xikb.json",\
"taxi2013":"user/bigdata/nyc_open_data/7rnv-m532.json",\
"taxi2014":"user/bigdata/nyc_open_data/gn7m-em8n.json",\
"taxi2015":"user/bigdata/nyc_open_data/ba8s-jw6u.json",\
"taxi2016":"user/bigdata/nyc_open_data/k67s-dv2t.json"}

## Citibike-Weather Merge
bike_weather = spark.read.format("csv").options(header="true", inferschema="true").load("bike_weather.csv")
bike_weather.createOrReplaceTempView("bike_weather")
bike_weather = spark.sql("SELECT * FROM bike_weather WHERE mean_duration IS NOT NULL")
bike_weather.createOrReplaceTempView("bike_weather")
bike_weather.show()
+---+------------------+------------------+------------------+-------------------+----------+------------------+---------+-------------------+
|_c0|              Temp|               Spd|              Prcp|              day_x|merge_date|     mean_duration|num_trips|              day_y|
+---+------------------+------------------+------------------+-------------------+----------+------------------+---------+-------------------+
|912| 22.83421052631578|4.9921052631578915|              58.8|2013-07-01 00:00:00|  20130701| 978.5992792792791|    16650|2013-07-01 00:00:00|
|913|23.210344827586205| 5.799999999999998|               1.6|2013-07-02 00:00:00|  20130702| 958.1295669377887|    22745|2013-07-02 00:00:00|
|914|23.536363636363635| 4.724242424242422|27.099999999999998|2013-07-03 00:00:00|  20130703| 974.3284851811197|    21864|2013-07-03 00:00:00|
|915|24.679310344827588| 4.282758620689656|               6.3|2013-07-04 00:00:00|  20130704|1273.1084385917763|    22326|2013-07-04 00:00:00|
|916|25.992000000000004|             4.804|               0.0|2013-07-05 00:00:00|  20130705| 1082.426609284864|    21842|2013-07-05 00:00:00|
|917|27.604166666666668|            4.8375|               0.0|2013-07-06 00:00:00|  20130706|1159.5757072360386|    20467|2013-07-06 00:00:00|
|918| 28.10416666666666| 4.558333333333333|               0.0|2013-07-07 00:00:00|  20130707|1118.2994090931288|    20477|2013-07-07 00:00:00|
|919|             27.25| 5.741666666666667|               0.0|2013-07-08 00:00:00|  20130708|1289.5298635207032|    21615|2013-07-08 00:00:00|
|920|25.904166666666665|            4.8125|               0.0|2013-07-09 00:00:00|  20130709| 965.9312713486732|    26641|2013-07-09 00:00:00|
|921| 25.11935483870968| 4.264516129032257|               5.6|2013-07-10 00:00:00|  20130710|  931.917262552464|    25732|2013-07-10 00:00:00|
|922|25.939999999999998| 3.476666666666665|               2.0|2013-07-11 00:00:00|  20130711|1009.7180652823853|    24417|2013-07-11 00:00:00|
|923| 24.07812500000001|          4.634375|               3.5|2013-07-12 00:00:00|  20130712| 938.1076502157215|    19006|2013-07-12 00:00:00|
|924|23.615624999999994| 4.068749999999997|              23.5|2013-07-13 00:00:00|  20130713|1116.9396607833378|    26119|2013-07-13 00:00:00|
|925| 26.57083333333334| 4.645833333333335|               0.0|2013-07-14 00:00:00|  20130714|1161.7602690613585|    29287|2013-07-14 00:00:00|
|926|           29.5125| 3.637499999999999|               0.0|2013-07-15 00:00:00|  20130715| 919.0969396843492|    28069|2013-07-15 00:00:00|
|927|30.483333333333324| 4.270833333333334|               0.0|2013-07-16 00:00:00|  20130716| 907.7130554252395|    29842|2013-07-16 00:00:00|
|928|30.579166666666666|3.1958333333333333|               0.0|2013-07-17 00:00:00|  20130717| 880.1669721767594|    30550|2013-07-17 00:00:00|
|929|30.241666666666664|4.3083333333333345|               0.0|2013-07-18 00:00:00|  20130718| 862.8295403373862|    28869|2013-07-18 00:00:00|
|930|           30.1875| 4.675000000000002|               0.0|2013-07-19 00:00:00|  20130719| 880.2631717498401|    26591|2013-07-19 00:00:00|
|931|30.123999999999995| 6.660000000000001|               0.5|2013-07-20 00:00:00|  20130720|1076.8692934567607|    25278|2013-07-20 00:00:00|
+---+------------------+------------------+------------------+-------------------+----------+------------------+---------+-------------------+
### Reading
## this one takes some time to read
reqs311 = spark.read.option("multiline","true").json("../../"+ str(datalinks["311requests"]))

citibike = spark.read.option("multiline","true").json("../../"+ str(datalinks["citibike"]))
crime_data = spark.read.option("multiline","true").json("../../"+ str(datalinks["crime_data"]))
vehicle_collisions = spark.read.option("multiline","true").json("../../"+ str(datalinks["vehicle_collisions"]))

taxi2011 = spark.read.option("multiline","true").json("../../"+ str(datalinks["taxi2011"]))
taxi2012 = spark.read.option("multiline","true").json("../../"+ str(datalinks["taxi2012"]))
taxi2013 = spark.read.option("multiline","true").json("../../"+ str(datalinks["taxi2013"]))
taxi2014 = spark.read.option("multiline","true").json("../../"+ str(datalinks["taxi2014"]))
taxi2015 = spark.read.option("multiline","true").json("../../"+ str(datalinks["taxi2015"]))
taxi2016 = spark.read.option("multiline","true").json("../../"+ str(datalinks["taxi2016"]))

taxi2011.createOrReplaceTempView("taxi2011")
taxi2012.createOrReplaceTempView("taxi2012")
taxi2013.createOrReplaceTempView("taxi2013")
taxi2014.createOrReplaceTempView("taxi2014")
taxi2015.createOrReplaceTempView("taxi2015")
taxi2016.createOrReplaceTempView("taxi2016")

###
###
### WEATHER DATA READINGS
###
###

weather2011 = spark.read.option("multiline","true").json("../../"+ str(datalinks["weather2011"]))
weather2011.createOrReplaceTempView("weather2011")
weather2011_data = weather2011.select(weather2011.data).collect()
# len(weather2011_data) = 1
# len(weather2011_data[0]) = 1
# len(weather2011_data[0][0]) = 4081 ## rows

weather2012 = spark.read.option("multiline","true").json("../../"+ str(datalinks["weather2012"]))
weather2012.createOrReplaceTempView("weather2012")
weather2012_data = weather2012.select(weather2012.data).collect()
# len(weather2012_data) = 1
# len(weather2012_data[0]) = 1
# len(weather2012_data[0][0]) = 14112 ## rows

weather2013 = spark.read.option("multiline","true").json("../../"+ str(datalinks["weather2013"]))
weather2013.createOrReplaceTempView("weather2013")
weather2013_data = weather2013.select(weather2013.data).collect()
# len(weather2013_data) = 1
# len(weather2013_data[0]) = 1
# len(weather2013_data[0][0]) = 16170 ## rows

weather2014 = spark.read.option("multiline","true").json("../../"+ str(datalinks["weather2014"]))
weather2014.createOrReplaceTempView("weather2014")
weather2014_data = weather2014.select(weather2014.data).collect()
# len(weather2014_data) = 1
# len(weather2014_data[0]) = 1
# len(weather2014_data[0][0]) = 13974 ## rows

weather2015 = spark.read.option("multiline","true").json("../../"+ str(datalinks["weather2015"]))
weather2015.createOrReplaceTempView("weather2015")
weather2015_data = weather2015.select(weather2015.data).collect()
# len(weather2015_data) = 1
# len(weather2015_data[0]) = 1
# len(weather2015_data[0][0]) = 13223 ## rows

weather2016 = spark.read.option("multiline","true").json("../../"+ str(datalinks["weather2016"]))
weather2016.createOrReplaceTempView("weather2016")
weather2016_data = weather2016.select(weather2016.data).collect()
# len(weather2016_data) = 1
# len(weather2016_data[0]) = 1
# len(weather2016_data[0][0]) = 11746 ## rows

###
###
### TAXI DATA READINGS
###
###

### taxi2011: _corrupt_record (TRY AGAIN)
taxi2011 = spark.read.option("multiline","true").json("../../"+ str(datalinks["taxi2011"]))

### taxi2012: _corrupt_record (TRY AGAIN)
taxi2012 = spark.read.option("multiline","true").json("../../"+ str(datalinks["taxi2012"]))

taxi2013 = spark.read.option("multiline","true").json("../../"+ str(datalinks["taxi2013"]))
taxi2013.createOrReplaceTempView("taxi2013")
taxi2013_data_top = taxi2013.select(taxi2013.data).take(10)

taxi2014 = spark.read.option("multiline","true").json("../../"+ str(datalinks["taxi2014"]))
taxi2014.createOrReplaceTempView("taxi2014")
taxi2014_data = taxi2014.select(taxi2014.data).collect()

taxi2015 = spark.read.option("multiline","true").json("../../"+ str(datalinks["taxi2015"]))
taxi2015.createOrReplaceTempView("taxi2015")
taxi2015_data = taxi2015.select(taxi2015.data).collect()

taxi2016 = spark.read.option("multiline","true").json("../../"+ str(datalinks["taxi2016"]))
taxi2016.createOrReplaceTempView("taxi2016")
taxi2016_data = taxi2016.select(taxi2016.data).collect()

## Other Read

crime_all = spark.read.option("multiline","true").json("../../"+ str(datalinks["crime_data"]))
crime_all.createOrReplaceTempView("crime_all")
crime_all_data = crime_all.select(crime_all.data).collect()

vehicle_collisions = spark.read.option("multiline","true").json("../../"+ str(datalinks["vehicle_collisions"]))
vehicle_collisions.createOrReplaceTempView("vehicle_collisions")
vehicle_collisions_data = vehicle_collisions.select(vehicle_collisions.data).collect()

### Problem with the link, on it
citibike = spark.read.json("../../"+ str(datalinks["citibike"]))
citibike.createOrReplaceTempView("citibike")
citibike_data = citibike.select(citibike.data).collect()