


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