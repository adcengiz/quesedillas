#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Sat May  5 13:55:14 2018

@author: Juan Jose Zamudio
"""
#Process crime violations
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
spark = SparkSession.builder.appName("process_crime").config("spark.some.config.option", "some-value").getOrCreate()

data= spark.read.format("csv").options(header="true", inferschema="true").load('crime.csv')
data.createOrReplaceTempView("data")

data=data.withColumnRenamed('CMPLNT_FR_DT','day')
data=data.withColumnRenamed('CMPLNT_FR_TM','hour')
data=data.withColumnRenamed('KY_CD','crime_key')
data=data.withColumnRenamed('OFNS_DESC','crime')
data=data.withColumnRenamed('LAW_CAT_CD','crime_level')

data=data.withColumn('day',regexp_replace('day', '/', '-'))
data=data.withColumn('time_hour',concat(data.day,lit(' '),data.hour))


data=data.withColumn('date',to_timestamp(unix_timestamp(data.time_hour,'MM-dd-yyyy').cast('timestamp')))

data=data.select('date','crime_level')

data=data.withColumn('felonies',lit(None).cast(StringType()))
data=data.withColumn("misdemeanors",lit(None).cast(StringType()))
data=data.withColumn('violations',lit(None).cast(StringType()))

data= data.withColumn("felonies", when(data.crime_level=='FELONY',1).otherwise(0))
data= data.withColumn("misdemeanors", when(data.crime_level=='MISDEMEANOR',1).otherwise(0))
data= data.withColumn("violations", when(data.crime_level=='VIOLATION',1).otherwise(0))


data.createOrReplaceTempView("data")

result=spark.sql("SELECT date, SUM(felonies) as felonies, SUM(misdemeanors) as misdemeanors, SUM(violations) as violations FROM data GROUP BY date")



result.select(format_string('%s\t%s%s%s',result.date,result.felonies,result.violations, result.misdemeanors)).write.save("crime_clean.csv",format="csv")
