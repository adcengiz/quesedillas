# atakan shortcuts


# to read a file from hdfs
hdfs dfs -cat ../../user/bigdata/nyc_open_data/erm2-nwe9.json
# to read some lines
# this work for json 
hdfs dfs -cat ../../user/bigdata/nyc_open_data/erm2-nwe9.json | head
hdfs dfs -cat ../../user/bigdata/nyc_open_data/erm2-nwe9.json | head -n 20
hdfs dfs -cat ../../user/bigdata/nyc_open_data/erm2-nwe9.json | tail -n 20

# 311
# data part starts like this
"""
"data" : [ [ 14412548, "80F2B842-F65D-4D19-BE15-4D8A5E0BC69D", 14412548, 1472525676, "399231", 1474507502, "399231", null, "26028126", "2013-07-30T15:24:00", "2013-07-31T08:35:00", "DEP", "Department of Environmental Protection", "Sewer", "Catch Basin Clogged/Flooding (Use Comments) (SC)", null, "11233", "2310 DEAN STREET", "DEAN STREET", "HOPKINSON AVE", "ROCKAWAY AVE", null, null, "ADDRESS", "BROOKLYN", null, "N/A", "Closed", null, "The Department of Environmental Protection inspected your complaint and cleaned the catch basin or inlet. If the condition returns, please call 311 (or 212-639-9675 if calling from a non-New York City area code) to submit a new complaint.", "2013-07-31T08:35:00", "16 BROOKLYN", "BROOKLYN", "1008832", "185124", "Unspecified", "BROOKLYN", "Unspecified", "Unspecified", "Unspecified", "Unspecified", "Unspecified", "Unspecified", "Unspecified", "Unspecified", "Unspecified", null, null, null, null, null, null, null, null, null, null, null, null, "40.674765239584154", "-73.9113797256202", [ "{\"address\":\"\",\"city\":\"\",\"state\":\"\",\"zip\":\"\"}", "40.674765239584154", "-73.9113797256202", null, false ] ]
, [ 14412549, "A11A87A7-8A8F-48F3-95B2-4ADF97BF2194", 14412549, 1472525676, "399231", 1474507502, "3992
"""


# csv data in dumbo and then put to hdfs
# pyspark session
hdfs dfs -put 311_Service_Requests_from_2010_to_Present.csv hdfs://dumbo/user/ao1512

#list all files on hdfs
hdfs dfs -ls

#pyspark reading the data
data = spark.read.csv("311_Service_Requests_from_2010_to_Present.csv", header = True, mode = "DROPMALFORMED")
data.createOrReplaceTempView("data")
data.show()
#file big and wide

# take 5 rows using Spark SQL
spark.sql("select * from data").take(5)

# Schema of data
data.printSchema()
root
 |-- Unique Key: string (nullable = true)
 |-- Created Date: string (nullable = true)
 |-- Closed Date: string (nullable = true)
 |-- Agency: string (nullable = true)
 |-- Agency Name: string (nullable = true)
 |-- Complaint Type: string (nullable = true)
 |-- Descriptor: string (nullable = true)
 |-- Location Type: string (nullable = true)
 |-- Incident Zip: string (nullable = true)
 |-- Incident Address: string (nullable = true)
 |-- Street Name: string (nullable = true)
 |-- Cross Street 1: string (nullable = true)
 |-- Cross Street 2: string (nullable = true)
 |-- Intersection Street 1: string (nullable = true)
 |-- Intersection Street 2: string (nullable = true)
 |-- Address Type: string (nullable = true)
 |-- City: string (nullable = true)
 |-- Landmark: string (nullable = true)
 |-- Facility Type: string (nullable = true)
 |-- Status: string (nullable = true)
 |-- Due Date: string (nullable = true)
 |-- Resolution Description: string (nullable = true)
 |-- Resolution Action Updated Date: string (nullable = true)
 |-- Community Board: string (nullable = true)
 |-- BBL: string (nullable = true)
 |-- Borough: string (nullable = true)
 |-- X Coordinate (State Plane): string (nullable = true)
 |-- Y Coordinate (State Plane): string (nullable = true)
 |-- Open Data Channel Type: string (nullable = true)
 |-- Park Facility Name: string (nullable = true)
 |-- Park Borough: string (nullable = true)
 |-- Vehicle Type: string (nullable = true)
 |-- Taxi Company Borough: string (nullable = true)
 |-- Taxi Pick Up Location: string (nullable = true)
 |-- Bridge Highway Name: string (nullable = true)
 |-- Bridge Highway Direction: string (nullable = true)
 |-- Road Ramp: string (nullable = true)
 |-- Bridge Highway Segment: string (nullable = true)
 |-- Latitude: string (nullable = true)
 |-- Longitude: string (nullable = true)
 |-- Location: string (nullable = true)

 # count number of rows
data.count()
17563556

# summary statistics
data.describe().show()

# take 5 rows of a selected column
data.select("Created Date").take(5)
data.select("Created Date").show(5)

# print first 5 rows from each column
for i in range(len(data.columns)):
    print(data.columns[i])
    data.select(data.columns[i]).show(5)
    #data.select(data.columns[i]).take(5)


# subset the data to get the features that we are going to use
data.columns
sel_col = [1,3,5,6,7,8,16,25,38,39]
sel_col_names = [data.columns[sel_col[i]] for i in range(len(sel_col))]
>>> sel_col_names
['Created Date', 'Agency', 'Complaint Type', 'Descriptor', 'Location Type', 
'Incident Zip', 'City', 'Borough', 'Latitude', 'Longitude']

data_2 = data.select(sel_col_names)
data_2.createOrReplaceTempView("data_2")
>>> data_2
DataFrame[Created Date: string, Agency: string, Complaint Type: string, 
Descriptor: string, Location Type: string, Incident Zip: string, 
City: string, Borough: string, Latitude: string, Longitude: string]

# convert Created Date column to datetime
from datetime import datetime

date_format = '%m/%d/%Y %I:%M:%S %p'

# data_2.select("Created Date").rdd.flatMap(lambda x: x[0:len(x)]).take(5)
# ['03/06/2010 11:38:30 PM', '03/06/2010 11:50:18 PM', 
# '03/06/2010 11:53:33 PM', '03/06/2010 11:57:30 PM', '03/07/2010 12:16:15 AM']

# >>> datez = data_2.select("Created Date").rdd.flatMap(lambda x: x[0:len(x)]).take(5)
# >>> datez
# ['03/06/2010 11:38:30 PM', '03/06/2010 11:50:18 PM', '03/06/2010 11:53:33 PM', '03/06/2010 11:57:30 PM', '03/07/2010 12:16:15 AM']
# >>> datez[0]
# '03/06/2010 11:38:30 PM'
# >>> date_format = '%m/%d/%Y %I:%M:%S %p'
# >>> datetime.strptime(datez[0], date_format)
# datetime.datetime(2010, 3, 6, 23, 38, 30)


# data_2.select("Created Date").rdd.flatMap(lambda x: datetime.strptime(x,date_format)).take(5)
# #must be str, not Row
# data_2.select("Created Date").rdd.flatMap(lambda x: datetime.strptime(x[0:len(x)],date_format)).take(5)

# data_2.select("Created Date").rdd.flatMap(lambda x: datetime.strptime(str(x[0:len(x)]),date_format)).take(5)
# #ValueError: time data "('03/06/2010 11:38:30 PM',)" does not match format '%m/%d/%Y %I:%M:%S %p'

# data_2.select("Created Date").rdd.flatMap(lambda x: x).take(5)
# data_2.select("Created Date").flatMap(lambda x: x).take(5)

# data_2.select("Created Date").rdd.flatMap(lambda x: datetime.strptime(str(x[0:len(x)])[2:-3],date_format)).take(5)
# data_2.select("Created Date").rdd.flatMap(lambda x: datetime.strptime(str(x[0:100]),date_format)).take(5)


### THIS WORKS
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import unix_timestamp
#Change the column name
data_2 = data_2.withColumnRenamed("Created Date","created_date")
data_2 = data_2.withColumn('dates',
                            to_timestamp(unix_timestamp(data_2.created_date,
                                                        'MM/dd/yyyy hh:mm:ss a').cast('timestamp')),
                                         )


# aggregation by hour
"""
Created Date -> aggregate using this column

Columns to be aggregated for every hour
Count by Agency -> several columns like Agency_NYPD_count
Count by Complaint Type -> Complaint_Type_Noise_Commercial_Count
Count by Descriptor
Count by Location Type
Count By Incident Zip
Count by City
Count by Borough
Average by Latitude to 0.000005 degrees regions
Average by Longtitude to 0.000005 degrees regions
"""

for i in range(len(data_2.columns)):
    print(data_2.columns[i])
    data_2.select(data_2.columns[i]).show(5)

# dropping the "Created Date" column because we have "date" column
data_2 = data_2.drop("created_date")

# convert dates column to either hours or days
# datetime.datetime(2010, 3, 6, 23, 38, 30)) to only days or hours
>>> data_2.select("dates").take(1)
[Row(dates=datetime.datetime(2010, 3, 6, 23, 38, 30))]
>>> data_2.select("dates").take(1)[0]
Row(dates=datetime.datetime(2010, 3, 6, 23, 38, 30))
>>> data_2.select("dates").take(1)[0][0]
datetime.datetime(2010, 3, 6, 23, 38, 30)

data_2.select("dates").take(1)[0][0].year
2010
data_2.select("dates").take(1)[0][0].month
3
data_2.select("dates").take(1)[0][0].day
6
data_2.select("dates").take(1)[0][0].hour
23


from pyspark.sql.functions import date_format
#data_3 = data_2.withColumn("new_dates",date_format("dates","YYYY-MM-dd HH"))
data_3 = data_2.withColumn("new_dates",date_format("dates","YYYY-MM-dd"))
data_3.createOrReplaceTempView("data_3")
data_3.take(1)
[Row(Agency='NYPD', Complaint Type='Noise - Commercial', 
Descriptor='Loud Music/Party', Location Type='Club/Bar/Restaurant', 
Incident Zip='11238', City='BROOKLYN', Borough='BROOKLYN', 
Latitude='40.677476821236894', Longitude='-73.96893730309779', 
dates=datetime.datetime(2010, 3, 6, 23, 38, 30), new_dates='2010-03-06 23')]

>>> from pyspark.ml.feature import StringIndexer
>>> indexer = StringIndexer(inputCol = "Agency",outputCol = "Agency_onehot")
>>> indexed = indexer.fit(data_3).transform(data_3)
>>> indexed.take(5)

# finding the unique values in a column
data_3.select("Agency").distinct().show()
agency_uniq = data_3.select("Agency").distinct().rdd.map(lambda r: r[0]).collect()
len(agency_uniq)
29

data_3.select("Complaint Type").distinct().show()
complaint_uniq = data_3.select("Complaint Type").distinct().rdd.map(lambda r: r[0]).collect()
len(complaint_uniq) 
279

from pyspark.sql.functions import lit
from pyspark.sql.functions import when
from pyspark.sql.types import StringType

data_3 = data_3.withColumnRenamed("Complaint Type","complaint_type")
col_names_list = []
for i in range(len(complaint_uniq)):
    #col_name = complaint_uniq[i]
    col_name = "comp_type_" + str(i)
    print(col_name)
    col_names_list.append(col_name)
    data_3=data_3.withColumn(col_name,
                             lit(None).cast(StringType()))
    data_3=data_3.withColumn(col_name, 
                              when(data_3.complaint_type==complaint_uniq[i],1).otherwise(0))

len(data_3.columns)
290

# aggregate using new_dates column
query = "SELECT Agency, complaint_type, Descriptor, new_dates,\
        Location Type,Incident Zip, City, Borough"
for i in range(len(data_3.columns)):
    print(data_3.columns[i])
    str_add = "COUNT(" + complaint_uniq[i] + ") as " + complaint_uniq[i]

result=spark.sql("SELECT date, COUNT(felonies) as felonies, \
                    COUNT(misdemeanors) as misdemeanors, COUNT(violations)\
                 as violations FROM data GROUP BY date")


dict_agg = {}
for i in range(len(complaint_uniq)):
    dict_agg[col_names_list[i]] = "sum"

print(dict_agg)

#dict_agg = {'comp_type_2': 'sum', "comp_type_1":"sum"}

data_3.groupby("new_dates").agg(dict_agg).show()
#data_3.groupby("new_dates").agg({'comp_type_1': 'sum'}).show()
#data_3.groupby("new_dates").agg({'comp_type_2': 'sum', "comp_type_1":"sum"}).show()

data_4 = data_3.groupby("new_dates").agg(dict_agg)
#data_4 = data_3.groupby("new_dates").agg(dict_agg).collcet()
data_4.take(1)


# write pyspark dataframe to csv
data_4.write.csv("311requests.csv")
data_4.repartition(1).write.csv("311requests2.csv")
# both saves csv as empty
data_4.coalesce(1).write.csv("311reqs_3.csv")
data_4.rdd.map(lambda x: ",".join(map(str,x))).coalesce(1).saveAsTextFile("311reqs_4.csv")
data_4.toPandas().to_csv("311req_pd.csv")

# write pyspark dataframe to csv with HDFS link
hdfs_link = "hdfs://dumbo/user/ao1512/"
data_4.write.csv(hdfs_link+"311requests.csv")
data_4.repartition(1).write.csv(hdfs_link+"311requests2.csv")
# both saves csv as empty
data_4.coalesce(1).write.csv(hdfs_link+"311reqs_3.csv")
data_4.rdd.map(lambda x: ",".join(map(str,x))).coalesce(1).saveAsTextFile(hdfs_link+"311reqs_4.csv")
data_4.toPandas().to_csv(hdfs_link+"311req_pd.csv")

# remove files from hdfs
hdfs dfs -rm "file"

# check if the dataframe was written to hdfs
hdfs dfs -ls

#Copy a file to local computer
hfs -get /user/ecc290/HW1data/open-violations-header.csv
#YENI TERMINAL AC
scp ao1512@dumbo.hpc.nyu.edu:~ao1512/311req_pd.csv .

