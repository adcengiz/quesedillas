# vehicular collisions
hdfs dfs -put vehcol.csv hdfs://dumbo/user/ao1512
hdfs dfs -ls

data = spark.read.csv("vehcol.csv", header = True, mode = "DROPMALFORMED")
data.createOrReplaceTempView("data")

data.printSchema()

data.count()

#data.describe().show()

# print first 5 rows from each column
for i in range(len(data.columns)):
    print(i)
    print(data.columns[i])
    data.select(data.columns[i]).show(5)

# subset the data to get the features that we are going to use
data.columns
sel_col = [0,2,10,11,12,13,14,15,16,17,24,25]
sel_col_names = [data.columns[sel_col[i]] for i in range(len(sel_col))]
sel_col_names
['DATE', 'BOROUGH', 'NUMBER OF PERSONS INJURED', 
'NUMBER OF PERSONS KILLED', 'NUMBER OF PEDESTRIANS INJURED', 
'NUMBER OF PEDESTRIANS KILLED', 'NUMBER OF CYCLIST INJURED', 
'NUMBER OF CYCLIST KILLED', 'NUMBER OF MOTORIST INJURED',
 'NUMBER OF MOTORIST KILLED', 'VEHICLE TYPE CODE 1', 'VEHICLE TYPE CODE 2']

data_2 = data.select(sel_col_names)
data_2.createOrReplaceTempView("data_2")

from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import unix_timestamp
#Change the column name
data_2 = data_2.withColumn('DATE',
                            to_timestamp(unix_timestamp(data_2.DATE,
                                                        'MM/dd/yyyy').cast('timestamp')),
                                         )

from pyspark.sql.functions import date_format
#data_3 = data_2.withColumn("new_dates",date_format("dates","YYYY-MM-dd HH"))
data_3 = data_2.withColumn("NEW_DATE",date_format("DATE","YYYY-MM-dd"))
data_3.createOrReplaceTempView("data_3")


boroughs = data_3.select("BOROUGH").distinct().rdd.map(lambda r: r[0]).collect()
len(boroughs)

veh_type_1 = data_3.select("VEHICLE TYPE CODE 1").distinct().rdd.map(lambda r: r[0]).collect()
len(veh_type_1)
veh_type_2 = data_3.select("VEHICLE TYPE CODE 2").distinct().rdd.map(lambda r: r[0]).collect()
len(veh_type_2)

data_3 = data_3.withColumnRenamed("VEHICLE TYPE CODE 1","vehicle_1")
data_3 = data_3.withColumnRenamed("VEHICLE TYPE CODE 2","vehicle_2")


# one hot boroughs
from pyspark.sql.functions import lit
from pyspark.sql.functions import when
from pyspark.sql.types import StringType
col_names_list = []
for i in range(len(boroughs)):
    #col_name = complaint_uniq[i]
    col_name = "borough_" + str(i)
    print(col_name)
    col_names_list.append(col_name)
    data_3=data_3.withColumn(col_name,
                             lit(None).cast(StringType()))
    data_3=data_3.withColumn(col_name, 
                              when(data_3.BOROUGH==boroughs[i],1).otherwise(0))

len(data_3.columns)


# one hot vehicle 1
#col_names_list = []
for i in range(len(veh_type_1)):
    #col_name = complaint_uniq[i]
    col_name = "veh_type_1_" + str(i)
    print(col_name)
    col_names_list.append(col_name)
    data_3=data_3.withColumn(col_name,
                             lit(None).cast(StringType()))
    data_3=data_3.withColumn(col_name, 
                              when(data_3.vehicle_1==veh_type_1[i],1).otherwise(0))

len(data_3.columns)


# one hot vehicle 2
#col_names_list = []
for i in range(len(veh_type_2)):
    #col_name = complaint_uniq[i]
    col_name = "veh_type_2_" + str(i)
    print(col_name)
    col_names_list.append(col_name)
    data_3=data_3.withColumn(col_name,
                             lit(None).cast(StringType()))
    data_3=data_3.withColumn(col_name, 
                              when(data_3.vehicle_2==veh_type_2[i],1).otherwise(0))

len(data_3.columns)
col_names_list

# aggregation dictionary
dict_agg = {}
for i in range(len(col_names_list)):
    dict_agg[col_names_list[i]] = "sum"

print(dict_agg)

# aggregation
data_4 = data_3.groupby("NEW_DATE").agg(dict_agg)

data_4.toPandas().to_csv("vehicular_collisions.csv")


# in a new terminal (in your local home)
scp ao1512@dumbo.hpc.nyu.edu:~ao1512/vehicular_collisions.csv .