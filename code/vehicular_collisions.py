# vehicular collisions

#putting the downloaded data to hdfs
#the data was downloaded using wget to DUMBO
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
# ['DATE', 'BOROUGH', 'NUMBER OF PERSONS INJURED', 
# 'NUMBER OF PERSONS KILLED', 'NUMBER OF PEDESTRIANS INJURED', 
# 'NUMBER OF PEDESTRIANS KILLED', 'NUMBER OF CYCLIST INJURED', 
# 'NUMBER OF CYCLIST KILLED', 'NUMBER OF MOTORIST INJURED',
#  'NUMBER OF MOTORIST KILLED', 'VEHICLE TYPE CODE 1', 'VEHICLE TYPE CODE 2']

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
# scp ao1512@dumbo.hpc.nyu.edu:~ao1512/vehicular_collisions.csv .

# read csv to local python
boroughs = [None, 'QUEENS', 'BROOKLYN', 'BRONX', 'MANHATTAN', 'STATEN ISLAND']
veh_type_1 = ['trail', 'RF', 'NYC M', 'FREIG', 'MAN L', 'subn', 'van', 'SKATE', 'PM', 'fork', 'KENWO', 'SPC', 'COMMM', 'TR/KI', 'REFG', 'GOLF', 'BS', 'WHITE', 'PASSENGER VEHICLE', 'AM', 'PL', 'MK', 'Road', 'Tow T', 'Box T', 'Golf', 'OP', 'SMALL COM VEH(4 TIRES) ', 'Motor', 'JCB40', 'horse', 'com', 'deliv', 'P/SE', 'AMBUL', 'Tract', 'OLM', 'mta b', 'BUS', 'utlit', 'SEMI', 'fdny', 'power', 'Schoo', 'USPS', 'G Sem', 'CARGO', 'LIVER', 'AMB', 'garba', 'COM', 'GOV V', 'sanit', 'BU', 'LF', 'scoot', 'VAN C', '013', 'SEMI-', 'RUBBE', 'Mini', 'bobca', None, '1S', 'EMS', 'bulld', 'VAN T', 'LL', 'GG', 'VAN/T', 'VAN', 'SCHOO', 'bus', 'MAIL', 'TAN P', 'carri', 'YELLO', 'SELF-', 'HINO', 'TAXI', 'FORD', 'FIRET', 'G OMR', 'US Po', 'tow', 'CAB', 'DUMPS', 'PK', 'bsd', 'passa', 'Util', 'FIRE TRUCK', 'BR', 'DELIV', 'LW', 'HO', 'SPRIN', 'VT', 'FORK', 'MD', 'ELECT', 'EN', 'FOOD', 'GN', 'FB', 'SP', '3D', 'JOHN', 'FREE', 'Humme', 'PUMP', 'OML', 'tr', 'US PO', 'TR', 'Cat', 'TRUCK', 'MB', 'P/SH', '994', 'tk', 'Truck', 'C0MME', 'jeep', 'mopet', 'LARGE COM VEH(6 OR MORE TIRES)', 'BOX T', 'OIL T', 'MS', 'HORSE', 'ABULA', 'POWER', 'Tow', 'semi-', 'PAS', 'BOBCA', 'Ice C', 'OMR', 'Movin', 'usps', 'Scoot', 'Comme', 'UNKNOWN', 'Light', 'BK', 'forkl', 'ROAD', 'PEDICAB', 'OMT', 'CM', 'TRAC', 'John', 'ambul', 'FR', 'ford', 'Mecha', 'AMBULANCE', 'BACKH', 'Elect', 'CONV', 'TN', 'FLAT', 'dsny', 'Ambul', 'BOOM', 'E ONE', 'SELF', 'PSD', 'CAMPE', 'MOTORCYCLE', 'UPS t', 'GR', 'LIMO', 'tow t', 'TANDU', 'POSTA', 'posta', 'PICKU', 'flat', 'TRAIL', 'EBIKE', 'PICK-UP TRUCK', 'BA', 'Trail', 'paylo', 'Car C', 'GE/SC', 'Pavin', 'SPORT UTILITY / STATION WAGON', '15 Pa', 'Forkl', 'elect', 'CONST', 'delv', 'Van', 'STREE', 'N/A', 'schoo', 'cargo', 'E3', 'PSR', 'unk', 'MH', '4whee', 'dp', 'mopad', 'dump', 'SANIT', 'GARBA', 'omni', 'DELV', 'Marke', 'Bus', 'Fire', 'White', 'OTHER', 'TL TR', 'e-bik', 'COMME', 'Tow t', 'VC', 'cemen', 'red,', 'PEDIC', 'FORKL', 'ambu', 'omr', 'WD', 'CB', 'Freig', 'TRANS', 'BICYCLE', 'house', 'MTA B', 'ST150', 'EMS A', 'Crane', 'DS', 'TOW T', 'UTIL', 'truck', 'stree', 'H/WH', 'TOW', 'MOPED', 'MACK', 'mta', 'van t', 'utili', 'unkno', 'RV', 'ladde', 'Sweep', 'UTILI', 'TT', 'pick', 'HWY C', 'picku', 'Picku', 'TRL', 'CO', 'HIGHL', 'east', 'Tlr', 'Cargo', 'MOTOR', 'e-350', 'UHAUL', 'comme', 'NYC F', 'TRACT', 'AR', 'Dump', 'DUMP', 'TK', 'tank', 'Ford', 'VN', 'DEPT', 'fire', 'E-BIK', 'nyc a', 'FDNY', 'GMC V', 'BULLD', 'VAN F', 'Firet', 'UNKNO', '3DC-', 'LIVERY VEHICLE', 'SCOOTER', 'DP', 'ST', 'p/sh']
veh_type_2 = ['BOX', 'trail', 'TRK', 'RF', 'FREIG', 'subn', 'van', 'PM', 'STAK', 'SKATE', 'SUBUR', 'PU', 'fork', 'moped', 'SPC', 'COM T', 'BS', 'PASSENGER VEHICLE', 'box t', 'AM', 'PL', 'MK', 'NYC a', 'OP', 'SMALL COM VEH(4 TIRES) ', 'horse', 'INTER', 'semi', 'com', 'deliv', 'AMBUL', 'Tract', 'Subn', 'mta b', 'BUS', 'FARM', 'SEMI', 'power', 'fdny', 'Schoo', 'USPS', 'NEW Y', '315 e', 'Pick', 'comm', 'AMB', 'APORT', 'garba', 'COM', 'sanit', 'BU', 'LF', 'scoot', 'FLATB', 'SEMI-', 'DELV.', 'plow', 'bobca', None, 'Self', 'New Y', 'DOT #', 'VAN T', 'CITY', 'frieg', 'LL', 'GG', 'VAN/T', 'VAN', 'SCHOO', 'bus', 'Tow-t', 'Unkno', 'BUs', 'MAIL', 'CABIN', 'E BIK', 'ACCES', 'HINO', 'TAXI', 'FORD', 'FIRET', 'nissa', 'uliti', '00', 'scaff', 'tow', 'pick-', 'PK', 'Veriz', 'MINI', 'NYPD', 'bsd', 'APPOR', 'AMULA', 'FIRE TRUCK', 'BR', 'DELIV', 'HO', 'LW', 'VT', 'vol', 'FORK', 'Passe', 'MD', 'ELECT', 'CATER', 'FOOD', 'FB', 'SP', 'DIESE', '3D', 'REFRI', 'nyc d', 'TR', 'MB', 'TRUCK', 'P/SH', 'tk', 'EB', 'hook', 'n/a', 'Truck', 'TRIAL', 'LARGE COM VEH(6 OR MORE TIRES)', 'FORTL', 'BOX T', 'MS', 'UD', 'CAT 4', 'POWER', 'WHBL', 'Tow', 'HI-LO', 'mail', 'AMABU', 'Scoot', 'util', 'Limou', 'forkl', 'UNKNOWN', 'PEDICAB', 'CM', 'TRAC', 'ups t', 'ambul', 'FRIEG', 'FR', 'omnib', 'AMBULANCE', 'detac', 'BACKH', 'CONV', 'TN', 'FLAT', 'Ambul', 'BOOM', 'SELF', 'RENTA', 'Wagon', 'MOTORCYCLE', 'E PAS', 'GR', 'LIMO', 'Bucke', 'RYDER', 'amb', 'Sprin', 'posta', 'POSTA', 'LIMO/', 'VANG', 'TRAIL', 'EBIKE', 'refg', 'PICK-UP TRUCK', 'BA', 'firet', 'Trail', 'BACK', 'RD/S', 'MV', 'work', 'SE', 'SPORT UTILITY / STATION WAGON', 'SNOW', 'VAN W', 'elect', 'CONST', 'Van', 'U.S.', 'STREE', 'DOT R', 'taxi', 'schoo', 'N/A', 'uhaul', 'ARMOR', 'MH', 'unk', 'WAGON', 'BLUE', 'TRLPM', 'LG', 'RMP V', 'TRACK', 'tract', 'dp', 'SANIT', 'dump', 'GARBA', 'DELV', 'e amb', 'Bus', 'small', 'OTHER', 'Fire', 'OMNIB', 'COMME', 'VC', '3 doo', 'CAT', 'motor', 'armor', 'FORKL', 'ambu', 'Mo pa', 'CB', 'TRANS', 'BICYCLE', '18 Wh', 'MTA B', '2 TON', 'Com', 'DS', 'TOW T', 'IP', 'UTIL', 'truck', 'Utili', 'MOPED', 'TOW', 'MACK', 'unkno', 'utili', 'Backh', 'ENGIN', 'RV', 'Gas T', 'UTILI', 'TT', 'pick', 'picku', 'Picku', 'TRL', 'Cargo', 'JLG B', 'MOTOR', 'REP', 'UHAUL', 'comme', 'TRACT', 'AR', 'DUMP', 'TK', 'tank', 'VN', 'Moped', 'trk', '2TON', 'fire', 'LIGHT', 'E-BIK', '15 PA', 'FDNY', 'NYC B', 'cat 3', 'UT', 'City', 'UNKNO', 'LIVERY VEHICLE', 'SCOOTER', 'DP', 'ST', 'TANK', 'sciss']

# # Vehicular Collisions

import pandas as pd

vehcol = pd.read_csv("vehicular_collisions.csv")
vehcol.head()
vehcol = vehcol.drop(vehcol.columns.values[0], axis=1)
vehcol.head()
vehcol = vehcol.set_index("NEW_DATE")
vehcol.head()
vehcol = vehcol.sort_index(axis = 0)
vehcol.head()
sorted(vehcol.columns)[0:6]

onehotlist = sorted(vehcol.columns)[6:]
onehotlist = [onehotlist[i][13:-1] for i in range(len(onehotlist))]
onehotlist

for i in range(len(onehotlist)):
    if len(onehotlist[i]) == 3:
        a = onehotlist[i].split("_")
        onehotlist[i] = a[0] + "_00" + a[1]
        #print(onehotlist[i])
    elif len(onehotlist[i]) == 4:
        a = onehotlist[i].split("_")
        onehotlist[i] = a[0] + "_0" + a[1]
        #print(onehotlist[i])
    else:
        pass
onehotlist

vehcol2 = vehcol.reindex_axis(sorted(vehcol.columns), axis=1)
vehcol2
vehcol2.columns.values
new_colnames = sorted(vehcol.columns)[0:6] + onehotlist
new_colnames
len(new_colnames)
vehcol2.shape
vehcol2.columns = new_colnames
vehcol3 = vehcol2.reindex_axis(sorted(vehcol2.columns), axis=1)
vehcol3.head()
new_colname_2 = veh_type_1 + veh_type_2 + boroughs
new_colname_2
vehcol3.columns = new_colname_2
vehcol3
# all zero column check
vehcol3.loc[:, (vehcol3 != 0).any(axis=0)].shape




