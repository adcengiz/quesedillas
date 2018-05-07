# merging 311 and vehicular collisions

merged_df = pd.merge(req311_new, vehcol3, 
                     left_on=req311_new.index.values, 
                     right_on=vehcol.index.values,
                     right_index= True,
                     left_index = True,
                     how = "inner")
merged_df.to_csv("311_vehcol.csv")
merged_df.head()

# merged_df.iloc[:,0].value_counts()[107]
# merged_df.iloc[:,0].value_counts()[0]

# sum(merged_df["Registration and Transfers"].values)
# merged_df["Registration and Transfers"].value_counts()[0] / merged_df.shape[0]

del_list = []
for i in range(merged_df.shape[1]):
    try:
        count_zero = merged_df.iloc[:,i].value_counts()[0]
        perc_zero = (count_zero / merged_df.shape[0])
        #print(perc_zero)
        if perc_zero > 0.0:
            del_list.append(merged_df.columns[i])
    except:
        pass
len(del_list)

merged_df_2 = merged_df.drop(del_list,axis=1)
merged_df_2.columns.values
merged_df_2["passenger_vehicle"] = merged_df_2["PASSENGER VEHICLE"].sum(axis = 1)
merged_df_2.head()
merged_df_2 = merged_df_2.drop("PASSENGER VEHICLE", axis = 1)
merged_df_2.to_csv("311_vehcol_2.csv")
# merged_df_2.columns[-6:]
# merged_df_2.columns[:-6]
