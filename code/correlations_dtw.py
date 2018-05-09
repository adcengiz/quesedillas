
# coding: utf-8

# In[71]:


import pandas as pd
import numpy as np
from dtw import dtw
import matplotlib.pyplot as plt
get_ipython().magic('matplotlib inline')
get_ipython().magic('pylab inline')





# ### Bike & weather data


data=pd.read_csv('bike_weather2.csv', header=0)
#data['date']=pd.to_datetime(data['day_x']).apply(lambda x: x.date())
data['date']=data['day_x'].astype(str)
data=data[['date','Temp','Spd','Prcp','mean_duration','num_trips']]

data.head()


# ### Crime

# In[74]:


crime=pd.read_csv('crime_clean.csv')
crime['date']=crime['date'].astype(str).str.slice(0,10)
crime=crime.drop('Unnamed: 0',axis=1)
crime.head()


# ### 311 & Vehicle collisions

# In[75]:


colls=pd.read_csv('311_vehcol_2.csv')
colls['date']=colls['Unnamed: 0']
colls=colls.drop('Unnamed: 0',axis=1)
colls['date']=pd.to_datetime(colls['date'], format='%Y-%m-%d').apply(lambda x: x.date())
#colls=colls.set_index('date')
#colls=colls.sort_index()
colls.head()


# ### Taxi

# In[76]:


taxi=pd.read_csv('taxis_2011_2016.csv')
taxi=taxi.drop('Unnamed: 0',axis=1)
taxi['date']=taxi['tripyear'].astype(str)+"-"+taxi['tripmonth'].astype(str)+'-'+taxi['tripday'].astype(str)
taxi['date']=pd.to_datetime(taxi['date'], format='%Y-%m-%d').apply(lambda x: x.date())
taxi=taxi.rename(columns={'count':'taxi_count'})
taxi=taxi[['date','taxi_count']]
taxi.head()


# In[77]:


result=crime.merge(data,how='left',on='date')
result['year']=result['date'].str.slice(0,4)
result=result[result.year!='nan']
result=result.dropna(subset=['year'])

result['year']=result['year'].astype(int)
result=result[(result['year']>=2011)&(result['year']<=2017)]

result['date']=pd.to_datetime(result['date'], format='%Y-%m-%d').apply(lambda x: x.date())
result=result.drop('year',axis=1)
result=result.merge(colls,how='left',on='date')
result=result.merge(taxi,how='left',on='date')

result=result.set_index('date')
result=result.sort_index()

result.head()


# # Correlations

# In[78]:


result.shape


# In[79]:


result=result.dropna()
result.shape


# In[80]:


result=result.dropna()
result.to_csv('final_data.csv')
result.head()


# In[81]:


lcrime=['felonies','misdemeanors','violations']
lweather=['Temp','Spd','Prcp']
lbike=['mean_duration','num_trips']
lcolls= ['QUEENS', 'BROOKLYN', 'BRONX', 'MANHATTAN', 'STATEN ISLAND','passenger_vehicle']
l311=['Traffic Signal Condition', 'ELECTRIC', 'FLOORING/STAIRS',
       'Sanitation Condition', 'Illegal Parking', 'Noise - Residential',
       'Sewer', 'Noise - Commercial', 'Water System', 'Blocked Driveway',
       'Taxi Complaint', 'Sidewalk Condition', 'For Hire Vehicle Complaint',
       'Found Property', 'Noise', 'Street Light Condition', 'PLUMBING',
       'Street Sign - Damaged', 'Damaged Tree', 'Rodent', 'Derelict Vehicles',
       'Dirty Conditions', 'Indoor Air Quality']
data_all=[lcrime,lweather,lbike,lcolls]
crime=0
taxi=0
colls=0
data=0


# In[82]:


mycolumns=result.columns
from sklearn.preprocessing import MinMaxScaler
scaler=MinMaxScaler()
result=pd.DataFrame(scaler.fit_transform(result),columns=mycolumns)
result.head()


# In[83]:


def correlation_dtw(x_col, y_col):
    x=np.array(result[x_col]).reshape(-1, 1)
    y=np.array(result[y_col]).reshape(-1, 1)
    dist, cost, acc, path = dtw(x, y, 
                                lambda x, y: np.linalg.norm(x - y, ord=2))
   
    return dist
   


# In[26]:


allcolumns=result.columns
corrs={}
run=[]
i=0
for c1 in allcolumns:
    for c2 in allcolumns:
        vars_=sorted([c1,c2])

        
        if (c1 in lcrime) and (c2 in lcrime):
            pass
        elif c1 in lweather and c2 in lweather:
            pass
        elif c1 in lbike and c2 in lbike:
            pass
        elif c1 in lcolls and c2 in lcolls:
            pass
        elif c1 in l311 and c2 in l311:
            pass
        
        elif sorted([c1,c2]) in run:
            pass
        else:
            run.append(vars_)
            
run


# In[27]:


len(run)


# In[33]:


new_run = run[:150]
new_run


# In[84]:


allcolumns=result.columns
corrs={}
run=[]
i=0
for c1 in allcolumns:
    for c2 in allcolumns:
        vars_=sorted([c1,c2])

        
        if (c1 in lcrime) and (c2 in lcrime):
            pass
        elif c1 in lweather and c2 in lweather:
            pass
        elif c1 in lbike and c2 in lbike:
            pass
        elif c1 in lcolls and c2 in lcolls:
            pass
        elif c1 in l311 and c2 in l311:
            pass
        
        elif sorted([c1,c2]) in run:
            pass
        else:
            run.append(vars_)
            
            dist=correlation_dtw(x_col= c1, y_col =c2)
            name=c1+'_'+c2
            corrs[name]=dist
            if i%10==0:
                print(i)
            i+=1
corrs


# In[85]:


corrs


# In[96]:


correlations=pd.DataFrame.from_dict(corrs,orient='index')


# In[108]:


correlations.columns=['dtw']
correlations=correlations.sort_values(by='dtw',ascending=True)
correlations.to_csv('correlations_dtw.csv')
correlations.h


# ### Graph correlations

# In[109]:


def correlation_dtw_graph(x_col, y_col):
    x=np.array(result[x_col]).reshape(-1, 1)
    y=np.array(result[y_col]).reshape(-1, 1)
    dist, cost, acc, path = dtw(x, y, 
                                lambda x, y: np.linalg.norm(x - y, ord=2))
   
    plt.figure(3)
    plt.xlabel(x_col)
    plt.ylabel(y_col)
    imshow(acc.T, origin='lower', cmap='Reds', interpolation='nearest')
    print(dist)
    plt.plot(path[0], path[1], 'w')
    plt.savefig(x_col+'_'+y_col+'.jpg')
    plt.show()


# In[110]:


tograph=[['Prcp','Damaged Tree'],['Prcp','Sewer'],['Prcp','Water System'],
         ['Prcp','Street Sign - Damaged'],['mean_duration','Dirty Conditions'],
        ['mean_duration','Water System'],['Illegal Parking','passenger_vehicle'],
         ['misdemeanors','Found Property'],['violations','Noise']]


# In[111]:


for m in tograph:
     correlation_dtw_graph(x_col=m[0], y_col=m[1])
    

