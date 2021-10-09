#!/usr/bin/env python
# coding: utf-8

# In[75]:


import pandas as pd
import numpy as np
import matplotlib as plt
import datetime
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from sqlite3 import connect
import seaborn as sns


# In[20]:


def get_data(week_num):
    url='http://web.mta.info/developers/data/nyct/turnstile/turnstile_{}.txt'
    defs=[]
    for week in week_num :
        file_url=url.format(week)
        defs.append(pd.read_csv(file_url))
    return pd.concat(defs)


week_num = [210703,210710,210717,210724,210731,210807,210814,210821,210828,210904,210911,210918,210925]

Total_Months = get_data(week_num)
Total_Months.to_csv('three_m2021.csv',index=False ) 


# In[10]:


pwd


# In[21]:


engine=create_engine("sqlite:///mta_project_2020.db")


# In[25]:


connect=connect(":memory:")
Total_Months.to_sql('three_m2021',connect)
Total_Months=pd.read_sql('SELECT * FROM three_m2021',connect)
Total_Months


# In[26]:


Total_Months.shape


# In[27]:


Total_Months.describe().apply(lambda s: s.apply('{0:.1f}'.format))


# In[28]:


Total_Months.columns


# In[29]:


Total_Months.info()


# In[30]:


Total_Months.DATE.value_counts().sort_index()


# In[31]:


mask = ((Total_Months["C/A"] == "A002") &
        (Total_Months["UNIT"] == "R051") & 
        (Total_Months["SCP"] == "02-00-00") & 
        (Total_Months["STATION"] == "59 ST"))

Total_Months[mask].head()


# In[33]:


#change to timeSerises and make it in one col called [DATE_TIME]
Total_Months["DATE_TIME"] = pd.to_datetime(Total_Months.DATE+" "+Total_Months.TIME,
                                                  format="%m/%d/%Y %H:%M:%S")


# In[34]:


#Check how many Entries i have at sepcific hour if its more 1 i have drop duplicated entries
(Total_Months
 .groupby(["C/A", "UNIT", "SCP", "STATION", "DATE_TIME"])
 .ENTRIES.count()
 .reset_index()
 .sort_values("ENTRIES", ascending=False)).head(5)


# In[35]:


mask = ((Total_Months["C/A"] == "S101") & 
(Total_Months["UNIT"] == "R070") & 
(Total_Months["SCP"] == "00-00-02") & 
(Total_Months["STATION"] == "ST. GEORGE") &
(Total_Months["DATE_TIME"].dt.date == datetime.datetime(2021,9,16).date()))
Total_Months[mask].head()


# In[36]:


mask = ((Total_Months["C/A"] == "A006") &
        (Total_Months["UNIT"] == "R079") & 
        (Total_Months["SCP"] == "00-03-02") & 
        (Total_Months["STATION"] == "5 AV/59 ST"))

Total_Months[mask].head()


# In[37]:


# duplicate entry
Total_Months.sort_values(["C/A", "UNIT", "SCP", "STATION", "DATE_TIME"], 
                          inplace=True, ascending=False)
Total_Months.drop_duplicates(subset=["C/A", "UNIT", "SCP", "STATION", "DATE_TIME"], inplace=True)


# In[38]:



#Take a look on after droping the duplicate 
mask = ((Total_Months["C/A"] == "R504") & 
(Total_Months["UNIT"] == "R276") & 
(Total_Months["SCP"] == "00-00-01") & 
(Total_Months["STATION"] == "VERNON-JACKSON") &
(Total_Months["DATE_TIME"].dt.date == datetime.datetime(2021, 7, 31).date()))
Total_Months[mask].head()


# In[39]:


(Total_Months
 .groupby(["C/A", "UNIT", "SCP", "STATION", "DATE_TIME"])
 .ENTRIES.count()
 .reset_index()
 .sort_values("ENTRIES", ascending=False)).head(5)


# In[40]:


# Drop Exits and Desc Column.
Three_Months = Total_Months.drop(["EXITS", "DESC"], axis=1, errors="ignore")


# In[41]:


turnstiles_daily = (Three_Months
                        .groupby(["C/A", "UNIT", "SCP", "STATION", "DATE"],as_index=False)
                        .ENTRIES.first())


# In[42]:



turnstiles_daily.head()


# In[43]:



Three_Months[(Three_Months["C/A"] == "A011") & 
(Three_Months["UNIT"] == "R080") & 
(Three_Months["SCP"] == "01-00-00") & 
(Three_Months["STATION"] == "57 ST-7 AV") &
(Three_Months["DATE"] == "07/03/2021")]


# In[44]:


turnstiles_daily = (Three_Months
                        .groupby(["C/A", "UNIT", "SCP", "STATION", "DATE"],as_index=False)
                        .ENTRIES.first())


# In[45]:


turnstiles_daily.head()


# In[46]:


turnstiles_daily[["PREV_DATE", "PREV_ENTRIES"]] = (turnstiles_daily
                                                   .groupby(["C/A", "UNIT", "SCP", "STATION"])["DATE", "ENTRIES"]
                                                   .apply(lambda grp: grp.shift(1)))


# In[47]:


turnstiles_daily.head()


# In[48]:


turnstiles_daily.dropna(subset=["PREV_DATE","PREV_ENTRIES"]
                                            ,axis=0,inplace=True)


# In[49]:


turnstiles_daily.head()


# In[50]:



turnstiles_daily[turnstiles_daily["ENTRIES"] < turnstiles_daily["PREV_ENTRIES"]].head()


# In[51]:


# What's the deal with counter being in reverse
mask = ((Three_Months["C/A"] == "A011") & 
(Three_Months["UNIT"] == "R080") & 
(Three_Months["SCP"] == "01-00-00") & 
(Three_Months["STATION"] == "57 ST-7 AV") &
(Three_Months["DATE_TIME"].dt.date == datetime.datetime(2021, 7, 3).date()))
Three_Months[mask].head()


# In[52]:


# Let's see how many stations have this problem

(turnstiles_daily[turnstiles_daily["ENTRIES"] < turnstiles_daily["PREV_ENTRIES"]]
    .groupby(["C/A", "UNIT", "SCP", "STATION"])
    .size())


# In[53]:


def get_daily_counts(row, max_counter):
    counter = row["ENTRIES"] - row["PREV_ENTRIES"]
    if counter < 0:
        # Maybe counter is reversed?
        counter = -counter
    if counter > max_counter:
        # Maybe counter was reset to 0? 
        print(row["ENTRIES"], row["PREV_ENTRIES"])
        counter = min(row["ENTRIES"], row["PREV_ENTRIES"])
    if counter > max_counter:
        # Check it again to make sure we're not still giving a counter that's too big
        return 0
    return counter

# If counter is > 1Million, then the counter might have been reset.  
# Just set it to zero as different counters have different cycle limits
# It'd probably be a good idea to use a number even significantly smaller than 1 million as the limit!
turnstiles_daily["DAILY_ENTRIES"] = turnstiles_daily.apply(get_daily_counts, axis=1, max_counter=1000000)


# In[54]:


ca_unit_station_daily = turnstiles_daily.groupby(["C/A", "UNIT", "STATION", "DATE"])[['DAILY_ENTRIES']].sum().reset_index()
ca_unit_station_daily.head()


# In[55]:


#daily time series for each STATION, by adding up all the turnstiles in a station.

station_daily = turnstiles_daily.groupby(["STATION", "DATE"])[['DAILY_ENTRIES']].sum().reset_index()
station_daily.head()


# In[56]:


#sum total ridership for each multiple weeks
#with the highest traffic during the time you investigate


station_totals = station_daily.groupby('STATION').sum().sort_values('DAILY_ENTRIES', ascending=False).reset_index()

station_totals.head()


# In[57]:



single_turnstile = turnstiles_daily[(turnstiles_daily["C/A"] == "A011") & 
(turnstiles_daily["UNIT"] == "R080") & 
(turnstiles_daily["SCP"] == "01-00-00") & 
(turnstiles_daily["STATION"] == "57 ST-7 AV")]

single_turnstile.head()


# In[70]:


plt.figure(figsize=(60,20))
plt.plot(single_turnstile['DATE'], single_turnstile['DAILY_ENTRIES'])
plt.ylabel('# of Entries')
plt.xlabel('Date')
plt.xticks(rotation=45)
plt.title('Daily Entries for Turnstile A011/R080/01-00-00 at 57 ST-7 AV Station')


# In[59]:


#Plot the daily time series for a station

station_daily_57_av = station_daily[station_daily['STATION'] == '57 ST-7 AV']
station_daily_57_av.head()


# In[60]:


plt.figure(figsize=(100,40))
plt.plot(station_daily_57_av['DATE'], station_daily_57_av['DAILY_ENTRIES'])
plt.ylabel('# of Entries')
plt.xlabel('Date')
plt.xticks(rotation=45)
plt.title('Daily Entries for 57 ST-7 AV Station')


# In[62]:


for i, group in station_daily_57_av.groupby('WEEK_OF_YEAR'): 
    plt.plot(group['DAY_OF_WEEK_NUM'], group['DAILY_ENTRIES'])
    if i == 29:
        break;
        
plt.xlabel('Day of the week')
plt.ylabel('Number of turnstile entries')
plt.xticks(np.arange(7),['Mo','Tu','We','Th','Fr','St','Sn'])
plt.title('Ridership per day for 57 ST-7 AV station')


# In[63]:


plt.hist(station_totals['DAILY_ENTRIES'])


# In[64]:


#Viewing top ten stations

plt.figure(figsize=(10,5))
plt.bar(x=station_totals['STATION'][:10], height=station_totals['DAILY_ENTRIES'][:10])
plt.xticks(rotation=90);
sns.barplot(data = mta2_station, y = mta2_station.STATION, x = mta2_station.TRAFFIC)

