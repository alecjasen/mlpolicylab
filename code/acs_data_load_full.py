#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import censusdata
import re
import subprocess
import shlex

# In[2]:


# received in email by filling out https://api.census.gov/data/key_signup.html
# took a while after 'activating' to actually become active
KEY = "569913f1c2bf1c28df9b1f0c05f120b8691ef91b"


# In[3]:


# pennsylvania is 42
state_dict = censusdata.geographies(censusdata.censusgeo([('state', '*'),]), 'acs5', 2015, key=KEY)


# In[4]:


state_dict_lookup = {v.geo[0][1]: k for k, v in state_dict.items()}
state_dict_lookup


# In[5]:


# allegheny is '003' for testing
county_dict = censusdata.geographies(censusdata.censusgeo([('state', '42'), ('county', '*')]), 'acs5', 2015, key=KEY)
sex_by_age_table_info_dict = censusdata.censustable('acs5', 2015, 'B01001')


# In[6]:


county_dict_lookup = {v.geo[1][1]: " ".join(k.split(',')[0].split(' ')[:-1]) for k, v in county_dict.items()}
county_dict_lookup


# In[7]:


# pennsylvania is 42

# get all county codes from above dictionary and loop through them with this
pa_state_code = '42'
all_age_ranges = ['B01001_{:03d}E'.format(i) for i in range(1,50)]
df_list = []
for county_code in county_dict_lookup:
    dlDF = censusdata.download('acs5', 
                           2018,
                           censusdata.censusgeo([('state', '42'), ('county', county_code), ('block group' , '*')]),
                           all_age_ranges,
                           key=KEY,
                          endpt='')

    new_cols = [re.sub("!!", "", re.sub(":", " ", sex_by_age_table_info_dict[col]['label'])) for col in all_age_ranges]

    sexageDF = dlDF.rename(columns = dict(zip(all_age_ranges, new_cols)))

    # https://www.census.gov/library/visualizations/2017/comm/voting-rates-age.html
    # combine 18-29, 30-44, 45-64, 65+ as age blocs of interest
    # print(sexageDF.columns)
    sexageDF['Male <18'] = sexageDF['Male Under 5 years'] + sexageDF['Male 5 to 9 years'] +                                sexageDF['Male 10 to 14 years'] + sexageDF['Male 15 to 17 years']
    sexageDF['Male 18-29'] = sexageDF['Male 18 and 19 years'] + sexageDF['Male 20 years'] +                                 sexageDF['Male 21 years'] + sexageDF['Male 22 to 24 years']
    sexageDF['Male 30-44'] = sexageDF['Male 30 to 34 years'] + sexageDF['Male 35 to 39 years'] +                                 sexageDF['Male 40 to 44 years']
    sexageDF['Male 45-64'] = sexageDF['Male 45 to 49 years'] + sexageDF['Male 50 to 54 years'] +                                 sexageDF['Male 55 to 59 years'] + sexageDF['Male 60 and 61 years'] +                                 sexageDF['Male 62 to 64 years']
    sexageDF['Male >=65'] = sexageDF['Male 65 and 66 years'] + sexageDF['Male 67 to 69 years'] +                                 sexageDF['Male 70 to 74 years'] + sexageDF['Male 75 to 79 years'] +                                 sexageDF['Male 80 to 84 years'] + sexageDF['Male 85 years and over']

    sexageDF['Female <18'] = sexageDF['Female Under 5 years'] + sexageDF['Female 5 to 9 years'] +                                sexageDF['Female 10 to 14 years'] + sexageDF['Female 15 to 17 years']
    sexageDF['Female 18-29'] = sexageDF['Female 18 and 19 years'] + sexageDF['Female 20 years'] +                                 sexageDF['Male 21 years'] + sexageDF['Female 22 to 24 years']
    sexageDF['Female 30-44'] = sexageDF['Female 30 to 34 years'] + sexageDF['Female 35 to 39 years'] +                                 sexageDF['Female 40 to 44 years']
    sexageDF['Female 45-64'] = sexageDF['Female 45 to 49 years'] + sexageDF['Female 50 to 54 years'] +                                 sexageDF['Female 55 to 59 years'] + sexageDF['Female 60 and 61 years'] +                                 sexageDF['Female 62 to 64 years']
    sexageDF['Female >=65'] = sexageDF['Female 65 and 66 years'] + sexageDF['Female 67 to 69 years'] +                                 sexageDF['Female 70 to 74 years'] + sexageDF['Female 75 to 79 years'] +                                 sexageDF['Female 80 to 84 years'] + sexageDF['Female 85 years and over']


    sexageDF[['Male <18', 'Female <18', 'Male 18-29', 'Female 18-29', 'Male 30-44', 'Female 30-44', 'Male 45-64', 'Female 45-64', 'Male >=65', 'Female >=65']]

    sexagelocDF = sexageDF.reset_index()
    sexagelocDF['state'] = sexagelocDF['index'].apply(lambda x: state_dict_lookup[x.geo[0][1]])
    sexagelocDF['county'] = sexagelocDF['index'].apply(lambda x: county_dict_lookup[x.geo[1][1]])
    sexagelocDF['tract'] = sexagelocDF['index'].apply(lambda x: x.geo[2][1])
    sexagelocDF['block_group'] = sexagelocDF['index'].apply(lambda x: x.geo[3][1])

    df_list.append(sexagelocDF[['state', 'county', 'tract', 'block_group', 'Male <18', 'Female <18', 'Male 18-29', 'Female 18-29', 'Male 30-44', 'Female 30-44', 'Male 45-64', 'Female 45-64', 'Male >=65', 'Female >=65']])


# In[8]:


all_pa = pd.concat(df_list)


# In[9]:


all_pa


# In[10]:


all_pa.to_csv('../data/age_sex_blockgrouplevel.csv', index=False, header=True)


# In[ ]:


args = shlex.split('psql -h mlpolicylab.db.dssg.io -U ajasen bills2_database -f acs_data_load.sql')
subprocess.run(args)

