import requests
from joblib import Parallel, delayed
import pandas as pd
from datetime import datetime
from datetime import date
import numpy as np

#Function to Get Data by Entity
def GetData(param,s):
    ent_url = r'URL'+param+r'OUTPUT FORMAT'
    resp = s.get(ent_url, headers = headers).json()
    resp1 = resp['data']
    return resp1

print("Starting...")
params_url = r'URL'
headers = {'authorization': 'Token '}

print("Calling Get Query Params By Entity")
response = requests.get(params_url, headers = headers)
data = response.json()
params = data['data']

print("Removing Cancellation Index Query Strings")
params_to_use = [param for param in params if "Subscription+Index" in param]

print("Calling Get Data By Entity")
s = requests.Session()
#Use joblib to parallelize the API calls to increase performance
w = Parallel(n_jobs=-1,verbose=0)(delayed(GetData)(param,s) for param in params_to_use)

print("Extracting Data Within Date Range")
start_date = date(2017,9,1)
end_date = date(2017,9,30)

#Loop through nested jsons to get list of dictionaries with data for the
#desired time span
new_list = []
for i in range(len(w)):
    w_step = w[i]
    for j in range(len(w_step)):
        dict_vals = w[i][j]
        per_start = datetime.strptime(dict_vals['date'], '%Y-%m-%d').date()
        per_end = datetime.strptime(dict_vals['period_end'], '%Y-%m-%d').date()
        if start_date <= per_start <=end_date and start_date <= per_end <=end_date:
            new_list.append(dict_vals)
            
print("Converting Dictionaries To Dataframe And Summarizing")
dfDataCombined = pd.DataFrame(new_list)
#Add IsInternational Column to tag other countries. Puerto Rico is considered domestic
dfDataCombined['IsInternational'] = np.where(((dfDataCombined.country_name == 'United States of America') | (dfDataCombined.country_name == 'Puerto Rico')),'N','Y')
dfToSum = dfDataCombined[['country_name','entity_name','IsInternational','value']]
dfSummary = dfToSum.groupby(['country_name','entity_name','IsInternational']).sum().reset_index()
dfPerc = dfSummary[['IsInternational','value']]
dfPerc1 = dfPerc.groupby(['IsInternational']).sum().reset_index()
dfPerc1['Percentage'] = dfPerc1['value']/dfPerc1['value'].sum()
dfPercentages = dfPerc1[['IsInternational','Percentage']]

print(dfPercentages)

