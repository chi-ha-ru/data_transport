import pandas as pd
import csv
import re

covidDF = pd.read_csv('COVID_county_data.csv')
censusDF = pd.read_csv('acs2017_census_tract_data.csv')

#print(censusDF.columns)

#A. Transform the ACS Data

#Comment out quotation marks to create new dataframe to be saved in csv file
#After that uncomment quotation marks to read from csv file
''''
#Aggregate census tract-level data into county-level summaries
countyDF = pd.read_csv('acs2017_census_tract_data.csv',usecols=['State','County','TotalPop','Poverty','IncomePerCap'])
state_county = countyDF[['State','County']]
state_county = state_county.drop_duplicates()
print(state_county)

state_list = state_county['State'].tolist()
county_list = state_county['County'].tolist()

#print(countyDF)
County_info = pd.DataFrame(columns = ['ID','County','Population','% poverty','PerCapitaIncome'])

for i in range(len(state_list)):
   temp = countyDF.loc[(countyDF['County'] == county_list[i]) & (countyDF['State'] == state_list[i] )]
   pop = temp['TotalPop'].sum()
   pov = (temp['Poverty'] * temp['TotalPop']).sum()/pop
   inc = (temp['IncomePerCap']*temp['TotalPop']).sum()/pop
   
   new_row = {'ID': i, 'County':county_list[i] + ', ' + state_list[i], 'Population':pop,'% poverty':pov,'PerCapitaIncome':inc}
   County_info = County_info.append(new_row,ignore_index=True)
  

print(County_info)
'''
# Save to csv file
#County_info.to_csv('County_info.csv',index=False)

# Read from csv file
County_info = pd.read_csv('County_info.csv')


print(County_info.loc[County_info['County'] == 'Loudoun County, Virginia'])
print(County_info.loc[County_info['County'] == 'Washington County, Oregon'])
print(County_info.loc[County_info['County'] == 'Harlan County, Kentucky'])
print(County_info.loc[County_info['County'] == 'Malheur County, Oregon'])

print(County_info.loc[County_info['Population'] == County_info['Population'].max()]) 
print(County_info.loc[County_info['Population'] == County_info['Population'].min()]) 



#B Transform the COVID Data


#Comment out quotation marks to create new dataframe to be saved to csv file
#After that, uncomment quotation marks to read from csv file
'''
#print(covidDF.columns)
#print(covidDF)

# new table
columns = ['ID','County','Month','# cases','# deaths']

# Removes day. Changes yyyy-mm--dd to yyyy-mm
def tidy_date(item):
   retval = re.sub(r'-\d\d$','',item)
   return retval

dateDF = covidDF[['date']].applymap(tidy_date)

# Create covid dataframe with updated date format
updatedcovidDF = pd.DataFrame().assign(date=dateDF['date'],county=covidDF['county'], state=covidDF['state'], fips=covidDF['fips'], cases=covidDF['cases'], deaths=covidDF['deaths'])
#print(updatedcovidDF)

#create search list
search_list = updatedcovidDF[['date','county','state']]
search_list = search_list.drop_duplicates()
#print(search_list)

date_list = search_list['date'].tolist()
state_list = search_list['state'].tolist()
county_list = search_list['county'].tolist()

# Create new dataframe with row size index
index=range(0,len(search_list))
COVID_monthly = pd.DataFrame(index=index,columns = ['ID','County','Month','# cases','# deaths'])

for i in range(len(search_list)):
   # returns table of all rows where given county and state match given date in search list
   temp = updatedcovidDF[(updatedcovidDF['county'] == county_list[i]) & (updatedcovidDF['state'] == state_list[i]) & (updatedcovidDF['date'] == date_list[i])]

   cases = temp['cases'].sum()
   deaths = temp['deaths'].sum()
   county = county_list[i] + ' County, ' + state_list[i]
   id = County_info[(County_info['County'] == county)]['ID'].values

   COVID_monthly.iloc[i].loc['ID'] = id
   COVID_monthly.iloc[i].loc['County'] = county
   COVID_monthly.iloc[i].loc['Month'] = date_list[i]
   COVID_monthly.iloc[i].loc['# cases'] = cases
   COVID_monthly.iloc[i].loc['# deaths'] = deaths

print(COVID_monthly)
'''
# Save to csv file
#COVID_monthly.to_csv('COVID_monthly.csv',index=False)

# Read from csv file
COVID_monthly = pd.read_csv('COVID_monthly.csv')


print(COVID_monthly.loc[(COVID_monthly['County'] == 'Malheur County, Oregon') & (COVID_monthly['Month'] == '2020-02')])
print(COVID_monthly.loc[(COVID_monthly['County'] == 'Malheur County, Oregon') & (COVID_monthly['Month'] == '2020-08')])
print(COVID_monthly.loc[(COVID_monthly['County'] == 'Malheur County, Oregon') & (COVID_monthly['Month'] == '2021-01')])


#C Integrate COVID Data with ACS Data

#Comment out quotation marks to create new dataframe which will be saved in csv file
#After that uncomment quotation marks to read from csv file
'''
#index range for new dataframe
index=range(0,len(County_info))

# create empty dataframe to size index range
COVID_summary = pd.DataFrame(index=index,columns = ['ID','Population','% Poverty','PerCapitaIncome', 'TotalCases','TotalDeaths', 'TotalCasesPer100K', 'TotalDeathsPer100K'])

for j in range(len(County_info)):
   temp = COVID_monthly[(COVID_monthly['County'] == County_info.iloc[j].loc['County'])]

   id = j
   pop = County_info.iloc[j].loc['Population']
   pov = County_info.iloc[j].loc['% poverty']
   pci = int(County_info.iloc[j].loc['PerCapitaIncome'])
   cases = temp['# cases'].sum()
   deaths = temp['# deaths'].sum()
   tcp100k = cases/(pop/100000)
   tdp100k = deaths/(pop/100000)
   
   COVID_summary.iloc[j,0] = id
   COVID_summary.iloc[j,1] = pop
   COVID_summary.iloc[j,2] = pov
   COVID_summary.iloc[j,3] = pci
   COVID_summary.iloc[j,4] = cases
   COVID_summary.iloc[j,5] = deaths
   COVID_summary.iloc[j,6] = tcp100k
   COVID_summary.iloc[j,7] = tdp100k
   
print(COVID_summary)
'''
# Save to csv file
#COVID_summary.to_csv('COVID_summary.csv',index=False)

# Read from csv file
COVID_summary = pd.read_csv('COVID_summary.csv')

c_info = County_info.loc[(County_info['County'] == 'Washington County, Oregon')]
print(COVID_summary.loc[(COVID_summary['ID'].values == c_info['ID'].values)])

c_info = County_info.loc[(County_info['County'] == 'Malheur County, Oregon')]
print(COVID_summary.loc[(COVID_summary['ID'].values == c_info['ID'].values)])

c_info = County_info.loc[(County_info['County'] == 'Loudoun County, Virginia')]
print(COVID_summary.loc[(COVID_summary['ID'].values == c_info['ID'].values)])

c_info = County_info.loc[(County_info['County'] == 'Harlan County, Kentucky')]
print(COVID_summary.loc[(COVID_summary['ID'].values == c_info['ID'].values)])



#D. Analysis

#1.Compute the correlation coefficient for the following relationships for all Oregon counties

#finds all counties from Oregon
oregon_county = County_info[County_info['County'].str.contains(', Oregon')]
#print(oregon_county)

#create dataframe for total cases and total deaths
oregon_covid = pd.DataFrame(columns =['TotalCases','TotalDeaths'])

def tidy_totals(item):
   retval = re.sub(r'\n.*','',item)           #remove everything after \n
   retval = re.sub(r'(\d+)(\s+)','',retval)   #remove sequence of numbers followed by spaces
   return int(float(retval))

for i in range(len(oregon_county)):
    temp = COVID_summary[(COVID_summary['ID'] == oregon_county.iloc[i].loc['ID'])]

    # tidying data
    totalcases = tidy_totals(temp['TotalCases'].to_string())
    totaldeaths = tidy_totals(temp['TotalDeaths'].to_string())
    
    # add new data to table
    new_row = {'TotalCases':totalcases,'TotalDeaths':totaldeaths}
    oregon_covid = oregon_covid.append(new_row,ignore_index=True)


# Oregon counties
oregon = pd.DataFrame(index=range(len(oregon_county)), columns=['Population','% Poverty','PerCapitaIncome', 'TotalCases', 'TotalDeaths'])

for i in range(len(oregon_county)):
  oregon.iloc[i].loc['Population'] = oregon_county.iloc[i].loc['Population']
  oregon.iloc[i].loc['% Poverty'] = oregon_county.iloc[i].loc['% poverty']
  oregon.iloc[i].loc['PerCapitaIncome'] = oregon_county.iloc[i].loc['PerCapitaIncome']
  oregon.iloc[i].loc['TotalCases'] = oregon_covid.iloc[i].loc['TotalCases']
  oregon.iloc[i].loc['TotalDeaths'] = oregon_covid.iloc[i].loc['TotalDeaths']
  
#print(oregon)

# USA counties
usa = pd.DataFrame().assign(Population=County_info['Population'],  Poverty=County_info['% poverty'], PerCapitaIncome=County_info['PerCapitaIncome'], TotalCases=COVID_summary['TotalCases'], TotalDeaths=COVID_summary['TotalDeaths'])



#correlation coeffient for Oregon counties
print('Oregon counties')
print(oregon.astype('float64').corr(method='pearson'))
R = oregon['TotalCases'].astype('float64').corr(oregon['% Poverty'].astype('float64'))
print('R for covid total cases vs % population in poverty:',R)
R = oregon['TotalDeaths'].astype('float64').corr(oregon['% Poverty'].astype('float'))
print('R for Covid total deaths vs % population in poverty:',R)
R = oregon['TotalCases'].astype('float64').corr(oregon['PerCapitaIncome'].astype('float64'))
print('R for covid total cases vs per capita income level:', R)

R = oregon['TotalDeaths'].astype('float64').corr(oregon['PerCapitaIncome'].astype('float'))
print('R for total deaths vs per capita income level:', R)

# correlation coefficient for all USA counties
print('USA counties')

print(usa.astype('float64').corr(method='pearson'))
R = usa['TotalCases'].astype('float64').corr(usa['Poverty'].astype('float64'))
print('R for covid total cases vs population:',R)
R = usa['TotalDeaths'].astype('float64').corr(usa['Poverty'].astype('float'))
print('R for Covid total deaths vs % population in poverty:',R)
R = usa['TotalCases'].astype('float64').corr(usa['PerCapitaIncome'].astype('float64'))
print('R for covid total cases vs per capita income level:', R)

R = usa['TotalDeaths'].astype('float64').corr(usa['PerCapitaIncome'].astype('float'))
print('R for total deaths vs per capita income level:', R)

#Other correlation
R = usa['TotalDeaths'].astype('float64').corr(usa['Population'].astype('float'))
print('R for total deaths vs population:', R)
