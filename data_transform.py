import pandas as pd
import csv
import re
import numpy as np

df = pd.read_csv('books.csv')

print(df)
#A
# Use drop() method
df1 = df.drop(['Edition Statement','Corporate Author', 'Corporate Contributors','Former owner','Engraver','Issuance type','Shelfmarks'], axis=1)

print(df1)

# Use usecols
df2 = pd.read_csv('books.csv', usecols = ['Edition Statement', 'Corporate Author','Corporate Contributors', 'Former owner','Engraver','Issuance type','Shelfmarks'])

print(df2)

#B

df3 = pd.read_csv('books.csv', usecols = ['Date of Publication'],index_col=False)
print(df3.head(30))
print(type(df3.loc[7]['Date of Publication']))

def tidy_dates(item):
  retval = re.sub('[\[]','',str(item))                # remove [ 
  retval = re.sub('[\]]','',retval)                   # remove ]
  retval = re.sub('[/\sa-zA-Z,.-].*','',retval)       # remove / , spaces, letters, . , - 
  retval = re.sub('[\d].*\?$','',retval)              # remove numbers , ? , 

  if retval == '' or retval == 'nan':
    return np.nan
  else:
    return retval

df4 = df3
for i in range(len(df3)):
  df4.iloc[i]['Date of Publication'] = tidy_dates(df3.iloc[i]['Date of Publication'])

print(df4.head(30))
print(type(df4.iloc[7]['Date of Publication']))
df4['Date of Publication'] = pd.to_numeric(df4['Date of Publication'])
print(type(df4.iloc[7]['Date of Publication']))


# Place of Publication
df5 = pd.read_csv('books.csv', usecols = ['Place of Publication'],index_col=False)
print(df5.head(50))
print(df5.loc[150:200])

def tidy_places(item):
  
  retval = re.sub('[\[]','',item)
  retval = re.sub('[\]]','',retval)
  retval = re.sub('([,].*)','',retval)
  retval = re.sub('[;].*','',retval)
  retval = re.sub('[\d]','',retval)
  retval = re.sub('^([a-zA-Z\s\.].*:)','',retval)
  retval = re.sub('[A-Z]\..*','',retval)            
  retval = re.sub('\?','',retval)
  if retval == '' or retval[0].islower():
    retval = 'unknown'
  
  return retval


df6 = df5
for i in range(len(df5)):
  df6.iloc[i]['Place of Publication'] = tidy_places(df5.iloc[i]['Place of Publication'])

print(df6.head(50))

print(df6.loc[150:200])


# C. Tidying with applymap()
df7 = pd.read_csv('uniplaces.txt',sep='\n',names=['State','City','University'])
print(df7.head(20))
df7 = df7.replace(np.nan,'',regex=True)
print(df7.head(20))

def tidy_university(item):
  match = re.search(r'\([\w].*\)',item)       #if anything matches between ( )
  retval = re.sub(r'.*\[edit\]','',item)      # remove state name that precedes [edit]
  if match:
    retval = re.sub(r'[\w\(].*\)',match.group(),retval)    # replace everything with match
    return re.sub(r'[\(\)]','',retval)                     # remove ( ) 
  else:
    return re.sub(r'[\w].*\(','',retval)                   # return empty string


#copy all values in ['State'] column to ['City'] and ['University'] columns
df7.loc[:,'City'] = df7['State']
df7.loc[:,'University'] = df7['State']
print(df7.head(10))



df8 = df7[['University']].copy()
print(df8)
df8 = df8.applymap(tidy_university)
print(df8.head(10))

def tidy_city(item):
  retval = re.sub(r'\([\w].*\)','',item)
  retval = re.sub(r'.*\[edit\]','',retval)
  return retval

df9 =  df7[['City']].copy()
df9 = df9.applymap(tidy_city)
print(df9.head(10))

def tidy_state(item):
  match = re.search(r'[\w].*\[edit\]',item)
  if match:
    return re.sub('\[edit\]','',item)
  else:
    return re.sub(r'[\w\(\)].*','',item)

df10 = df7[['State']].copy()
df10 = df10.applymap(tidy_state)
print(df10.head(10))    

df7['State'] = df10['State']
df7['City'] = df9['City']
df7['University'] = df8['University']
print(df7.head(10))

def tidy_artifact(item):
  retval = re.sub('\[[\d]*\]','',item)    # remove artifacts
  return retval 


df7 = df7.applymap(tidy_artifact)
print(df7.head(50))


# D. Decoding

DecodeDF = pd.read_csv('trimet_03411_2.csv')
print(DecodeDF['OCCURRENCES'].loc[30:50])
print(DecodeDF['OCCURRENCES'].sum())

ExplodeDF = DecodeDF.loc[DecodeDF.index.repeat(DecodeDF['OCCURRENCES'])].reset_index(drop=True)
print(ExplodeDF['OCCURRENCES'].loc[30:50])

# E. Filling

FillDF = DecodeDF.copy() 
print(FillDF['VALID_FLAG'].loc[160:180])
FillDF['VALID_FLAG'] = FillDF['VALID_FLAG'].ffill()
print(FillDF['VALID_FLAG'].loc[160:180])

# F. Interpolating

InterpolateDF = DecodeDF.copy()
print(InterpolateDF['ARRIVE_TIME'].loc[130:150])
InterpolateDF['ARRIVE_TIME'] = InterpolateDF['ARRIVE_TIME'].interpolate(method='linear',limit_direction='forward',axis=0)
print(InterpolateDF['ARRIVE_TIME'].loc[130:150])

# G. More Transformations



