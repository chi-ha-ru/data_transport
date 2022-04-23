import pandas as pd
import csv
import re

df = pd.read_csv('Oregon Hwy 26 Crash Data for 2019 - Crashes on Hwy 26 during 2019.csv',index_col=False)

# SPLIT THE ODOT DATA INTO THREE DATAFRAMES 
CrashesDF = df[df['Record Type'] == 1]
VehiclesDF = df[df['Record Type'] == 2]
ParticipantsDF = df[df['Record Type'] == 3]

CrashesDF = CrashesDF.dropna(axis=1, how='all')
VehiclesDF = VehiclesDF.dropna(axis=1, how='all')
ParticipantsDF = ParticipantsDF.dropna(axis=1, how='all')

# EVERY CRASH OCCURED ON A DATE
print('Existence assertion: Every crash occured on a date')
CrashDateDF = CrashesDF[['Crash Month','Crash Day','Crash Year']]
if CrashDateDF.notnull().values.any():
   print('Assertion validated')
else:
   print('Assertion violated')
print(CrashDateDF)

# EVERY CRASH HAPPENED ON HIGHWAY NUMBER 26
print('Existence assertion: Every crash happend on Highway Number 26')
HighwayDF = CrashesDF[['Highway Number']]
if (HighwayDF['Highway Number'] == 26).all():
  print('Assertion validated')
else:
  print('Assertion violated')
print(HighwayDF)


# EVERY CRASH OCCURED DURING YEAR 2019
print('Limit assertion: Every crash occured during year 2019')
YearDF = CrashesDF[['Crash Year']]

if (YearDF['Crash Year'] == 2019).all():
  print('Assertion validated')
else:
  print('Assertion violated')
print(YearDF)


# AGES OF PARTICIPANTS IS BETWEEN 0 AND 120
print('Limit assertion: Every participant\'s age is a number between 0 and 120')
AgesDF = ParticipantsDF[['Age']]
if AgesDF['Age'].between(0,120,inclusive=True).any():
  if AgesDF['Age'].isnull().values.any():
     AssertionViolation = AgesDF[AgesDF['Age'].isnull()]
     print('Assertion violated')
     print(AssertionViolation)
  else:
    print('Assertion validated')
    print(AgesDF)
else:
  print('Assertion violated')
  AssertionViolation = AgesDF[~AgesDF['Age'].isin(range(0,120))].dropna(axis=1)  

# RESOLVE VIOLATION
ParticipantsDF['Age'] = ParticipantsDF['Age'].fillna(0.0)
print('\nReplaced nan values with 0.0  ')
print('Limit assertion: Every participant\'s age is a number between 0 and 120')
AgesDF = ParticipantsDF[['Age']]
if AgesDF['Age'].between(0,120,inclusive=True).any():
  if AgesDF['Age'].isnull().values.any():
     AssertionViolation = AgesDF[AgesDF['Age'].isnull()]
     print('Assertion violated')
     print(AssertionViolation)
  else:
    print('Assertion validated')
    print(AgesDF)
else:
  print('Assertion violated')
  AssertionViolation = AgesDF[~AgesDF['Age'].isin(range(0,120))].dropna(axis=1)  



# EVERY CRASH HAS A UNIQUE ID
print('Intra-record assertion: Every crash has a unique ID')
CrashIdDF = CrashesDF[['Crash ID']]
if len(CrashIdDF['Crash ID'].unique()) == len(CrashIdDF):
  print('Assertion validated')
  print(CrashIdDF)
else:
  print('Assertion violated')

# EVERY PARTICIPANT IN A CRASH HAS A UNIQUE ID
print('Intra-record assertion: Every participant in a crash has a unique ID')
ParticipantIdDF = ParticipantsDF[['Participant ID']]
if len(ParticipantIdDF['Participant ID'].unique()) == len(ParticipantIdDF):
  print('Assertion validated')
  print(ParticipantIdDF)
else:
  print('Assertion violated')


# EVERY VEHICLE IN THE CRASH DATA WAS PART OF A KNOWN CRASH
print('Inter-record assertion: Every vehicle in the crash data was part of a known crash')
UniqueVehicleCrashIdDF = VehiclesDF[['Crash ID']].drop_duplicates()
UniqueVehicleCrashIdResetIndexDF = UniqueVehicleCrashIdDF.reset_index(drop=True)

UniqueCrashIdDF = CrashesDF[['Crash ID']]
UniqueCrashIdResetIndexDF = UniqueCrashIdDF.reset_index(drop=True)

if UniqueVehicleCrashIdResetIndexDF['Crash ID'].equals(UniqueCrashIdResetIndexDF['Crash ID']):
  print('Assertion validated')
  print(UniqueVehicleCrashIdResetIndexDF)
  print(UniqueCrashIdResetIndexDF)
else:
  print('Assertion violated')


# EVERY VEHICLE IN THE CRASH DATA HAD AN OCCUPANT
print('Inter-record assertion: Every vehicle in the crash data had an occupant')
ParticipantVehicleDF = ParticipantsDF[['Vehicle ID']].drop_duplicates()
ParticipantVehicleResetIndexDF = ParticipantVehicleDF.reset_index(drop=True)

VehicleDF = VehiclesDF[['Vehicle ID']].drop_duplicates()
VehicleResetIndexDF = VehicleDF.reset_index(drop=True)

print(ParticipantVehicleResetIndexDF)
print(VehicleResetIndexDF)

if ParticipantVehicleResetIndexDF['Vehicle ID'].equals(VehicleResetIndexDF['Vehicle ID']):
  print('Assertion validated')
  print(ParticipantVehicleResetIndexDF)
  print(VehicleResetIndexDF)
else:
  print('Assertion violated')


# THERE WERE HUNDREDS OF CRASHSES BUT NOT THOUSANDS
print('\nSummary assertion: There were hundreds of crashes but not thousands')
if len(CrashesDF) < 999:
   print('Assertion validated')
else:
  print('Assertion violated')
print('Number of crashes:',len(CrashesDF))

# THERE WERE LESS THAN 100 FATAL INJURIES
print('\nSummary assertion: There were less than 100 fatal injuries')
FatalInjuriesDF = CrashesDF[['Total Fatality Count']]

if FatalInjuriesDF['Total Fatality Count'].sum() < 100:
  print('Assertion validated')
else:
  print('Assertion violated')
print('Number of fatal injuries:',FatalInjuriesDF['Total Fatality Count'].sum())

# CRASHES ARE EVENLY/UNIFORMLY DISTRIBUTED THROUGHOUT THE MONTHS OF THE YEAR
print('\nStatistical assertion: Crashes are evenly/uniformly distributed throughout the months of the year')
CrashDistribution = CrashesDF[['Crash Month']]
CrashDistribution = CrashDistribution.groupby(['Crash Month']).size()
result = 'true'

for i in CrashDistribution:
  if i != CrashDistribution[1]:  
    result = 'false'
if result == 'true':
  print('Assertion validated')
else:
  print('Assertion violated')
print(CrashDistribution)

# RESOLVE VIOLATION
print('\nRevised assertion')
print('\nStatistical assertion: Crashes are not evenly/uniformly distributed throughout the months of the year')
CrashDistribution = CrashesDF[['Crash Month']]
CrashDistribution = CrashDistribution.groupby(['Crash Month']).size()
result = 'true'

for i in CrashDistribution:
  if i != CrashDistribution[1]:  
    result = 'false'
if result == 'false':
  print('Assertion validated')
else:
  print('Assertion violated')
print(CrashDistribution)




# MOST CRASHES OCCUR DURING WEEKENDS
print('\nStatistical distribution assertions: More crashes occur during weekend days compared to week days')
DayOfWeekCrashesDF = CrashesDF[['Week Day Code']]
DayOfWeekCrashesDF = DayOfWeekCrashesDF.groupby(['Week Day Code']).size()

Sunday = DayOfWeekCrashesDF[1]
Saturday = DayOfWeekCrashesDF[7]
result = 'true'
count = 1
for i in DayOfWeekCrashesDF:
  if (count != 1) and (count != 7):  
    if (Sunday <= i) or (Saturday <= i):
      result = 'false'
  count += 1
 
if result == 'true':
  print('Assertion validated')
else:
  print('Assertion violated')
print(DayOfWeekCrashesDF)

# RESOLVE VIOLATIONS
print('\nRevised assertion')
print('\nStatistical distribution assertions: More crashes occur during Fridays and Saturdays compared to other days of the week ')
DayOfWeekCrashesDF = CrashesDF[['Week Day Code']]
DayOfWeekCrashesDF = DayOfWeekCrashesDF.groupby(['Week Day Code']).size()

Friday = DayOfWeekCrashesDF[6]
Saturday = DayOfWeekCrashesDF[7]
result = 'true'
count = 1
for i in DayOfWeekCrashesDF:
  if (count != 6) and (count != 7):  
    if (Friday <= i) or (Saturday <= i):
      result = 'false'
  count +=1
 
if result == 'true':
  print('Assertion validated')
else:
  print('Assertion violated')
print(DayOfWeekCrashesDF)

