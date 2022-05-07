# this program loads Census ACS data using basic, slow INSERTs
# run it with -h to see the command line options
import time
import psycopg2
import argparse
import re
import csv
import psycopg2.extras

DBname = "postgres"
DBuser = "postgres"
DBpwd = "password"
TableName = 'CensusData'
Datafile = ""  # name of the data file to be loaded
CreateDB = True  # indicates whether the DB table should be (re)-created

def row2vals(row):
    for key in row:
        if not row[key]:
            row[key] = 0  # ENHANCE: handle the null vals
        row['County'] = row['County'].replace('\'','')  # TIDY: eliminate quotes within literals
    ret = f"""
       {row['CensusTract']},            -- CensusTract
       '{row['State']}',                -- State
       '{row['County']}',               -- County
       {row['TotalPop']},               -- TotalPop
       {row['Men']},                    -- Men
       {row['Women']},                  -- Women
       {row['Hispanic']},               -- Hispanic
       {row['White']},                  -- White
       {row['Black']},                  -- Black
       {row['Native']},                 -- Native
       {row['Asian']},                  -- Asian
       {row['Pacific']},                -- Pacific
       {row['Citizen']},                -- Citizen
       {row['Income']},                 -- Income
       {row['IncomeErr']},              -- IncomeErr
       {row['IncomePerCap']},           -- IncomePerCap
       {row['IncomePerCapErr']},        -- IncomePerCapErr
       {row['Poverty']},                -- Poverty
       {row['ChildPoverty']},           -- ChildPoverty
       {row['Professional']},           -- Professional
       {row['Service']},                -- Service
       {row['Office']},                 -- Office
       {row['Construction']},           -- Construction
       {row['Production']},             -- Production
       {row['Drive']},                  -- Drive
       {row['Carpool']},                -- Carpool
       {row['Transit']},                -- Transit
       {row['Walk']},                   -- Walk
       {row['OtherTransp']},            -- OtherTransp
       {row['WorkAtHome']},             -- WorkAtHome
       {row['MeanCommute']},            -- MeanCommute
       {row['Employed']},               -- Employed
       {row['PrivateWork']},            -- PrivateWork
       {row['PublicWork']},             -- PublicWork
       {row['SelfEmployed']},           -- SelfEmployed
       {row['FamilyWork']},             -- FamilyWork
       {row['Unemployment']}            -- Unemployment
    """
    return ret
    
def initialize():
  parser = argparse.ArgumentParser()
  parser.add_argument("-d", "--datafile", required=True)
  parser.add_argument("-c", "--createtable", action="store_true")
  args = parser.parse_args()
  global Datafile
  Datafile = args.datafile
  global CreateDB
  CreateDB = args.createtable
  
# read the input data file into a list of row strings
def readdata(fname):
    print(f"readdata: reading from File: {fname}")
    with open(fname, mode="r") as fil:
        dr = csv.DictReader(fil)
        rowlist = []
        for row in dr:
            rowlist.append(row)
    return rowlist, fil
    
# convert list of data rows into list of SQL 'INSERT INTO ...' commands
def getSQLcmnds(rowlist):
    cmdlist = []
    valuelist = []
    #cmdlist = f"INSERT INTO TempTable VALUES ({rowlist});"
    for row in rowlist:
        valstr = row2vals(row)
        cmd = f"INSERT INTO {TableName} VALUES ({valstr});"
        #cmd = f"INSERT INTO {'TempTable'} VALUES ({valstr});"
        cmdlist.append(cmd)
        valuelist.append(valstr)
    return cmdlist,valuelist
    
# connect to the database
def dbconnect():
    connection = psycopg2.connect(
        host="localhost",
        database=DBname,
        user=DBuser,
        password=DBpwd,
        )
    connection.autocommit = True#False#True
    return connection
    
# create the target table
# assumes that conn is a valid, open connection to a Postgres database
def createTable(conn):
    with conn.cursor() as cursor:
        cursor.execute(f"""
            DROP TABLE IF EXISTS {TableName};
            CREATE TABLE {TableName} (
                CensusTract         NUMERIC,
                State               TEXT,
                County              TEXT,
                TotalPop            INTEGER,
                Men                 INTEGER,
                Women               INTEGER,
                Hispanic            DECIMAL,
                White               DECIMAL,
                Black               DECIMAL,
                Native              DECIMAL,
                Asian               DECIMAL,
                Pacific             DECIMAL,
                Citizen             DECIMAL,
                Income              DECIMAL,
                IncomeErr           DECIMAL,
                IncomePerCap        DECIMAL,
                IncomePerCapErr     DECIMAL,
                Poverty             DECIMAL,
                ChildPoverty        DECIMAL,
                Professional        DECIMAL,
                Service             DECIMAL,
                Office              DECIMAL,
                Construction        DECIMAL,
                Production          DECIMAL,
                Drive               DECIMAL,
                Carpool             DECIMAL,
                Transit             DECIMAL,
                Walk                DECIMAL,
                OtherTransp         DECIMAL,
                WorkAtHome          DECIMAL,
                MeanCommute         DECIMAL,
                Employed            INTEGER,
                PrivateWork         DECIMAL,
                PublicWork          DECIMAL,
                SelfEmployed        DECIMAL,
                FamilyWork          DECIMAL,
                Unemployment        DECIMAL
            );
        """)
        print(f"Created {TableName}")
        conn.commit()

def createTempTable(conn):
    with conn.cursor() as cursor:
        cursor.execute(f"""
            SET temp_buffers TO 256;
            DROP TABLE IF EXISTS {'TempTable'};
            CREATE TEMP TABLE {'TempTable'}(
                CensusTract         NUMERIC,
                State               TEXT,
                County              TEXT,
                TotalPop            INTEGER,
                Men                 INTEGER,
                Women               INTEGER,
                Hispanic            DECIMAL,
                White               DECIMAL,
                Black               DECIMAL,
                Native              DECIMAL,
                Asian               DECIMAL,
                Pacific             DECIMAL,
                Citizen             DECIMAL,
                Income              DECIMAL,
                IncomeErr           DECIMAL,
                IncomePerCap        DECIMAL,
                IncomePerCapErr     DECIMAL,
                Poverty             DECIMAL,
                ChildPoverty        DECIMAL,
                Professional        DECIMAL,
                Service             DECIMAL,
                Office              DECIMAL,
                Construction        DECIMAL,
                Production          DECIMAL,
                Drive               DECIMAL,
                Carpool             DECIMAL,
                Transit             DECIMAL,
                Walk                DECIMAL,
                OtherTransp         DECIMAL,
                WorkAtHome          DECIMAL,
                MeanCommute         DECIMAL,
                Employed            INTEGER,
                PrivateWork         DECIMAL,
                PublicWork          DECIMAL,
                SelfEmployed        DECIMAL,
                FamilyWork          DECIMAL,
                Unemployment        DECIMAL
            );
        """)
        print(f"Created TempTable")
        
def load(conn, icmdlist):
    with conn.cursor() as cursor:
        print(f"Loading {len(icmdlist)} rows")
        start = time.perf_counter()
        for cmd in icmdlist:
            cursor.execute(cmd)
        cursor.execute(f"""
            ALTER TABLE {TableName} ADD PRIMARY KEY (CensusTract);
            CREATE INDEX idx_{TableName}_State ON {TableName}(State);
        """)
        conn.commit()
        elapsed = time.perf_counter() - start
        print(f'Finished Loading. Elapsed Time: {elapsed:0.4} seconds')
    #with conn.cursor() as cursor:
    #   cursor.execute(f"""
    #        INSERT INTO {TableName} SELECT * FROM {'TempTable'};
    #       ALTER TABLE {TableName} ADD PRIMARY KEY (CensusTract);
    #       CREATE INDEX idx_{TableName}_State ON {TableName}(State);
    #   """)
    
def batch_load(conn,icmdlist,valstr):
    with conn.cursor() as cursor:
        print(f"Loading {len(icmdlist)} rows")
        start = time.perf_counter()
        psycopg2.extras.execute_batch(cursor,icmdlist[0],valstr)
        """
        cursor.execute(f"""
            #ALTER TABLE {TableName} ADD PRIMARY KEY (CensusTract);
            #CREATE INDEX idx_{TableName}_State ON {TableName}(State);
        """)
        """
        elapsed = time.perf_counter() - start
        print(f'Finished Loading.Elapsed Time: {elapsed:0.4} seconds')
        
#def clean_csv_value():
def create_csv_without_header(fil):
    with open (fil, mode ="r") as fil1:
        with open("no_header_file.csv",'w') as fil2:
          next(fil1) #skip header line
          for line in fil1:
              fil2.write(line)
              
def load_with_copy(conn,icmdlist,rowlist):
    with conn.cursor() as cursor:
        print(f"Loading {len(icmdlist)} rows")
        start = time.perf_counter()
        create_csv_without_header(Datafile)
        
        with open('no_header_file.csv', mode="r") as fil:
           cursor.copy_from(fil,'censusdata',sep=',')
           
        elapsed = time.perf_counter() - start
        print(f'Finished Loading.Elapsed Time: {elapsed:0.4} seconds')
        
def main():
    initialize()
    conn = dbconnect()
    rlis,fil = readdata(Datafile)
    cmdlist,valuelist = getSQLcmnds(rlis)
    
    if CreateDB:
        createTable(conn)
        
    #createTempTable(conn)
    #batch_load(conn, cmdlist,valuelist)
    load_with_copy(conn, cmdlist,fil)
    #load(conn, cmdlist)
    conn.commit()
    
if __name__ == "__main__":
    main()
~
~
~
~
~
~
~
                                                                                                 302,21-24     Bot
Page down



