# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import pyodbc 
import pandas as pd
import pyodbc
import sqlalchemy as db
from random import random
from random import randrange, uniform
import uuid



number = 1

sql_conn = pyodbc.connect('DRIVER={SQL Server}; \
                            SERVER=rs-msp-mockv17.public.bc8bb9eb8ba4.database.windows.net,3342; \
                            DATABASE=digitalag-core-sandbox-sql-db; \
                            UID=deloitte_sa; \
                            PWD=Saturnrawscheduling2020; \
                            Trusted_connection=no') 

query = "SELECT * FROM [digitalag-core-sandbox-sql-db].[dbo].[PLCY20BinInSeasonSampling]"

df = pd.read_sql(query, sql_conn)
#print(df)
dfToDump = pd.DataFrame(columns=['RegionCode ','SeasonCode','SampleGUID','SampleCategoryID','SampleRefCID ','SampleDate','GrowerGUID','RawSourceTypeCode','RawSourceL1GUID','RawSourceL2GUID','RawSourceL3GUID', 'VendorNumber'])

for i,row in df.iterrows():
    
    RegionCode = row['RegionCode']
    SeasonCode = row['SeasonCode']
    SampleGUID = uuid.UUID('C4244F0C-ABCE-451B-A895-83C0E6D1F468').hex
    SampleCategoryID = row['SampleCategoryID']
    SampleRefCID = 'PL-CY20-BinSamplingDataV4-26072020'
    SampleDate = row['SampleDate']
    GrowerGUID = '11CAAE81-2AC2-4FF5-93C8-2CEAFAEA2DE6'
    RawSourceTypeCode = '1'
    RawSourceL1GUID = row['RawSourceL1GUID']
    RawSourceL2GUID = row['RawSourceL2GUID']
    RawSourceL3GUID = row['RawSourceL3GUID']
    VendorNumber = row['RawSourceL3GUID']
#    print(RegionCode,SeasonCode ,SampleGUID)
    
        
    cursor = sql_conn.cursor()    
    SQLCommand = ("INSERT INTO [dbo].[Sampling.SampleT3](RegionCode,SeasonCode,SampleGUID, SampleCategoryID, SampleRefCID, SampleDate, RawSourceTypeCode, RawSourceL1GUID, RawSourceL2GUID, RawSourceL3GUID,VendorNumber) VALUES (?,?,NEWID(),?,?,?,?,?,?,?,?)")    
    Values = [RegionCode,SeasonCode,SampleCategoryID,SampleRefCID,SampleDate, RawSourceTypeCode,RawSourceL1GUID,RawSourceL2GUID,RawSourceL3GUID,VendorNumber]   
    print(SQLCommand)
#    break
    cursor.execute(SQLCommand,Values)     
    #Commiting any pending transaction to the database.    
    cursor.commit()    
    #closing connection 