#########################################
#               package                 #
#########################################
import os
import pyodbc
import pandas as pd
import numpy as np
import pymongo
from urllib.parse import quote_plus
from pymongo import IndexModel , ASCENDING ,DESCENDING


#########################################
#               Function                #
#########################################

class Mongo():

    ## Initialize  ##

    def __init__(self , user: str, password: str , host: str , port: str, database: str):
        self.user = user
        self.password = quote_plus(password)
        self.host = host
        self.port = port
        self.database = database

    def check_connect(self):
        try :
            with pymongo.MongoClient(f'mongodb://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}?authSource={self.database}') as client :
                print(client.server_info())
                print('\nSuccess to Connect Mongo !')
        except :
            print('Fail to Connect Mongo !')

    def connect_to_Mongo(self , connect_to_db : bool=True) :
        with pymongo.MongoClient(f'mongodb://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}?authSource={self.database}') as client :
            if connect_to_db == True :
                db = client[self.database]
                return db
            else :
                return  client
    ## DataFrame to Mongo
    def dataframe_to_Mongo(self , df : pd.DataFrame, table_name : str ) :
        database = self.connect_to_Mongo()
        collection = database[table_name]
        records = df.to_dict(orient='records')
        collection.insert_many(records)

    ## Mongo to DataFrame
    def Mongo_to_dataframe(self , query : list , table_name : str , no_id : bool = True) :
        database = self.connect_to_Mongo()
        collection = database[table_name]
        data = collection.aggregate( pipeline = query )
        df = pd.DataFrame( data = data)
        if (no_id == True) & ('_id' in df.columns):
            del df['_id']
        return  df

    ## Delete Mongo data
    def delete_to_Mongo(self , filter : dict ,table_name : str) :
        database = self.connect_to_Mongo()
        collection = database[table_name]
        collection.delete_many( filter = filter )



######################################
#               Setting              #
######################################
if __name__ == '__main__' :

    host = '127.0.0.1'

    port = '27017'

    user = 'Tibame_Class'

    password = 'P@ssword'

    database = 'tibame_lab'


    ## 建立Mongo class 的 Instance
    Mongo_Info = Mongo( user = user, password = password , host = host , port = port,database = database)


    ## 確認連線狀況
    Mongo_Info.check_connect()

    database = Mongo_Info.connect_to_Mongo()

    ## 建立 Index
    # collection = database['Table_Name']
    # index1 = IndexModel([('Part_No', ASCENDING)])
    # index2 = IndexModel([('Lot', ASCENDING)])
    # index3 = IndexModel([('Part_No', ASCENDING) , ('Lot', ASCENDING)])
    # collection.create_index([index1,index2,index3])

    #檢視所有資料表名稱
    for i in sorted(database.list_collection_names()) :
        print(i)

    #檢視資料表內容
    collection = database['Table_Name']
    for i in collection.find({}).limit(1) :
        print(i)







