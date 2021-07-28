#########################################
#               package                 #
#########################################
import os
import pyodbc
import pandas as pd
import numpy as np
import pymysql

#########################################
#               Function                #
#########################################
class MySQL():

    ## Initialize  ##

    def __init__(self , user: str, password: str , host: str , database: str):
        self.user = user
        self.password = password
        self.host = host
        self.database = database

    def check_connect(self):
        connect = 1
        while connect < 6:
            # connect_info = pymysql.connect( host = f'{self.host}' , user = f'{self.user}' , password = f'{self.password}' , db = f'{self.database}')

            connect_info = pyodbc.connect("DRIVER={MySQL ODBC 8.0 Unicode Driver};" +
                                          f'SERVER={self.host};DATABASE={self.database}; UID={self.user}; PASSWORD={self.password};CHARSET=UTF8;')

            try:
                print('Connect Success')
                break
            except :

                if connect < 5:
                    connect += 1

                else:
                    print(f'Connect Fail')
                    os._exit(0)

        return  connect_info

    def mysql_to_dataframe(self, query: str) -> pd.DataFrame:
        connect_info = self.check_connect()

        df = pd.read_sql(query, connect_info)

        return  df

    def create_table_sql(self, df : pd.DataFrame , table_name : str):
        column_list = list(df.columns)
        columns = [f"{col}" for col in column_list]
        type_dict = { 'object' : 'varchar(200)' ,
                      'int64' : 'int' ,
                      'int32' : 'int' ,
                      'float64' : 'float' ,
                      'datetime64[ns]' : 'datetime'}

        types = [type_dict[str(type)] for type in df.dtypes]
        sql = ""
        for col,type in zip(columns , types) :
            if col == column_list[-1] :
                sql += f"`{col}` {type}"
            else:
                sql += f"`{col}` {type},"
        sql = f""" CREATE TABLE {self.database}.{table_name} ({sql});"""

        return sql

    def insert_table_sql( self , df : pd.DataFrame , table_name : str ) :
        column_list = list(df.columns)
        columns = [f"{col}" for col in column_list]
        columns = ','.join(columns)
        sql = fr"INSERT INTO {self.database}.{table_name} ({columns}) VALUES "


        for row in df.itertuples(index = False , name = None) :

            row = list(row)
            remove_column_list = []
            remove_row_list = []

            for i in range(len(row)):
                try:
                    if np.isnan( row[i] ) :
                        remove_column_list.append(column_list[i])
                        remove_row_list.append(row[i])
                except:
                    pass

            for col in remove_column_list :
                column_list.remove(col)

            for r in remove_row_list :
                row.remove(r)

            row = tuple(row)

            if row == list(df.itertuples(index=False, name=None))[-1] :
                sql += f"""{row};"""
            else:
                sql += f"""{row},"""
        return sql


    def dataframe_to_mysql(self , df : pd.DataFrame , table_name : str) :
        connect_info = self.check_connect()
        cursor = connect_info.cursor()
        # with connect_info.cursor(as_dict = True) as cursor :
        try :
            sql = self.create_table_sql( df , table_name )
            cursor.execute(fr'{sql}')
            sql = self.insert_table_sql(df , table_name)
            cursor.execute(fr'{sql}')
            connect_info.commit()

            print('Create Table')
        except Exception as e :
            sql = self.insert_table_sql(df , table_name)
            cursor.execute(fr'{sql}')
            connect_info.commit()
            print('Table has already exist')
        finally :
            sql = self.insert_table_sql(df , table_name)
            cursor.execute(sql)
            connect_info.commit()


#########################################
#                Project                #
#########################################

######################################
#               Setting              #
######################################

host = '127.0.0.1'

port = '3306'

user = 'Tibame_Class'

password = 'P@ssword'

database = 'tibame_lab'

######################################
#               Process              #
######################################

print('Start Project')


MySQL_Info = MySQL( user = user, password = password , host = host , database = database)

df = pd.read_csv('test.csv')

MySQL_Info.dataframe_to_mysql( df = df , table_name = 'test2')

sql_query = """
            SELECT  * 
            FROM tibame_lab.test2
            Limit 0, 1000
            """

df = MySQL_Info.mysql_to_dataframe(query=sql_query)
print(df)
