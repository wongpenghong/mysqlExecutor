import psycopg2
import pandas as pd
import json
from datetime import datetime
import sqlalchemy
import sys
import ndjson

with open("config_mysql.json", "r") as read_file:
    CONFIG = json.load(read_file)['config_mysql']

class mysql_execute:
    def __init__(self):
        self.database = CONFIG['database_mysql']
        self.user = CONFIG['username_mysql']
        self.password = CONFIG['password_mysql']
        self.host = CONFIG['host_mysql']
        self.port = CONFIG['port_mysql']
        self.path = CONFIG['path'],
        self.column = CONFIG['column']
        
    def _build_connection_mysql(self):
        """
        Create Connection For MySQL
        """
        database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{username}:{password}@{host}:{port}/{database}'.format(
            username=CONFIG['username_mysql'],
            password=CONFIG['password_mysql'], 
            host=CONFIG['host_mysql'],
            port=CONFIG['port_mysql'],
            database=CONFIG['database_mysql'], echo=False))
        dbConnection    = database_connection.connect()
        return dbConnection
    
    def list_to_df(self,data):
        """
        Create dataFrame List
        """
        df = pd.DataFrame(data, columns = self.column)
        return df
    
   
    def fetch_data(self,verbose=1): 
        
        # First initial to know performance script
        if verbose:
            print('executing query....')
            start = datetime.now()
        
        conn = self._build_connection_mysql()
        
        # Sampe Query
        query ="""
            SELECT {column}
            FROM {db}
            where date(created) = '2018-08-08'
        """.format(column=CONFIG['query_column'],db=CONFIG['db'])
        
        # Create dataFrame from Query
        df = pd.read_sql_query(query, conn)
        
        if verbose:
            print('query executed in ' + str(datetime.now() - start))
        
        return df
    
    def df_to_json(self):
        """
        dump dataFrame to Json File
        """
        df = self.fetch_data()
        
        df['created'] = pd.to_datetime(df['created'], format='%Y-%m-%d %H:%M:%S')
        df['created'] = df['created'].dt.strftime('%Y-%m-%d %H:%M:%S')
        datas = df.to_json(orient='records')
        json_data = json.loads(datas)
        
        if(json_data != []):
            with open(self.path, 'w') as f:
                ndjson.dump(json_data, f)
                print('success')
        else:
            print('no_data!')
        return True