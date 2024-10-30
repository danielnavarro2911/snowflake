import snowflake.connector
import pandas as pd
import os

class snowflake_connection:
    def __init__(self):
        #trata de usar mis credenciales guardadas en mi pc
        self.user=os.getlogin()
    #conectamos con snowflake, introducimos el query y regresa un dataframe
    def query(self,SQL):
        #conectamos con snowflake, introducimos el query y regresa un dataframe
        self.conn= snowflake.connector.connect(user=self.user.upper()+'@WFSCORP.COM',
                                 authenticator='externalbrowser',
                                 account='wfs.us-east-1',
                                 database='EDW_PROD',
                                 schema='DAL')
        res=self.conn.cursor().execute(SQL).fetchall()
        return pd.read_sql(SQL,self.conn)
        
