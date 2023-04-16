import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
#engine = create_engine("oracle+cx_oracle://majan_uat:log@192.168.0.13/?service_name=orcl", arraysize=1000)
engine = create_engine("oracle+cx_oracle://majan13042023:log@localhost/?service_name=orcl.majanglass.com", arraysize=1000)
def readoraclequery(query):
   try:
      with engine.begin() as conn:
         df_data = pd.read_sql_query(sql=text(query), con=conn)
      return df_data
   except SQLAlchemyError as e:
      print(e)