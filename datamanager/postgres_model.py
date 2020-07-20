from frastro.frastro.core.database.postgresql.postgresql_manager import PostgrestSQLManager
import datetime
from astropy.time import Time
import pandas as pd
class PosgrestModel():

    def __init__(self):
        self.setConection()
    def setConection(self):
        self.connection = PostgrestSQLManager()

    def getQuery(self,query):
        query_str= query.encode("utf-8")
        data = self.connection.select(query_str)


        titles = self.connection.getCursor().description

        colnames = [col[0] for idx, col in enumerate(titles)]
        pddata = pd.DataFrame(data,columns=colnames).fillna('null')

        return {"columns": colnames, "data": pddata.values.tolist()}




    def getLasObjects(self,days_ago=5,limit=1000):
        time = Time(datetime.datetime.now() - datetime.timedelta(days=int(days_ago)), format='datetime').to_value('mjd','long')
        query="select * from objects where lastmjd>={0} limit {1};"
        query=query.format(str(int(time)),str(int(limit)))
        return self.getQuery(query)

    def getLighCurve(self,ztfid):
        query = "select * from detections where oid='{0}' and isdiffpos=1 order by mjd asc limit 2000"
        query = query.format(str(ztfid))
        return self.getQuery(query)




