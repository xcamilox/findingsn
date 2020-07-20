import psycopg2
import time
from datetime import datetime

from frastro.frastro.core.data.archive.lasair_archive_cp import LasairArchive
from frastro.frastro.core.data.archive.tns_archive_cp import TNSServices
import multiprocessing
import threading
import concurrent.futures
import random

#calc am magnituds imports
import math
from astropy import units as u
from astropy.coordinates import Distance
import numpy as np
import pandas as pd
import random

class UpdateDB():
    local_db = {
        
    }

    remote_db = {
        
    }
    lastdetections_date = 0

    days_ago=1


    def setLocalDB(self):
        self.__localclient = psycopg2.connect("dbname=" + self.local_db["dbname"] + " user=" + self.local_db["user"],
                                              host=self.local_db['host'], password=self.local_db['password'])
        self.__localclient.autocommit = True
        self.__localclient_cursor = self.__localclient.cursor()

    def setAlerceDB(self):
        self.__remoteclient = psycopg2.connect(dbname=self.remote_db['dbname'], user=self.remote_db['user'],
                                               host=self.remote_db['host'], password=self.remote_db['password'])
        self.__remoteclient.autocommit = True
        self.__remoteclient_cursor = self.__remoteclient.cursor()

    def getLastRemoteObjects(self, lastobj):
        query = "select * from objects where lastmjd >= " + str(lastobj)+" and nobs>=2 order by lastmjd ASC;"
        #query = "select * from objects where lastmjd > 58990 order by lastmjd ASC;"
        #query = "select * from objects o where  (o.classrf in (10, 11, 12, 13, 14) OR (o.classearly = 19 AND o.classrf ISNULL)) and lastmjd >58985 order by lastmjd ASC;"
        print(query)
        self.__remoteclient_cursor.execute(query)
        return self.__remoteclient_cursor.fetchall()

    def getLastRemoteDetections(self, lastobj):
        #query = "select * from detections where mjd > " + str(lastobj) +"order by mjd ASC;"
        query="select d.* from detections d,objects o where d.mjd >=" + str(lastobj) +" and o.nobs >2 and d.oid=o.oid order by d.mjd ASC;"
        #query = "select * from detections where mjd > 58990 order by mjd ASC;"
        print(query)
        self.__remoteclient_cursor.execute(query)
        return self.__remoteclient_cursor.fetchall()

    def getlastLocalObject(self):
        query = "select max(lastmjd) from objects"
        query = "select mjdnow()::integer -{0};"
        query= query.format(self.days_ago)
        self.__localclient_cursor.execute(query)

        return self.__localclient_cursor.fetchone()

    def getlastTNSObject(self):
        query = "select max(id) from tnsdb"
        self.__localclient_cursor.execute(query)

        return self.__localclient_cursor.fetchone()

    def getlastLasairObject(self):
        query = "select max(mjdmax) from lasair"
        query = "select mjdnow()::integer -{0};"
        query = query.format(self.days_ago)
        self.__localclient_cursor.execute(query)

        return self.__localclient_cursor.fetchone()

    def getlastLocalDetections(self):
        query = "select max(mjd) from detections"
        query = "select mjdnow()::integer -{0};"
        query = query.format(self.days_ago)
        #query = "select mjdnow()-1"
        self.__localclient_cursor.execute(query)
        return self.__localclient_cursor.fetchone()

    def getlastLasairDetections(self):
        query = "select max(mjdmax) from lasair"
        query = "select mjdnow()::integer -{0};"
        query = query.format(self.days_ago)
        self.__localclient_cursor.execute(query)
        return self.__localclient_cursor.fetchone()

    def updateLasairClassification(self):


        now = datetime.now()

        current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
        print("---------------------------------------")
        print("{0}: UPDATING Lasair".format(current_time))
        self.setLocalDB()
        self.setAlerceDB()
        lastdate = self.getlastLasairObject()
        lasair = LasairArchive()

        #lastdate[0]=58266
        #lasair.getAllData(lastdate[0], page=0)
        lasair.getAllData(lastdate[0], page=0)

    def updateObjects(self):

        start_time = time.time()

        now = datetime.now()

        current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
        print("---------------------------------------")
        print("{0}: UPDATING Objects".format(current_time))
        self.setLocalDB()
        self.setAlerceDB()
        lastItem = self.getlastLocalObject()
        #self.lastdetections_date = lastItem[0]



        # lastmjd = "/Users/camilojimenez/Projects/iacsearch/lastmjd.csv"
        #
        # with open(lastmjd, "w") as f:
        #     f.write(now.strftime("%d/%m/%Y-%H:%M:%S")+","+str(lastItem[0]))
        #     f.close()
        #lastItem=[58973]
        items = self.getLastRemoteObjects(lastItem[0])
        now = datetime.now()

        current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
        print("{0}: Objects collected from Alerce {1} in {2}min ".format(current_time,len(items), str((time.time() - start_time) / 60)[0:4]))

        #self.generateParrallelTask(items, self.updateLocalObjects)
        #self.updateLocalObjects(items)

        already = 0
        news = 0
        start_time = time.time()
        for index, item in enumerate(items):
            try:
                # query_insert = "INSERT INTO public.objects (oid,nobs,mean_magap_g,mean_magap_r,median_magap_g,median_magap_r,max_magap_g,max_magap_r,min_magap_g,min_magap_r,sigma_magap_g,sigma_magap_r,last_magap_g,last_magap_r,first_magap_g,first_magap_r,mean_magpsf_g,mean_magpsf_r,median_magpsf_g,median_magpsf_r,max_magpsf_g,max_magpsf_r,min_magpsf_g,min_magpsf_r,sigma_magpsf_g,sigma_magpsf_r,last_magpsf_g,last_magpsf_r,first_magpsf_g,first_magpsf_r,meanra,meandec,sigmara,sigmadec,deltajd,lastmjd,firstmjd,period,catalogid,classxmatch,classrf,pclassrf,pclassearly,classearly) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT ON CONSTRAINT objects_pkey DO UPDATE SET nobs = EXCLUDED.nobs,mean_magap_g = EXCLUDED.mean_magap_g,mean_magap_r = EXCLUDED.mean_magap_r,median_magap_g = EXCLUDED.median_magap_g,median_magap_r = EXCLUDED.median_magap_r,max_magap_g = EXCLUDED.max_magap_g,max_magap_r = EXCLUDED.max_magap_r,min_magap_g = EXCLUDED.min_magap_g,min_magap_r = EXCLUDED.min_magap_r,sigma_magap_g = EXCLUDED.sigma_magap_g,sigma_magap_r = EXCLUDED.sigma_magap_r,last_magap_g = EXCLUDED.last_magap_g,last_magap_r = EXCLUDED.last_magap_r,first_magap_g = EXCLUDED.first_magap_g,first_magap_r = EXCLUDED.first_magap_r,mean_magpsf_g = EXCLUDED.mean_magpsf_g,mean_magpsf_r = EXCLUDED.mean_magpsf_r,median_magpsf_g = EXCLUDED.median_magpsf_g,median_magpsf_r = EXCLUDED.median_magpsf_r,max_magpsf_g = EXCLUDED.max_magpsf_g,max_magpsf_r = EXCLUDED.max_magpsf_r,min_magpsf_g = EXCLUDED.min_magpsf_g,min_magpsf_r = EXCLUDED.min_magpsf_r,sigma_magpsf_g = EXCLUDED.sigma_magpsf_g,sigma_magpsf_r = EXCLUDED.sigma_magpsf_r,last_magpsf_g = EXCLUDED.last_magpsf_g,last_magpsf_r = EXCLUDED.last_magpsf_r,first_magpsf_g = EXCLUDED.first_magpsf_g,first_magpsf_r = EXCLUDED.first_magpsf_r,meanra = EXCLUDED.meanra,meandec = EXCLUDED.meandec,sigmara = EXCLUDED.sigmara,sigmadec = EXCLUDED.sigmadec,deltajd = EXCLUDED.deltajd,lastmjd = EXCLUDED.lastmjd,firstmjd = EXCLUDED.firstmjd,period = EXCLUDED.period,catalogid = EXCLUDED.catalogid,classxmatch = EXCLUDED.classxmatch,classrf = EXCLUDED.classrf,pclassrf = EXCLUDED.pclassrf,pclassearly = EXCLUDED.pclassearly,classearly = EXCLUDED.classearly;"
                query_insert = "INSERT INTO public.objects (oid,nobs,mean_magap_g,mean_magap_r,median_magap_g,median_magap_r,max_magap_g,max_magap_r,min_magap_g,min_magap_r,sigma_magap_g,sigma_magap_r,last_magap_g,last_magap_r,first_magap_g,first_magap_r,mean_magpsf_g,mean_magpsf_r,median_magpsf_g,median_magpsf_r,max_magpsf_g,max_magpsf_r,min_magpsf_g,min_magpsf_r,sigma_magpsf_g,sigma_magpsf_r,last_magpsf_g,last_magpsf_r,first_magpsf_g,first_magpsf_r,meanra,meandec,sigmara,sigmadec,deltajd,lastmjd,firstmjd,period,catalogid,classxmatch,classrf,pclassrf,pclassearly,classearly) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
                record_to_insert = item
                self.__localclient_cursor.execute(query_insert, record_to_insert)

                news += 1
            except Exception as ex:

                postgres_update_query = "UPDATE public.objects set (oid,nobs,mean_magap_g,mean_magap_r,median_magap_g,median_magap_r,max_magap_g,max_magap_r,min_magap_g,min_magap_r,sigma_magap_g,sigma_magap_r,last_magap_g,last_magap_r,first_magap_g,first_magap_r,mean_magpsf_g,mean_magpsf_r,median_magpsf_g,median_magpsf_r,max_magpsf_g,max_magpsf_r,min_magpsf_g,min_magpsf_r,sigma_magpsf_g,sigma_magpsf_r,last_magpsf_g,last_magpsf_r,first_magpsf_g,first_magpsf_r,meanra,meandec,sigmara,sigmadec,deltajd,lastmjd,firstmjd,period,catalogid,classxmatch,classrf,pclassrf,pclassearly,classearly) = (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) where oid='" + \
                                        item[0] + "'"
                record_to_update = item
                self.__localclient_cursor.execute(postgres_update_query, record_to_update)
                already += 1
        self.__localclient.close()
        now = datetime.now()
        current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
        print("{0}: INGESTED:{1} news:{2}, already:{3} in{4}min".format(current_time, len(items), news, already,
                                                                        str((time.time() - start_time) / 60)[0:4]))


    def updateLocalObjects(self,items):
        connectiondb = psycopg2.connect("dbname=" + self.local_db["dbname"] + " user=" + self.local_db["user"],
                                        host=self.local_db['host'], password=self.local_db['password'])
        connectiondb.autocommit = True
        cursor = connectiondb.cursor()
        already = 0
        news = 0
        start_time = time.time()
        for index, item in enumerate(items):
            try:
                #query_insert = "INSERT INTO public.objects (oid,nobs,mean_magap_g,mean_magap_r,median_magap_g,median_magap_r,max_magap_g,max_magap_r,min_magap_g,min_magap_r,sigma_magap_g,sigma_magap_r,last_magap_g,last_magap_r,first_magap_g,first_magap_r,mean_magpsf_g,mean_magpsf_r,median_magpsf_g,median_magpsf_r,max_magpsf_g,max_magpsf_r,min_magpsf_g,min_magpsf_r,sigma_magpsf_g,sigma_magpsf_r,last_magpsf_g,last_magpsf_r,first_magpsf_g,first_magpsf_r,meanra,meandec,sigmara,sigmadec,deltajd,lastmjd,firstmjd,period,catalogid,classxmatch,classrf,pclassrf,pclassearly,classearly) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT ON CONSTRAINT objects_pkey DO UPDATE SET nobs = EXCLUDED.nobs,mean_magap_g = EXCLUDED.mean_magap_g,mean_magap_r = EXCLUDED.mean_magap_r,median_magap_g = EXCLUDED.median_magap_g,median_magap_r = EXCLUDED.median_magap_r,max_magap_g = EXCLUDED.max_magap_g,max_magap_r = EXCLUDED.max_magap_r,min_magap_g = EXCLUDED.min_magap_g,min_magap_r = EXCLUDED.min_magap_r,sigma_magap_g = EXCLUDED.sigma_magap_g,sigma_magap_r = EXCLUDED.sigma_magap_r,last_magap_g = EXCLUDED.last_magap_g,last_magap_r = EXCLUDED.last_magap_r,first_magap_g = EXCLUDED.first_magap_g,first_magap_r = EXCLUDED.first_magap_r,mean_magpsf_g = EXCLUDED.mean_magpsf_g,mean_magpsf_r = EXCLUDED.mean_magpsf_r,median_magpsf_g = EXCLUDED.median_magpsf_g,median_magpsf_r = EXCLUDED.median_magpsf_r,max_magpsf_g = EXCLUDED.max_magpsf_g,max_magpsf_r = EXCLUDED.max_magpsf_r,min_magpsf_g = EXCLUDED.min_magpsf_g,min_magpsf_r = EXCLUDED.min_magpsf_r,sigma_magpsf_g = EXCLUDED.sigma_magpsf_g,sigma_magpsf_r = EXCLUDED.sigma_magpsf_r,last_magpsf_g = EXCLUDED.last_magpsf_g,last_magpsf_r = EXCLUDED.last_magpsf_r,first_magpsf_g = EXCLUDED.first_magpsf_g,first_magpsf_r = EXCLUDED.first_magpsf_r,meanra = EXCLUDED.meanra,meandec = EXCLUDED.meandec,sigmara = EXCLUDED.sigmara,sigmadec = EXCLUDED.sigmadec,deltajd = EXCLUDED.deltajd,lastmjd = EXCLUDED.lastmjd,firstmjd = EXCLUDED.firstmjd,period = EXCLUDED.period,catalogid = EXCLUDED.catalogid,classxmatch = EXCLUDED.classxmatch,classrf = EXCLUDED.classrf,pclassrf = EXCLUDED.pclassrf,pclassearly = EXCLUDED.pclassearly,classearly = EXCLUDED.classearly;"
                query_insert = "INSERT INTO public.objects (oid,nobs,mean_magap_g,mean_magap_r,median_magap_g,median_magap_r,max_magap_g,max_magap_r,min_magap_g,min_magap_r,sigma_magap_g,sigma_magap_r,last_magap_g,last_magap_r,first_magap_g,first_magap_r,mean_magpsf_g,mean_magpsf_r,median_magpsf_g,median_magpsf_r,max_magpsf_g,max_magpsf_r,min_magpsf_g,min_magpsf_r,sigma_magpsf_g,sigma_magpsf_r,last_magpsf_g,last_magpsf_r,first_magpsf_g,first_magpsf_r,meanra,meandec,sigmara,sigmadec,deltajd,lastmjd,firstmjd,period,catalogid,classxmatch,classrf,pclassrf,pclassearly,classearly) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
                record_to_insert = item
                cursor.execute(query_insert, record_to_insert)

                news += 1
            except Exception as ex:

                postgres_update_query = "UPDATE public.objects set (oid,nobs,mean_magap_g,mean_magap_r,median_magap_g,median_magap_r,max_magap_g,max_magap_r,min_magap_g,min_magap_r,sigma_magap_g,sigma_magap_r,last_magap_g,last_magap_r,first_magap_g,first_magap_r,mean_magpsf_g,mean_magpsf_r,median_magpsf_g,median_magpsf_r,max_magpsf_g,max_magpsf_r,min_magpsf_g,min_magpsf_r,sigma_magpsf_g,sigma_magpsf_r,last_magpsf_g,last_magpsf_r,first_magpsf_g,first_magpsf_r,meanra,meandec,sigmara,sigmadec,deltajd,lastmjd,firstmjd,period,catalogid,classxmatch,classrf,pclassrf,pclassearly,classearly) = (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) where oid='" + \
                                        item[0] + "'"
                record_to_update = item
                cursor.execute(postgres_update_query, record_to_update)
                already += 1
        connectiondb.close()
        now = datetime.now()
        current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
        print("{0}: INGESTED:{1} news:{2}, already:{3} in{4}min".format(current_time, len(items), news, already,
                                                                       str((time.time() - start_time) / 60)[0:4]))



    def updateDetections(self):

        now = datetime.now()

        current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
        print("---------------------------------------")
        print("{0}: UPDATING Detections".format(current_time))
        start_time = time.time()
        self.setLocalDB()
        self.setAlerceDB()
        lastItem = self.getlastLocalDetections()

        items = self.getLastRemoteDetections(lastItem[0])

        now = datetime.now()

        current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
        print("{0}: Detections collected from Alerce {1} in {2}min ".format(current_time, len(items),
                                                                         str((time.time() - start_time) / 60)[0:4]))
        #self.__localclient.close()
        #self.generateParrallelTask(items, self.updateLocalDetections)
        #self.updateLocalDetections(items)

        already = 0
        news = 0
        start_time = time.time()
        for index, item in enumerate(items):
            try:
                query_insert = 'INSERT INTO public.detections (oid,candid,mjd,fid,diffmaglim,magpsf,magap,sigmapsf,sigmagap,ra,"dec",sigmara,sigmadec,isdiffpos,distpsnr1,sgscore1,field,rcid,magnr,sigmagnr,rb,magpsf_corr,magap_corr,sigmapsf_corr,sigmagap_corr,has_stamps,parent_candid) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'
                record_to_insert = item
                self.__localclient_cursor.execute(query_insert, record_to_insert)
                # connectiondb.commit()
                news += 1
            except Exception as ex:
                already += 1

        self.__localclient.close()
        now = datetime.now()
        current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
        print("{0}: INGESTED:{1} news:{2}, already:{3} in{4}min".format(current_time, len(items), news, already,
                                                                        str((time.time() - start_time) / 60)[0:4]))


    def updateLocalDetections(self,items):
        connectiondb = psycopg2.connect("dbname=" + self.local_db["dbname"] + " user=" + self.local_db["user"],
                                        host=self.local_db['host'], password=self.local_db['password'])
        connectiondb.autocommit = True
        cursor = connectiondb.cursor()
        print(connectiondb.autocommit)
        already = 0
        news = 0
        start_time = time.time()
        for index, item in enumerate(items):
            try:
                query_insert = 'INSERT INTO public.detections (oid,candid,mjd,fid,diffmaglim,magpsf,magap,sigmapsf,sigmagap,ra,"dec",sigmara,sigmadec,isdiffpos,distpsnr1,sgscore1,field,rcid,magnr,sigmagnr,rb,magpsf_corr,magap_corr,sigmapsf_corr,sigmagap_corr,has_stamps,parent_candid) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'
                record_to_insert = item
                cursor.execute(query_insert, record_to_insert)
                #connectiondb.commit()
                news+=1
            except Exception as ex:
                already+=1

        connectiondb.close()
        now = datetime.now()
        current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
        print("{0}: INGESTED:{1} news:{2}, already:{3} in{4}min".format(current_time,len(items), news,already,str((time.time() - start_time) / 60)[0:4]))

    def updateTNS(self,page=0,olny_new=True):
        now = datetime.now()

        current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
        print("---------------------------------------")
        print("{0}: UPDATING TNS".format(current_time))
        self.setLocalDB()
        self.setAlerceDB()
        lastid = self.getlastTNSObject()
        tns = TNSServices()
        tns.downloadCSV(lastid[0], page,olny_new)

    def radec_tns(self):
        tns = TNSServices()
        tns.updateRADEC()

    def crossMatch(self):
        self.setLocalDB()
        print("crossmatch Database")
        #query = "CALL crossmatch_objects(" + str(lastdate) + ")"


        query_declas="""INSERT INTO public.crossmatch (oid, object_id, photoz, mass,sfr,distance,catalog,cat_id,date_match,specz)
            SELECT o.oid,c.object_id,c.photo_z as photoz,c.mass_best as mass, sfr_best as sfr,
            3600*q3c_dist(o.meanra, o.meandec, c.ra, c.dec) as distance, 
            'decals_dr8' as catalog, 2, NOW(),c.spec_z 
            FROM  objects AS o,decals_dr8 AS c 
            WHERE ((o.classrf IN (10,11,12,13,14) OR (o.classearly =19 and o.classrf isnull )) or 
            o.lasair_class NOT IN ('VS' , 'AGN', 'CV', 'BS') or tns_obj_type like '%SN%') and 
            o.firstmjd>=(mjdnow()-1)::integer and q3c_join(o.meanra, o.meandec,c.ra, c.dec, 0.001388888888888889) 
            ON CONFLICT ON CONSTRAINT crossmatch_pkey DO NOTHING;"""
        self.__localclient_cursor.execute(query_declas)
        query_hsc="""INSERT INTO public.crossmatch (oid, object_id, photoz, mass,sfr,distance,catalog,cat_id,date_match,specz )
            SELECT o.oid,h.object_id,h.photoz_best as photoz,h.stellar_mass as mass, sfr,
            3600*q3c_dist(o.meanra, o.meandec, h.ra, h.dec) as distance, 
            'hsc_wide_dr2' as catalog, 1, NOW(),0
            FROM  objects AS o,hsc_wide_dr2 AS h 
            WHERE ((o.classrf IN (10,11,12,13,14) OR (o.classearly =19 and o.classrf isnull )) or 
            o.lasair_class NOT IN ('VS' , 'AGN', 'CV', 'BS') or tns_obj_type like '%SN%') and  
            o.firstmjd>=(mjdnow()-1)::integer and q3c_join(o.meanra, o.meandec,h.ra, h.dec, 0.001388888888888889)
            ON CONFLICT ON CONSTRAINT crossmatch_pkey 
            DO NOTHING;"""
        self.__localclient_cursor.execute(query_hsc)
        query_tns="""update objects set tns_name=tns_ref.name,tns_obj_type=tns_ref.obj_type,tns_redshift=tns_ref.redshift,tns_host_redshift=tns_ref.host_redshift
            from
            (select o.oid,t.name,t.obj_type,t.redshift,t.host_redshift from tnsdb as t,objects as o where t.disc_internal_name=o.oid) as tns_ref
            where objects.oid=tns_ref.oid;"""
        self.__localclient_cursor.execute(query_tns)

        self.__localclient.close()

    def updateWholeDB(self):

        tns_proc = multiprocessing.Process(target=self.updateTNS)
        obj_proc = multiprocessing.Process(target=self.updateObjects)
        det_proc = multiprocessing.Process(target=self.updateDetections)
        lasair_proc = multiprocessing.Process(target=self.updateLasairClassification)

        tns_proc.start()
        obj_proc.start()
        det_proc.start()
        lasair_proc.start()


    def updateMinMax(self):
        now = datetime.now()
        start_time = time.time()
        current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
        print("---------------------------------------")
        print("{0}: UPDATING PEAKS".format(current_time))
        df = pd.read_csv('/Users/camilojimenez/Projects/iacsearch/lastmjd.csv', sep=',', header=None)
        lastmjd=df.iloc[0][1]
        self.setLocalDB()
        query="select zsn_updateminmaxmjd2(oid) from objects o where lastmjd >mjdnow()-{0} and ((o.classrf IN (10,11,12,13,14) OR (o.classearly =19 and o.classrf isnull)) OR o.lasair_class IN ('SN','NT','ORPHAN') OR o.tns_obj_type LIKE '%SN%')"#+str(lastmjd)
        query = query.format(self.days_ago)
        #query="select zsn_updateminmaxmjd(oid) from objects where peakr_mjd isnull and peakg_mjd isnull"

        self.__localclient_cursor.execute(query)

        query = "select zsn_updatepeak2(oid) from objects o where lastmjd >mjdnow()-{0} and ((o.classrf IN (10,11,12,13,14) OR (o.classearly =19 and o.classrf isnull)) OR o.lasair_class IN ('SN','NT','ORPHAN') OR o.tns_obj_type LIKE '%SN%')"#+ str(lastmjd)
        query = query.format(self.days_ago)
        #query = "select zsn_updatepeak(oid) from objects where peakr_mjd isnull and peakg_mjd isnull"

        self.__localclient_cursor.execute(query)
        data = self.__localclient_cursor.fetchall()
        now = datetime.now()
        current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
        print("{0}: PEAKSCALCULATED:{1} from mjd:{2} in{3}min".format(current_time, len(data),lastmjd, str((time.time() - start_time) / 60)[0:4]))


    def generateRedshift(self):
        self.setLocalDB()
        self.calcABMagnitudes(limit=1000000,page=0)

    def calcABMagnitudes(self,limit=10000,page=0):
        now = datetime.now()
        start_time = time.time()
        current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
        print("---------------------------------------")
        print("{0}: UPDATING ABMagnitudes".format(current_time))

        #query = "select c.oid,c.object_id,d.candid,c.cat_id,d.magpsf,d.magap,d.magpsf_corr,d.magap_corr,c.photoz from crossmatch as c left join detections as d on d.oid=c.oid limit {} offset {};"
        #query="select c.oid,c.object_id,d.candid,c.cat_id,d.magpsf,d.magap,d.magpsf_corr,d.magap_corr,c.photoz from crossmatch as c left join detections as d on d.oid=c.oid where d.mjd>=(select ((cast(extract(epoch from max(datematch)) as integer) / 86400.0 ) + 2440587.5)-2400000.5 from abmagnitudes)::integer;"
        query="select c.oid,c.object_id,d.candid,c.cat_id,d.magpsf,d.magap,d.magpsf_corr,d.magap_corr,c.photoz,c.specz from crossmatch as c left join detections as d on d.oid=c.oid and d.candid notnull and NOT EXISTS (select candid from abmagnitudes where candid = d.candid) order by d.mjd asc;"
        #query = "select c.oid,c.object_id,d.candid,c.cat_id,d.magpsf,d.magap,d.magpsf_corr,d.magap_corr,c.photoz,c.specz from crossmatch as c left join detections as d on d.oid=c.oid where c.cat_id=3 and d.candid notnull order by d.mjd asc;"
        #update from tns catalog
        #query='select c.oid,c.object_id,d.candid,c.cat_id,d.magpsf,d.magap,d.magpsf_corr,d.magap_corr,c.photoz,c.specz from crossmatch as c left join detections as d on d.oid=c.oid and d.candid notnull where c.specz >0 and c.cat_id =3;'
        #query="select c.oid,c.object_id,d.candid,c.cat_id,d.magpsf,d.magap,d.magpsf_corr,d.magap_corr,c.photoz,c.specz from crossmatch as c left join detections as d on d.oid=c.oid and d.candid notnull where c.specz >0 and c.cat_id =3 and c.oid='iPTF16geu'"

        self.__localclient_cursor.execute(query)
        data = self.__localclient_cursor.fetchall()
        news=0
        already=0
        wrong_z=0
        for ind,row in enumerate(data):
            photoz = row[8]
            specz = row[9]
            if photoz == None or photoz <= 0:
                #dont calculate redshift iqual o less than 0
                if specz== None or specz<= 0:
                    wrong_z += 1
                    continue

            try:

                ifspecz = False
                if specz> 0:
                    ifspecz = True

                ab_magpsf=None
                ab_magap=None
                ab_magpsf_corr=None
                ab_magap_corr=None
                abs_magpsf = None
                abs_magap = None
                abs_magpsf_corr = None
                abs_magap_corr = None
                if row[4] != None:
                    if photoz!=None and photoz>0:
                        ab_magpsf = self.aparentToAbsoluteMagnitud(row[4],redshift=photoz)
                    if specz!=None and specz> 0:
                        abs_magpsf = self.aparentToAbsoluteMagnitud(row[4], redshift=specz)
                if row[5] != None:
                    if photoz!=None and photoz>0:
                        ab_magap = self.aparentToAbsoluteMagnitud(row[5],redshift=photoz)
                    if specz!=None and specz> 0:
                        abs_magap = self.aparentToAbsoluteMagnitud(row[5], redshift=specz)
                if row[6] != None:
                    if photoz!=None and photoz>0:
                        ab_magpsf_corr = self.aparentToAbsoluteMagnitud(row[6],redshift=photoz)
                    if specz!=None and specz> 0:
                        abs_magpsf_corr = self.aparentToAbsoluteMagnitud(row[6], redshift=specz)
                if row[7] != None:
                    if photoz!=None and photoz>0:
                        ab_magap_corr = self.aparentToAbsoluteMagnitud(row[7],redshift=photoz)
                    if specz!=None and specz> 0:
                        abs_magap_corr = self.aparentToAbsoluteMagnitud(row[7], redshift=specz)

                query_insert = 'INSERT INTO public.abmagnitudes (oid,object_id,candid,cat_id,ab_magpsf,ab_magap,ab_magpsf_corr,ab_magap_corr,datematch,ztype,abs_magpsf,abs_magap,abs_magpsf_corr,abs_magap_corr) values (%s,%s,%s,%s,%s,%s,%s,%s,NOW(),%s,%s,%s,%s,%s)'
                #query_insert = 'INSERT INTO public.abmagnitudes (oid,object_id,candid,cat_id,ab_magpsf,ab_magap,ab_magpsf_corr,ab_magap_corr,datematch,ztype,abs_magpsf,abs_magap,abs_magpsf_corr,abs_magap_corr) values (%s,%s,%s,%s,%s,%s,%s,%s,NOW(),%s,%s,%s,%s,%s) ON CONFLICT ON CONSTRAINT abmagnitudes_pkey DO UPDATE SET abs_magpsf=EXCLUDED.abs_magpsf,abs_magap=EXCLUDED.abs_magap,abs_magpsf_corr=EXCLUDED.abs_magpsf_corr,abs_magap_corr=EXCLUDED.abs_magap_corr'
                record_to_insert = (row[0],row[1],row[2],row[3],ab_magpsf,ab_magap,ab_magpsf_corr,ab_magap_corr,ifspecz,abs_magpsf,abs_magap,abs_magpsf_corr,abs_magap_corr)
                self.__localclient_cursor.execute(query_insert, record_to_insert)
                news+=1
            except Exception as ex:
                already+=1
                #print("detection already", row[0],ex)

        now = datetime.now()
        current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
        print("{0}: INGESTED:{1} news:{2}, already:{3}, z_wrong:{5} in{4}min".format(current_time, len(data), news, already,
                                                                      str((time.time() - start_time) / 60)[0:4],wrong_z))


    def multiprocess_photoz_distance(self):
        now = datetime.now()
        start_time = time.time()
        current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
        print("---------------------------------------")
        print("{0}: set distnace".format(current_time))
        self.setLocalDB()

        #query = "select object_id,photo_z,spec_z from decals_dr8 where photoz_distance is null and specz_distance is null limit 2000000;"
        #query="select object_id,photo_z,spec_z from decals_dr8 where photoz_distance isnull and specz_distance isnull and (photo_z>0 or spec_z>0) limit 2000000;"
        #query = "select id,redshift,null from tnsdb where redshift>0"
        query = "select hsc.object_id,hsc.photoz_best,null from snview as s inner join hsc_wide_dr2 hsc on hsc.object_id =s.hsc_obj_id where s.hsc_obj_id notnull limit 2000000;"
        self.__localclient_cursor.execute(query)
        data = self.__localclient_cursor.fetchall()

        self.__localclient.close()
        number_parallel_process = int(multiprocessing.cpu_count() / 2)
        process_data_size = int(len(data) / number_parallel_process)
        # self.multithread_calc_photz_distance(data)
        data = np.array(data)
        jobs=[]
        for i in range(number_parallel_process):
            #process = [.submit(self.setAllPhotoz, data[i:i + step - 1, :].tolist()) for i in range(0, data.shape[0], step)]
            process= multiprocessing.Process(target=self.multithread_calc_photz_distance,args=(data[i:i + process_data_size - 1, :].tolist(),))
            jobs.append(process)
            process.start()
        for j in jobs:
            j.join()

        now = datetime.now()
        current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
        print("{0}: Complete all Jobs in{1}min".format(current_time, str((time.time() - start_time) / 60)[0:4]))
        if len(data) >= 2000000:
            self.multiprocess_photoz_distance()


    def multithread_calc_photz_distance(self,data):
        id = random.random()
        now = datetime.now()
        start_time = time.time()
        current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
        print("---------------------------------------")
        print("{0}: set Jobs {1}".format(current_time,id))
        #self.setLocalDB()

        # query = "select object_id,photo_z,spec_z from decals_dr8 where photoz_distance is null and specz_distance is null limit 1000000;"
        # self.__localclient_cursor.execute(query)
        # data = self.__localclient_cursor.fetchall()
        #
        # self.__localclient.close()
        size = 2
        step=int(len(data)/size)
        #self.setAllPhotoz(data)
        data = np.array(data)


        with concurrent.futures.ThreadPoolExecutor(max_workers=size) as executor:
            list_of_dfs = [executor.submit(self.setAllPhotoz, data[i:i + step - 1, :].tolist()) for i in range(0, data.shape[0], step)]

        now = datetime.now()
        current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
        print("{0}: Complete all Threads for job {1} in{2}min".format(current_time,id, str((time.time() - start_time) / 60)[0:4]))

        # if len(data)>=1000000:
        #     self.multithread_calc_photz_distance()





    def setAllPhotoz(self,data):
        id=random.random()
        now = datetime.now()
        start_time = time.time()
        current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
        print("---------------------------------------")
        print("{0}: Running Thread {1} data {2}".format(current_time,id,len(data)))
        #self.setLocalDB()
        connectiondb=psycopg2.connect("dbname=" + self.local_db["dbname"] + " user=" + self.local_db["user"],
                         host=self.local_db['host'], password=self.local_db['password'])
        connectiondb.autocommit = True
        cursor = connectiondb.cursor()

        #query = "select object_id,photo_z,spec_z from decals_dr8 where photoz_distance is null and specz_distance is null order by object_id asc limit 1000000;"

        #self.__localclient_cursor.execute(query)

        #data = self.__localclient_cursor.fetchall()
        #print(len(data))
        for ind, row in enumerate(data):
            if (row[2] == None or row[2] <= 0) and  (row[1] == None or row[1] <= 0):
                continue
            disphotoz=None
            disspecz=None
            if row[1] != None and row[1] > 0:
                disphotoz=self.calcDistanceParsec(row[1])
            if row[2] != None and row[2] > 0:
                disspecz=self.calcDistanceParsec(row[2])
            #query_update="UPDATE public.decals_dr8 set (photoz_distance,specz_distance) =(%s,%s) where object_id="+str(int(row[0]))
            #query_update = "UPDATE public.tnsdb set redshift_distance =%s where id=" + str(int(row[0]))
            query_update = "UPDATE public.hsc_wide_dr2 set photoz_distance =%s where object_id=" + str(int(row[0]))
            #record_to_update=(disphotoz,disspecz)
            record_to_update = [disphotoz]
            cursor.execute(query_update, record_to_update)
        connectiondb.close()
        now = datetime.now()
        current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
        print("{0}: Done Thread {1}  in{1}min".format(current_time, id,str((time.time() - start_time) / 60)[0:4]))
        # if len(data)>=1000000:
        #     self.__localclient.close()
        #     self.setAllPhotoz()

    def calcDistanceParsec(self,redshift):
        distance = Distance(z=redshift, allow_negative=True).to(u.parsec).value
        return distance




    def aparentToAbsoluteMagnitud(self,apmag, redshift="",distance_pc=""):

        if redshift=="" and distance_pc =="":
            raise Exception('Distance in pc or redshift is necessary')

        if redshift!="":

            distance =  Distance(z=redshift,allow_negative=True).to(u.parsec).value
        if distance_pc != "":
            distance = distance_pc

        #expected peak mag = -19.2 + 5 * log10 (DL*1e6) -5 - 2.5 * log10 (1 + redshift)
        M = apmag - 5 * np.log10(distance) + 5 + 2.5 * np.log10(1 + redshift)


        return M

    def updateView(self):
        now = datetime.now()
        start_time = time.time()
        current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
        print("---------------------------------------")
        print("{0}: UPDATING SNVIEW".format(current_time))
        self.setLocalDB()
        query = "REFRESH MATERIALIZED VIEW snview_update with data;"
        self.__localclient_cursor.execute(query)
        current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
        print("{0}: UPDATED SNVIEW in {1}min".format(current_time,str((time.time() - start_time) / 60)[0:4]))

    def addIPTLightcurve(self):
        self.setLocalDB()
        path="/Users/camilojimenez/Projects/IPTF16geu/lightcurve_fixed.csv"
        df=pd.read_csv(path)

        # query_insert = "INSERT INTO public.objects (oid,nobs,mean_magap_g,mean_magap_r,median_magap_g,median_magap_r,max_magap_g,max_magap_r,min_magap_g,min_magap_r,sigma_magap_g,sigma_magap_r,last_magap_g,last_magap_r,first_magap_g,first_magap_r,mean_magpsf_g,mean_magpsf_r,median_magpsf_g,median_magpsf_r,max_magpsf_g,max_magpsf_r,min_magpsf_g,min_magpsf_r,sigma_magpsf_g,sigma_magpsf_r,last_magpsf_g,last_magpsf_r,first_magpsf_g,first_magpsf_r,meanra,meandec,sigmara,sigmadec,lastmjd,firstmjd,classrf,pclassrf,pclassearly,classearly,lasair_class,tns_name,tns_redshift) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
        # record_to_insert = ('iPTF16geu',71,20.559,19.5559,20.559,19.5559,21.505,19.876,22.742,18.771,0.605,0.810,21.505,20.044,19.926,20.803,20.559,19.5559,20.559,19.5559,21.505,19.876,22.742,18.771,0.605,0.810,21.505,20.044,19.926,20.803,316.066097,-6.340139,0,0,57674.21,57631.33,10,1,1,19,'SN','SN 2016geu',0.409)
        # self.__localclient_cursor.execute(query_insert, record_to_insert)
        for row_relected in df.iterrows():
            row=row_relected[1]
            query_insert = 'INSERT INTO public.detections (oid,candid,mjd,fid,magpsf,magap,sigmapsf,sigmagap,ra,"dec",sigmara,sigmadec,isdiffpos,sgscore1,rcid,rb) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'
            seed=random.randrange(1000, 10000, 5)
            record_to_insert = (row["oid"],int(row["mjd"]+seed),row["mjd"],row["fid"],row["magpsf"],row["magap"],row["sigmapsf"],row["sigmaap"],row["ra"],row["dec"],0,0,1,1,63,1)
            print(record_to_insert)
            self.__localclient_cursor.execute(query_insert, record_to_insert)

    def generateParrallelTask(self,data_list, functionToCall):
        now = datetime.now()
        start_time = time.time()
        mainid = random.random()
        current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
        number_parallel_process = int(multiprocessing.cpu_count() / 2)
        process_data_size = int(len(data_list) / number_parallel_process)
        print("---------------------------------------")

        print("{0}: Split process for {1} in {2} ".format(str(functionToCall), len(data_list), process_data_size))

        data = np.array(data_list)
        jobs = []
        print("number of process {0}".format(number_parallel_process))
        for i in range(number_parallel_process):
            processid = random.random()
            # process = [.submit(self.setAllPhotoz, data[i:i + step - 1, :].tolist()) for i in range(0, data.shape[0], step)]
            datashare=data[i:i + process_data_size - 1, :].tolist()
            process = multiprocessing.Process(target=self.processToSplit,
                                              args=(data[i:i + process_data_size - 1, :].tolist(), functionToCall,processid,))
            jobs.append(process)
            process.start()
        for j in jobs:
            j.join()
        now = datetime.now()
        current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
        print("{0}: Complete all process {1} in{2}min".format(current_time, id,
                                                                      str((time.time() - start_time) / 60)[0:4]))

    def processToSplit(self,data_list, thredToCall,idp):
        id = random.random()
        now = datetime.now()
        start_time = time.time()
        current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
        print("---------------------------------------")
        print("{0}: set Jobs {1} process {2}".format(str(thredToCall), id,idp))

        size = 2
        step = int(len(data_list) / size)

        data = np.array(data_list)

        print("split process in therds",range(0, data.shape[0], step))

        with concurrent.futures.ThreadPoolExecutor(max_workers=size) as executor:

            list_of_dfs = [executor.submit(thredToCall, data[i:i + step, :].tolist()) for i in
                           range(0, data.shape[0]-1, step)]

        now = datetime.now()
        current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
        print("{0}: Complete all Threads for job {1} in{2}min for process id{3}".format(current_time, id,
                                                                      str((time.time() - start_time) / 60)[0:4],idp))


    def exportQuery(self):
        self.setLocalDB()
        id = random.random()
        now = datetime.now()
        start_time = time.time()
        current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
        print("---------------------------------------")
        print("{0}: Create MYSQL Update {1}".format(current_time, id))

        query="select * from snview_update where lastmjd >= (mjdnow()-1)::integer;"
        query = query.format(self.days_ago)
        self.__localclient_cursor.execute(query)
        data = self.__localclient_cursor.fetchall()
        #for ind, row in enumerate(data):
        query_insert ="""INSERT INTO snhunter.snview (oid,tns_name,nobs,nobs_r,nobs_g,ra,dec_,days_active_g,days_active_r,theoretical_obs,theoretical_obs_g,theoretical_obs_r,alerce_early,classearly,pclassearly,alerce_late,classrf,pclassrf,tns_obj_type,lasair_class,tns_id,lasair_id,desis_distance,hsc_distance,desi_photoz,hsc_photoz,tns_redshift,tns_host_redshift,decals_spec_z,desi_mass,hsc_mass,desi_sfr,hsc_sfr,g_sigmapsf,g_minmagpsf,g_sigmaap,g_minmagap,g_sgscore1,r_sgscore1,r_sigmapsf,r_minmagpsf,r_sigmaap,r_minmagap,redshift_distance_tns,ab_g_minmagpsf_desi,ab_g_minmagap_desi,ab_r_minmagpsf_desi,ab_r_minmagap_desi,ab_spec_g_minmagpsf_desi,ab_spec_g_minmagap_desi,ab_spec_r_minmagpsf_desi,ab_spec_r_minmagap_desi,ab_g_minmagpsf_hsc,ab_g_minmagap_hsc,ab_r_minmagpsf_hsc,ab_r_minmagap_hsc,ab_g_minmagpsf_tns,ab_g_minmagap_tns,ab_r_minmagpsf_tns,ab_r_minmagap_tns,decals_obj_id,decals_type,decals_g_mag,decals_r_mag,decals_z_mag,decals_w1_mag,decals_w2_mag,decals_hp_idx,decals_field,desi_specz_distance,desi_photoz_distance,hsc_obj_id,hsc_g_mag,hsc_r_mag,hsc_i_mag,hsc_z_mag,hsc_y_mag,hsc_hp_idx,hsc_field,hsc_photoz_distance,tns_reporting_group,tns_discovery_date_ut,tns_discovery_mag_flux,tns_discovery_data_source,tns_classifying_group,tns_associated_group,tns_sender,tns_disc_instrument,tns_class_instrument,tns_discoverymjd,lastmjd,firstmjd,g_mjdmin,firstmjd_g,lastmjd_g,r_mjdmin,firstmjd_r,lastmjd_r,days_bf_gpeak,days_af_gpeak,days_bf_rpeak,days_af_rpeak,last_mjd_g,last_magpsf_g,ab_last_g_minmagpsf_desi,ab_last_spec_g_minmagpsf_desi,ab_last_g_minmagpsf_hsc,ab_last_g_minmagpsf_tns,last_candid_g,first_mjd_g,first_magpsf_g,ab_first_g_minmagpsf_desi,ab_first_spec_g_minmagpsf_desi,ab_first_g_minmagpsf_hsc,ab_first_g_minmagpsf_tns,first_candid_g,last_mjd_r,last_magpsf_r,ab_last_r_minmagpsf_desi,ab_last_spec_r_minmagpsf_desi,ab_last_r_minmagpsf_hsc,ab_last_r_minmagpsf_tns,last_candid_r,first_mjd_r,ab_first_r_magpsf_desi,ab_first_spec_r_magpsf_desi,ab_first_r_minmagpsf_hsc,ab_first_r_minmagpsf_tns,first_magpsf_r,first_candid_r,delta_magpsf_lf_g,delta_magpsf_lf_r,delta_magpsf_pl_g,delta_magpsf_pf_g,delta_magpsf_pl_r,delta_magpsf_pf_r) VALUES 
        ('ZTF19acmduyt',NULL,1,1,0,26.2536413,23.7639778,NULL,0,0,NULL,0,'Asteroid',21,0.9963471293449402,NULL,NULL,NULL,NULL,'SN',NULL,6,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,0.20283299684524536,0.18590426445007324,19.886262893676758,0.29679998755455017,19.57710075378418,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,58789.13674770016,58789.13674770016,NULL,NULL,NULL,58789.13674770016,58789.13674770016,58789.13674770016,NULL,NULL,0.0,0.0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,58789.13674770016,19.886262893676758,NULL,NULL,NULL,NULL,1035136740115015010,58789.13674770016,NULL,NULL,NULL,NULL,19.886262893676758,1035136740115015010,NULL,0.0,NULL,NULL,0.0,0.0)"""

