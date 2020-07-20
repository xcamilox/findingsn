import dramatiq
import time
import logging

from datetime import datetime
import json
import os
import numpy as np
from astroquery.ned import Ned
from astroquery.sdss import SDSS
from astropy import coordinates
from matplotlib.backends.backend_pdf import PdfPages
from bson import ObjectId

from frastro.frastro.core.data.archive.lasair_archive_cp import LasairArchive
from frastro.frastro.core.data.archive.alerce_archive_cp import AlerceArchive
from frastro.frastro.core.data.archive.sdss_archive_cp import SDSSArchiveCP
from frastro.frastro.core.data.archive.ned_archive import NEDArchive
from frastro.frastro.core.data.archive.hsc_archive import HSCArchive
from frastro.frastro.core.database.mongodb.mongodb_manager import MongodbManager
from frastro.frastro.external.sncosmos_fit import SNCosmosFit
from frastro.frastro.core.data.archive.tns_archive_cp import TNSServices
from frastro.frastro.core.utils.config import Config
from frastro.frastro.core.utils.convertions_util import Convertion
from datamanager.best_candidates import BestCandidates
from frastro.frastro.core.data.archive.sussex_archive_cp import SussexArchive

from astropy.io import ascii
import pandas as pd
from astropy.table import Table, join, vstack, QTable
from astropy.coordinates import SkyCoord
from astropy import units as u

data_path= "/Users/camilojimenez/Projects/broker_data/"
LOG_FILENAME = data_path+'snlog.log'

#t!importatn: the  tasks file shoulbe call tasks to dramatick recongnice as valid actor
logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG)
logger = logging.getLogger(__name__)

current_collection = "lastdetections7"
STATE_INITIATED = "initiated"
STATE_RUNNING = "running"
STATE_COMPLETED = "completed"
STATE_ERROR = "error"
STATES_PIPELINE = [STATE_INITIATED,STATE_RUNNING,STATE_COMPLETED,STATE_ERROR]

@dramatiq.actor(max_retries=2,time_limit=600000000)
def checkLastDetections(**kwargs):
    # try:
    allrecords=0
    collection=current_collection

    days_ago=15
    if "collection" in kwargs.keys() and kwargs["collection"]!="":
        collection = kwargs["collection"]
    if "days_ago" in kwargs.keys() and kwargs["days_ago"]!="":
        days_ago=kwargs["days_ago"]
    if "IDpipeLine" in kwargs.keys() and kwargs["IDpipeLine"]!="":
        updatePipeline(kwargs["IDpipeLine"],"checkLastDetections",STATE_RUNNING)
    logger.info("checkLastDetections:: getting the last ZTF detections from brokers...")
    lasairarchive = LasairArchive()
    #coneecto to DATABASE

    db = MongodbManager()
    config=Config()
    dbconfig=config.getDatabase("mongodb")
    db.setDatabase(dbconfig["dbname"])
    db.setCollection(collection)

    #Get last candidates and update previews detection and light curves

    bestCandidates = BestCandidates()
    table_candidates, alerceDF, lasairDF = bestCandidates.searchCadidates(days_ago)

    #check if the new candidates is already in DB

    #get all zft id in and array to validate if exist into ddatabase and filter by
    listcandidates=table_candidates["id"]
    filter={"oid":{"$in":listcandidates.data.tolist()}}
    projection={"oid":1 ,"lastmjd":1 ,"last_update":1}

    current_data = db.getData(filter=filter, projection=projection)



    for remove_data in current_data:
        oid=remove_data["oid"]
        print("get info for ",oid)
        table_candidates.remove_rows(table_candidates["id"] == oid)

    #get desi photoz
    dataarchive = SussexArchive()
    desi_targetsvo, desi_targetstable = dataarchive.getDesiPhotoZfromTable(table_candidates)

    alerceTable = QTable.from_pandas(alerceDF)
    lasairTable = QTable.from_pandas(lasairDF)




    alerceTable.rename_column("oid","id")
    lasairTable.rename_column("oid", "id")
    alerceTable["id"] = alerceTable["id"].astype(str)
    lasairTable["id"] = lasairTable["id"].astype(str)
    desi_targetstable["id"] =desi_targetstable["id"].astype(str)

    desi_targetstable["desidec"].mask = False
    desi_targetstable["desira"].mask = False


    #calc separation desi source
    ra_ref = desi_targetstable["ramean"].tolist()
    dec_ref = desi_targetstable["decmean"].tolist()
    cref = SkyCoord(ra_ref, dec_ref, frame='icrs', unit='deg')
    ra_desi = desi_targetstable["desira"].tolist()
    dec_desi = desi_targetstable["desidec"].tolist()
    c1 = SkyCoord(ra_desi, dec_desi, frame='icrs', unit='deg')
    desi_distance = cref.separation(c1).arcsec
    desi_targetstable["separation"] = desi_distance


    #merge all table in one json to save in mongo

    desi_targetstable = Table(desi_targetstable, masked=False)
    alerceTable = Table(alerceTable, masked=False)
    lasairTable = Table(lasairTable, masked=False)
    alerceTable["broker"] = "alerce"
    lasairTable["broker"] = "lasair"

    update_alerce_table = join(alerceTable, lasairTable, join_type='outer', keys='id')
    merge_table = join(update_alerce_table, desi_targetstable, join_type='outer', keys='id')


    merge_table["desiid"] = merge_table["desiid"].astype(str)
    merge_table["field"] = merge_table["field"].astype(str)

    lastItems= merge_table.to_pandas()
    newItems = lastItems.fillna('', axis=1)
    dic_result = newItems.to_dict('records')

    newCandidates=0
    logger.info("checkLastDetections:: Ingested {0} candidates".format(str(len(dic_result))))
    allrecords=len(dic_result)
    for index,row in enumerate(dic_result):
        id=row["id"]
        print("saving candidate",id)
        row["comments"]={}
        row["snh_score"] = 0.0
        if row["broker_1"] != "":
            #alerce
            #row["pclassearly"]=row["pclassearly_1"]
            if row["broker_2"]!="":
                row["broker"]=row["broker_1"]+"/"+row["broker_2"]
            else:
                row["broker"] = row["broker_1"]
            row["meanra"]=row["meanra_1"]
            row["meandec"]=row["meandec_1"]
            row["lastmjd"]=row["lastmjd_1"]

        else:
            #lasair
            #row["pclassearly"] = row["pclassearly_2"]
            row["broker"] = row["broker_2"]
            row["meanra"] = row["meanra_2"]
            row["meandec"] = row["meandec_2"]
            row["lastmjd"] = row["lastmjd_2"]

        try:

            #remove duplicate fields
            #del row["pclassearly_1"]
            #del row["pclassearly_2"]
            del row["broker_1"]
            del row["broker_2"]
            del row["meanra_1"]
            del row["meandec_1"]

            del row["meanra_2"]
            del row["meandec_2"]

            del row["lastmjd_2"]
            del row["lastmjd_1"]

        except KeyError as er:
            print("key error",er,id)

        # check if already exist this candidate, if exist update light curve and run check list to alerts
        currentdata = db.getData(filter={"id": id}, projection={"nobs": 1, "last_update": 1, "id": 1})
        now = datetime.now().timestamp()
        rowupdated={}
        if len(currentdata) > 0:
            currentdata = currentdata[0]
            days_from_update = ((now - float(currentdata["last_update"])) / 3600) / 24
            if days_from_update < 0.6:
                print("last detections is the same, not getting enough to services update classify",id)
                logger.info("checkLastDetections:: {0} last detections is the same, not getting enough to services update classify".format(id))
                continue

        classification = getClassification(id)
        #peak = lasairarchive.getPeakLightCurve(classification["light_curve"]["candidates"])
        rowupdated["ra"] = row["meanra"]
        rowupdated["dec"] = row["meandec"]
        rowupdated["lasair_clas"]=classification["lasair_clas"]
        rowupdated["alerce_clas"]=classification["alerce_clas"]
        rowupdated["alerce_early_class"] = classification["alerce_early_class"]
        rowupdated["alerce_late_class"] = classification["alerce_late_class"]
        rowupdated["crossmatch"]={"lasair":classification["light_curve"]["crossmatches"],"check":False}

        rowupdated["lightcurve"] = classification["light_curve"]["candidates"]
        rowupdated["report"] = row
        rowupdated["broker"] = row["broker"]
        rowupdated["nobs"] = row["nobs"]
        rowupdated["lastmjd"] = row["lastmjd"]
        rowupdated["sigmara"] = row["sigmara"]
        rowupdated["sigmadec"] = row["sigmadec"]
        rowupdated["last_magpsf_g"] = row["last_magpsf_g"]
        rowupdated["last_magpsf_r"] = row["last_magpsf_r"]
        rowupdated["first_magpsf_g"] = row["first_magpsf_g"]
        rowupdated["first_magpsf_r"] = row["first_magpsf_r"]
        rowupdated["sigma_magpsf_g"] = row["sigma_magpsf_g"]
        rowupdated["sigma_magpsf_r"] = row["sigma_magpsf_r"]
        rowupdated["max_magpsf_g"] = row["max_magpsf_g"]
        rowupdated["max_magpsf_r"] = row["max_magpsf_r"]
        rowupdated["id"] = row["id"]



        #check if already exist this candidate, if exist update light curve and run check list to alerts
        currentdata=db.getData(filter={"id":id},projection={"nobs":1,"last_update":1,"id":1})
        now = datetime.now().timestamp()



        if len(currentdata)>0 :
            #update current data
            try:
                if currentdata[0]["nobs"] < rowupdated["nobs"]:
                    peak = lasairarchive.getPeakLightCurve(classification["light_curve"]["candidates"])
                    rowupdated["lightpeak"] = peak

                    update_query={"last_update":now,"lightcurve":rowupdated["lightcurve"],"lightpeak":peak,"lasair_clas":rowupdated["lasair_clas"],"alerce_clas":rowupdated["alerce_clas"],"nobs":rowupdated["nobs"],"state":"updated"}
                    update_id = db.update(filter={"id":id}, query={"$set":update_query})
                    print("updated source",id,update_id.raw_result)
                else:
                    print("last detections is the same, not getting enough to services update classify",id)
            except Exception as err:
                print("Error updated",id,currentdata[0]["nobs"],rowupdated["nobs"])
                logger.error("checkLastDetections:: {0} Error updated..".format(str(id)))

        else:
            peak = lasairarchive.getPeakLightCurve(classification["light_curve"]["candidates"])
            rowupdated["lightpeak"] = peak

            #insert new candidate
            print("save new candidate")
            rowupdated["state"]="new"
            rowupdated["last_update"] = now
            db.saveData(rowupdated)
            logger.info("checkLastDetections:: {0} Saved candidate with {1} observations".format(id,rowupdated["nobs"]))
            newCandidates+=1

    logger.info("checkLastDetections:: {0} candidates stored..".format(str(len(dic_result))))
    logger.info("checkLastDetections:: alerce table detections {0}".format(str(len(alerceTable))))
    logger.info("checkLastDetections:: lasair table detections {0}".format(str(len(lasairTable))))
    logger.info("checkLastDetections:: desi detections {0}".format(str(len(desi_targetstable))))
    logger.info("checkLastDetections:: new Candidates {0}".format(str(newCandidates)))


    db.saveData(data={"date":now,"newcandidates":newCandidates,"allrecords":allrecords,"alerce_records":len(alerceTable),"lasair_records":len(lasairTable),"desi_matchs":len(desi_targetstable),"process":"lastdetections"},collection="tasks")

    if "IDpipeLine" in kwargs.keys() and kwargs["IDpipeLine"]!="":
        updatePipeline(kwargs["IDpipeLine"],"checkLastDetections",STATE_COMPLETED)
    # except Exception as err:
    #     if "IDpipeLine" in kwargs.keys() and kwargs["IDpipeLine"]!="":
    #         updatePipeline(kwargs["IDpipeLine"],"checkLastDetections",STATE_ERROR,error=err.__str__)
    #     print("Error checkLastDetections !!!",err)



def classifyCandidate():
    #
    db = MongodbManager()
    config = Config()
    dbconfig = config.getDatabase("mongodb")
    db.setDatabase(dbconfig["dbname"])
    db.setCollection(current_collection)
    data=db.getData(filter={"$or":[{"lightpeak.lightcurve.g.magab": {"$exists": True}},{"lightpeak.lightcurve.r.magab": {"$exists": True}}]})


    for indx, row in enumerate(data):
        print("row",row["id"])
        db.setCollection("tnssn")
        probabilities={}
        filters=[]
        if ("g" in row["peak"]["stats"].keys() and "magab" in row["peak"]["stats"]["g"]) or ("r" in row["peak"]["stats"].keys() and "magab" in row["peak"]["stats"]["r"]):

            try:
                keys = row["peak"]["stats"]["g"]["magab"].keys()
            except Exception as err:
                keys = row["peak"]["stats"]["r"]["magab"].keys()

            for archive in keys:
                try:
                    maxg = min(row["peak"]["stats"]["g"]["magab"][archive])
                    filters.append({"peak.stats.g.abmag": {"$lte": maxg}})
                except Exception:
                    print("not g band",row["id"])

                try:
                    maxr = min(row["peak"]["stats"]["r"]["magab"][archive])
                    filters.append({"peak.stats.r.abmag": {"$lte": maxr}})
                except Exception:
                    print("not r band", row["id"])

                if len(filters) > 0:
                    classtypes=db.getData({"$or":filters},projection={"Redshift":1,"ObjType":1,"id":1,"peak":1,"DiscInternalName":1})

                    if len(classtypes)>0:
                        classify = []
                        for idx, classtype in enumerate(classtypes):
                            if idx >10:
                                break
                            data_classifier= {"redshift":classtype["Redshift"],"ObjType":classtype["ObjType"],"id":classtype["id"],"ztfid":classtype["DiscInternalName"]}

                            if "g" in classtype["peak"]["stats"]:
                                data_classifier["slope_g"]=classtype["peak"]["stats"]["g"]["slope"]
                                data_classifier["abmagpeak_g"]= min(classtype["peak"]["stats"]["g"]["abmag"])
                                data_classifier["magpeak_g"] = min(classtype["peak"]["stats"]["g"]["y"])

                            if "r" in classtype["peak"]["stats"]:
                                data_classifier["slope_r"] = classtype["peak"]["stats"]["r"]["slope"]
                                data_classifier["abmagpeak_r"] = min(classtype["peak"]["stats"]["r"]["abmag"])
                                data_classifier["magpeak_r"] = min(classtype["peak"]["stats"]["r"]["y"])


                            classify.append(data_classifier)
                        probabilities[archive]=classify
        if len(probabilities.keys()) > 0:
            try:
                db.setCollection(current_collection)
                upd=db.update(filter={"id":row["id"]},query={"$set":{"probabilities":probabilities}})

                logger.error("classifyCandidate:: classifier update {0}".format(row["id"]))
            except Exception as err:
                logger.error("classifyCandidate:: many candidates try to save for {0}".format(row["id"]))
        del probabilities



@dramatiq.actor(max_retries=1,time_limit=6000000)
def scoreCandidates(collection,filter={}):
    lasairarchive = LasairArchive()
    db = MongodbManager()
    config = Config()
    dbconfig = config.getDatabase("mongodb")
    db.setDatabase(dbconfig["dbname"])
    db.setCollection(collection)
    data = db.getData(filter=filter)
    for indx, row in enumerate(data):
        print("ID score",row["id"])



@dramatiq.actor(max_retries=1,time_limit=600000000)
def getPeaks(**kwargs):
    collection = current_collection
    filter = {"lightcurve": {"$exists": True}}
    projection = {}
    if "collection" in kwargs.keys() and kwargs["collection"] != "":
        collection = kwargs["collection"]

    if "filter" in kwargs.keys() and kwargs["filter"] != "":
        filter = kwargs["filter"]

    if "projection" in kwargs.keys() and kwargs["projection"] != "":
        projection = kwargs["projection"]

    if "IDpipeLine" in kwargs.keys() and kwargs["IDpipeLine"] != "":
        updatePipeline(kwargs["IDpipeLine"], "getPeaks", STATE_RUNNING)


    lasairarchive = LasairArchive()
    db = MongodbManager()
    config = Config()
    dbconfig = config.getDatabase("mongodb")
    db.setDatabase(dbconfig["dbname"])
    db.setCollection(collection)
    data = db.getData(filter=filter)
    for indx,row in enumerate(data):
        print("try to get peak",row["id"])
        if len(row["lightcurve"])>0:
            peak = lasairarchive.getPeakLightCurve(row["lightcurve"])
            query = {}
            query["best_photoz_gabmag"] = 999
            query["best_photoz_rabmag"] = 999
            query["best_specz_gabmag"] = 999
            query["best_specz_rabmag"] = 999
            if "Redshift" in row or ("redshift" in row and len(row["redshift"].keys())>0):
                redshift = []

                if "Redshift" in row:
                    z=row["Redshift"]
                    redshifts_archives=["tns"]
                else:
                    redshifts_archives=row["redshift"].keys()


                for z_key in redshifts_archives:
                    if z_key == "sncosmos":
                        if "best"in row["redshift"]["sncosmos"] and "redshift" in row["redshift"]["sncosmos"]["best"]:
                            z = row["redshift"]["sncosmos"]["best"]["redshift"]
                        else:
                            continue
                    else:
                        z=row["redshift"][z_key]

                    if "g" in peak["stats"].keys():
                        if "magab" not in peak["stats"]["g"]:
                            peak["stats"]["g"]["magab"]={}
                        peak["stats"]["g"]["magab"][z_key] = Convertion.aparentToAbsoluteMagnitud(peak["stats"]["g"]["y"],z=z).tolist()


                    if "r" in peak["stats"].keys():
                        if "magab" not in peak["stats"]["r"]:
                            peak["stats"]["r"]["magab"]={}
                        peak["stats"]["r"]["magab"][z_key] = Convertion.aparentToAbsoluteMagnitud(peak["stats"]["r"]["y"],
                                                                                           z=z).tolist()

                    if "g" in peak["lightcurve"].keys():
                        if "magab" not in peak["lightcurve"]["g"]:
                            peak["lightcurve"]["g"]["magab"]={}
                        peak["lightcurve"]["g"]["magab"][z_key] = Convertion.aparentToAbsoluteMagnitud(
                            peak["lightcurve"]["g"]["mag"], z=z).tolist()

                    if "r" in peak["lightcurve"].keys():
                        if "magab" not in peak["lightcurve"]["r"]:
                            peak["lightcurve"]["r"]["magab"]={}
                        peak["lightcurve"]["r"]["magab"][z_key] = Convertion.aparentToAbsoluteMagnitud(
                            peak["lightcurve"]["r"]["mag"], z=z).tolist()


                peaks=[]
                gmag=False
                rmag = False
                if "g" in peak["stats"].keys():
                    peak_g = peak["stats"]["g"]["peakmag"]
                    peaks.append(peak_g)
                    gmag = True
                if "r" in peak["stats"].keys():
                    peak_r = peak["stats"]["r"]["peakmag"]
                    peaks.append(peak_r)
                    rmag=True

                if "best_photo_z" in row.keys() and len(row["best_photo_z"])>0:
                    photoz = row["best_photo_z"]["photo_z"]
                    best_photomagab=Convertion.aparentToAbsoluteMagnitud(peaks, z=photoz).tolist()
                    if gmag:
                        query["best_photoz_gabmag"]=best_photomagab[0]
                    if rmag:
                        idxphotorbest = 1 if gmag else 0
                        query["best_photoz_rabmag"]=best_photomagab[idxphotorbest]

                if "best_spec_z" in row.keys() and len(row["best_spec_z"])>0:
                    specz = row["best_spec_z"]["spec_z"]
                    best_specmagab=Convertion.aparentToAbsoluteMagnitud(peaks, z=specz).tolist()
                    if gmag:
                        query["best_specz_gabmag"]=best_specmagab[0]
                    if rmag:
                        idxspecbest = 1 if gmag else 0
                        query["best_specz_rabmag"]=best_specmagab[idxspecbest]



            query["lightpeak"]= peak
            if "g" in peak["status"].keys():
                query["g_state"]= peak["status"]["g"]

            if "r" in peak["status"].keys():
                query["r_state"]= peak["status"]["r"]

            update=db.update(filter={"id":row["id"]},query={"$set":query})
            print("update peak ",row["id"],update)

    if "IDpipeLine" in kwargs.keys() and kwargs["IDpipeLine"] != "":
        updatePipeline(kwargs["IDpipeLine"], "getPeaks", STATE_COMPLETED)

def getClassification(ztfid):
    print("getting classification for:",ztfid)
    alercearchive = AlerceArchive()
    lasairarchive = LasairArchive()
    lightCurve = lasairarchive.getObjectInfo(ztfid)
    lasair_classification = ""
    alerce_classification = ""
    if "objectData" in lightCurve:
        if "classification" in lightCurve["objectData"]:
            lasair_classification = lightCurve["objectData"]["classification"]


    else:
        lightCurve_lasair = alercearchive.getLightCurve(ztfid)
        stats = alercearchive.getStats(ztfid)
        lasair_classification = "not in lasair"
        lightCurve = {"candidates":lightCurve_lasair["result"]["detections"],"crossmatches":[],"objectId":ztfid,"objectData":{"ramean":stats["result"]["stats"]["meanra"],"decmean":stats["result"]["stats"]["meandec"]}}
    alerce_classification = alercearchive.getProbabilities(ztfid)

    best_late = 0
    best_late_key = ""
    for late_key in alerce_classification["result"]["probabilities"]["late_classifier"]:
        #print(type(alerce_classification["result"]["probabilities"]["late_classifier"][late_key]), late_key)

        if type(alerce_classification["result"]["probabilities"]["late_classifier"][late_key]) is float and late_key != "classifier_version":
            if best_late < alerce_classification["result"]["probabilities"]["late_classifier"][late_key]:
                best_late = alerce_classification["result"]["probabilities"]["late_classifier"][late_key]
                best_late_key = str(late_key)[0:str(late_key).find("_")].upper()

    best_early = 0
    best_early_key = ""
    for early_key in alerce_classification["result"]["probabilities"]["early_classifier"]:
        #print(type(alerce_classification["result"]["probabilities"]["early_classifier"][early_key]), early_key)

        if type(alerce_classification["result"]["probabilities"]["early_classifier"][
                    early_key]) is float and early_key != "classifier_version":
            if best_early < alerce_classification["result"]["probabilities"]["early_classifier"][early_key]:
                best_early = alerce_classification["result"]["probabilities"]["early_classifier"][early_key]
                best_early_key = str(early_key)[0:str(early_key).find("_")].upper()
    if "probabilities" in alerce_classification["result"]:
        alerce_classification = alerce_classification["result"]["probabilities"]

    return {"light_curve":lightCurve,"lasair_clas":lasair_classification,"alerce_clas":alerce_classification,"alerce_early_class":best_early_key,"alerce_late_class":best_late_key}

#cross match a colletion by ra,dec
@dramatiq.actor(max_retries=1,time_limit=600000000)
def crossMatchCollection(**kwargs):
    collection = current_collection
    filter = {'crossmatch.check': False}
    projection = {}
    forcecrossmatch = False
    radio = 5
    if "collection" in kwargs.keys() and kwargs["collection"] !="":
        collection = kwargs["collection"]

    if "forcecrossmatch" in kwargs.keys() and kwargs["forcecrossmatch"] !="":
        forcecrossmatch = kwargs["forcecrossmatch"]

    if "filter" in kwargs.keys() and kwargs["filter"] !="":
        filter = kwargs["filter"]

    if "radio" in kwargs.keys() and kwargs["radio"] !="":
        radio = kwargs["radio"]

    if "projection" in kwargs.keys() and kwargs["projection"] !="":
        projection = kwargs["projection"]

    if "IDpipeLine" in kwargs.keys() and kwargs["IDpipeLine"]!="":
        updatePipeline(kwargs["IDpipeLine"],"crossMatchCollection",STATE_RUNNING)

    db = MongodbManager()
    config = Config()
    dbconfig = config.getDatabase("mongodb")
    db.setDatabase(dbconfig["dbname"])

    db.setCollection(collection)
    if "crossmatch" not in projection:
        projection["crossmatch"]=1
    if "id" not in projection:
        projection["id"] = 1
    if "ra" not in projection:
        projection["ra"] = 1
    if "dec" not in projection:
        projection["dec"] = 1


    data = db.getData(filter=filter, projection=projection)
    print("cross match source to update",len(data))
    for index,row in enumerate(data):

        if forcecrossmatch or ("crossmatch" not in row.keys() or row["crossmatch"]["check"] == False) :
            try:
                id = row["id"]

                logger.info("try cross match..." + id)
                print("try cross match..." + id,index,row["crossmatch"]["check"])
                ra = row["ra"]
                dec = row["dec"]
                #ra = row["ra"]
                #ra = row["dec"]
                current=row["crossmatch"]
                print("cross match",id)
                crossdata=crossMatch(ra,dec,radio=radio)
                crossdata["lasair"]=current["lasair"]
                crossdata["check"] = True
                logger.info("check follow up candidates and update ZTF light curves..."+id)
                updated=db.update(filter={"id":id}, query={"$set":{"crossmatch":crossdata}})
                print("id {0} updated {1}",id,updated)
            except Exception as err:
                #print("crossmatch error by"+id,err)
                print("error cross match",err)
                logger.error("crossMatchCollection:: Cant crossmatch  {0}".format(row["id"]))

    if "IDpipeLine" in kwargs.keys() and kwargs["IDpipeLine"]!="":
        updatePipeline(kwargs["IDpipeLine"],"crossMatchCollection",STATE_COMPLETED)



@dramatiq.actor(max_retries=1, time_limit=600000000)
def tnsUpdate(**kwargs):
    collection = current_collection
    projection = {}
    radio = 5
    db = MongodbManager()
    config = Config()
    dbconfig = config.getDatabase("mongodb")
    db.setDatabase(dbconfig["dbname"])
    filter = {}

    if "collection" in kwargs.keys() and kwargs["collection"] != "":
        collection = kwargs["collection"]
    if "filter" in kwargs.keys() and kwargs["filter"] !="":
        filter = kwargs["filter"]

    db.setCollection(collection)

    data = db.getData(filter=filter, projection=projection)
    cont=0
    for index, row in enumerate(data):
        ra = row["ra"]
        dec = row["dec"]
        tns = tnsxmatch(ra,dec,radio)
        if tns!=None:
            updated = db.update(filter={"id": row["id"]}, query={"$set": {"crossmatch.tns": tns}})
            print("tns update ",row["id"])
            cont+=1
    print("updated {} of {}".format(cont,len(data)))




def tnsxmatch(ra,dec,radio=5):
    # ================================
    # Get TNS cross Match
    # documentation on https://wis-tns.weizmann.ac.il/content/tns-getting-started
    # ================================
    try:
        tnsService = TNSServices()
        tnsdata = tnsService.searchOBJ(ra, dec,radio=radio)
        tns_crossmatch = []
        print("tns crossmatch", len(tnsdata['data']['reply']))
        if len(tnsdata['data']['reply']) > 0:

            # find relate photometry and spectroscopy reported in tns
            for items in tnsdata['data']['reply']:
                objname = items["objname"]
                content_data = tnsService.get_data_object(objname)["data"]["reply"]
                ra_tns = content_data["radeg"]
                dec_tns = content_data["decdeg"]
                cref = SkyCoord([ra], [dec], frame='icrs', unit='deg')
                c1 = SkyCoord([ra_tns], [dec_tns], frame='icrs', unit='deg')
                tns_distance = cref.separation(c1).arcsec
                content_data["separation"] = tns_distance[0]
                tns_crossmatch.append(content_data)

            return tns_crossmatch
        else:
            return None
    except Exception as errtns:
        print("error getting TNS data", errtns)
        return None



#ra, dec in degrees , radiu in arcsec
def crossMatch(ra,dec,radio=15):

    crossmatch={}

    try:
        hscarchive=HSCArchive()
        hscdata=hscarchive.search(ra,dec,radio)
        if len(hscdata) > 0:
            hsccrossmatch=hscdata.fillna('', axis=1).to_dict('r')
            crossmatch["hsc"]=hsccrossmatch
    except Exception as errhsc:
        print("error getting HSC data", errhsc)


    tns=tnsxmatch(ra,dec,radio)
    if tns!= None:
        crossmatch["tns"]=tns




    # ================================
    # crossmatch with alerce catsHTM
    # documentation on https://alerceapi.readthedocs.io/en/latest/catshtm.html
    # ================================
    try:
        alercearchive = AlerceArchive()
        alerce_crossmatch = alercearchive.crossMatch(ra, dec, radio)
        print("alerce crossmatch",len(alerce_crossmatch))
        if len(alerce_crossmatch)>0:
            crossmatch["alerce"] = alerce_crossmatch
    except Exception as erralerce:
        print("error getting Alerce data", erralerce)

    #================================
    # NED crossmatch by ra dec and radio
    # documentation on https://astroquery.readthedocs.io/en/latest/ned/ned.html
    # ================================
    try:

        ned_df = NEDArchive.getCatalog(ra,dec,radio)
        ned_match = ned_df.fillna('', axis=1)
        ned_match["refcode"]=ned_match["refcode"].str.decode("utf-8")
        ned_match["pretype"] = ned_match["pretype"].str.decode("utf-8")
        ned_match["zflag"] = ned_match["zflag"].str.decode("utf-8")
        ned_match["zrefcode"] = ned_match["zrefcode"].str.decode("utf-8")
        ned_crossmatch = ned_match.to_dict("r")
        print("ned crossmatch", len(ned_crossmatch))
        if len(ned_crossmatch)> 0:

            crossmatch["ned"] = ned_crossmatch

        # co = coordinates.SkyCoord(ra=ra, dec=dec, unit=(u.deg, u.deg))
        # ned_df = Ned.query_region(co, radius=radio * u.arcsec).to_pandas()
        # # remove No. column due key field error saving in mongo DB
        # ned_df = ned_df.drop(columns="No.")
        # # decode bite string to single string to save on database
        # ned_df['Object Name'] = ned_df['Object Name'].str.decode("utf-8")
        # ned_df['Type'] = ned_df['Type'].str.decode("utf-8")
        # ned_df['Redshift Flag'] = ned_df['Redshift Flag'].str.decode("utf-8")
        # ned_df['Magnitude and Filter'] = ned_df['Magnitude and Filter'].str.decode("utf-8")
        # #removo nan values due JSON can to understand this type of value in frontend
        # ned_crossmatch = ned_df.fillna('', axis=1).to_dict('r')
        # print("ned crossmatch",len(ned_crossmatch))
        # if len(ned_crossmatch)>0:
        #     crossmatch["ned"] = ned_crossmatch
    except Exception as errtns:
        print("error getting NED data",errtns)
    # ================================
    # Cross Match with SDSS last data archive (16)
    # ================================
    try:
        sdssarchive = SDSSArchiveCP()
        sdss_redshift = sdssarchive.getRedshift(ra, dec,radio=radio)
        print("sdss crossmatch", len(sdss_redshift))
        if len(sdss_redshift.index)>0:
            sdss_redshift["objid"] = sdss_redshift["objid"].astype(str)
            sdss_redshift["specobjid"] = sdss_redshift["specobjid"].astype(str)
            sdss_crossmatch = sdss_redshift.fillna('', axis=1).to_dict('r')

            crossmatch["sdss"] = sdss_crossmatch
    except Exception as errsdss:
        print("error getting SDSS data",errsdss)

    # ================================
    # Get TNS cross DESI PhotoZ
    # documentation on https://wis-tns.weizmann.ac.il/content/tns-getting-started
    # ================================

    try:
        desiService = SussexArchive()
        desi_table = desiService.getDESI(ra, dec, radio)
        desi_table["field"] = desi_table["field"].astype(str)
        desi_table["id"] = desi_table["id"].astype(str)
        desi_table["type"] = desi_table["type"].astype(str)

        desi_table["ra"].mask = False
        desi_table["type"].mask = False
        desi_table["dec"].mask = False

        # calc separation desi source
        ra_ref = desi_table["ra"].tolist()
        dec_ref = desi_table["dec"].tolist()
        cref = SkyCoord([ra], [dec], frame='icrs', unit='deg')
        c1 = SkyCoord(ra_ref, dec_ref, frame='icrs', unit='deg')
        desi_distance = cref.separation(c1).arcsec
        desi_table["separation"] = desi_distance

        # merge all table in one json to save in mongo

        desi_targetstable = Table(desi_table, masked=False)
        desi_df = desi_targetstable.to_pandas()
        desi_match = desi_df.fillna('', axis=1)
        desi_crossmatch = desi_match.to_dict("r")
        print("desi crossmatch", len(desi_crossmatch))
        if len(desi_crossmatch) > 0:
            crossmatch["desi"] = desi_crossmatch
    except Exception as errdesi:
        print("error getting DESI data",errdesi)

    # row["lightpeak"] = peak
    # row["lightcurve"] = lightCurve
    # row["crossmath"] = crossmatch
    # redshifts = getRedshifts(row)
    #
    # row["redshifts"] = redshifts
    # rowupdated = calcAbsoluteMagnitud(row)
    return crossmatch

def lightCurveABMagnitud(candidate):

    if "redshifts" in candidate:
        rowupdated = calcAbsoluteMagnitud(candidate)
    else:
        redshifts = getRedshifts(candidate)
        candidate["redshifts"] = redshifts
        rowupdated = calcAbsoluteMagnitud(candidate)

@dramatiq.actor(max_retries=1,time_limit=600000000)
def calcRedshiftCandidates(**kwargs):
    collection = current_collection
    if "collection" in kwargs.keys() and kwargs["collection"] != "":
        collection = kwargs["collection"]
    filter={}
    if "filter" in kwargs.keys() and kwargs["filter"] != "":
        filter = kwargs["filter"]
    if "IDpipeLine" in kwargs.keys() and kwargs["IDpipeLine"] != "":
        updatePipeline(kwargs["IDpipeLine"], "calcRedshiftCandidates", STATE_RUNNING)

    db = MongodbManager()
    config = Config()
    dbconfig = config.getDatabase("mongodb")
    db.setDatabase(dbconfig["dbname"])
    db.setCollection(collection)
    data = db.getData(filter=filter)
    for index,row in enumerate(data):
        # try:
        print("get redshift to " + row["id"])
        if "crossmatch" in row.keys() and len(row["crossmatch"].keys())>0:
            good_spec,good_photo,photo_spec,redshift = getRedshifts(row["crossmatch"])
            sncos=False
            if len(redshift)<=0 and sncos:
                if "g" in row["lightpeak"]["lightcurve"] and "r" in row["lightpeak"]["lightcurve"]:
                    if row["lightpeak"]["lightcurve"]["g"]["detections"]>=2 and row["lightpeak"]["lightcurve"]["r"]["detections"]>=2:
                        snclasifier = getSNCosmosFit(row["lightcurve"],id=row["id"])
                        if snclasifier != None:
                            redshift["sncosmos"]=snclasifier
            query={"redshift": redshift, "best_photo_z": good_photo, "best_spec_z": good_spec}
            query.update(photo_spec)

            up = db.update(filter={"id": row["id"]},query={"$set":query})


            print("udpdate " + row["id"])
        # except Exception as ex:
        #     logger.error("calcRedshiftCandidates:: Cant calculate Redshift for {0} error {1}".format(row["id"],str(ex)))


    if "IDpipeLine" in kwargs.keys() and kwargs["IDpipeLine"] != "":
        updatePipeline(kwargs["IDpipeLine"], "calcRedshiftCandidates", STATE_COMPLETED)

def getSNCosmosFit(lightcurve_json,id="none"):
    base_data_path = "/Users/camiloj/projects/broker_data/"
    file_path_report = "/Users/camiloj/projects/broker_data/reports/"
    try:
        sn_classifier = SNCosmosFit()
        tablecurve = sn_classifier.getTableFromJson(lightcurve_json)
        pp = PdfPages(file_path_report + id + '.pdf')
        usfulmodels=["snana-2004hx","nugent-sn1a","nugent-sn91t","nugent-sn91bg","nugent-sn1bc","nugent-hyper","nugent-sn2p",'nugent-sn2l','nugent-sn2n','s11-2004hx','s11-2005lc','s11-2005hl','s11-2005hm',"salt2","snf-2011fe","hsiao","s11-2005gi","s11-2006fo","s11-2006jo","s11-2006jl"]
        allmodels,best = sn_classifier.fitLightCurve(tablecurve,title=id, redshift_range=(0.0,0.5),report=pp,usemodels=usfulmodels)
        pp.close()
        return {"best":best,"all":allmodels}
    except Exception as err:
        logger.error("getSNCosmosFit:: cant fit sncosmo error {1}".format(str(err)))
        return {}

@dramatiq.actor(max_retries=2)
def calcABMagCandidates():
    db = MongodbManager()
    config = Config()
    dbconfig = config.getDatabase("mongodb")
    db.setDatabase(dbconfig["dbname"])
    db.setCollection(current_collection)
    data = db.getData()
    for index,row in enumerate(data):
        try:
            if "crossmatch" in row.keys() and len(row["crossmatch"])>0:
                ab_mags = calcAbsoluteMagnitud(row)
                db.update(filter={"id":row["id"]},query={"$set":{"abmag":ab_mags}})
                logger.info("calcABMagCandidates:: updated {1}".format(row["id"]))
        except Exception as ex:
            logger.error("calcABMagCandidates:: error getting abmags {0} error {1}".format(row["id"],str(ex)))



@dramatiq.actor(max_retries=2)
def calcABMagnitud():
    db = MongodbManager()
    config = Config()
    dbconfig = config.getDatabase("mongodb")
    db.setDatabase(dbconfig["dbname"])
    db.setCollection("tnssn")
    data=db.getData(filter={"Redshift":{'$gt':0},"DiscInternalName":{"$regex":'^ZTF'}},projection={"DiscInternalName":1,"lightcurve.candidates":1,"Redshift":1,"id":1,"Name":1})
    for index,row in enumerate(data):
        try:
            dt=pd.DataFrame(row["lightcurve"]["candidates"])
            magsd=dt["magpsf"].tolist()
            ab=Convertion.aparentToAbsoluteMagnitud(magsd,z=row["Redshift"])
            db.update({"id":row["id"]},query={"$set":{"abmag":ab.tolist()}})

        except Exception as err:
            logger.error("calcABMagnitud:: Cant calculate ABmagnituds for {0}".format(row["id"]))


def calcAbsoluteMagnitud(candidate):
    magAb={}
    if "redshifts" in candidate:
        if "lightpeak" in candidate:

                if "g" in candidate["lightpeak"]["peak"]:
                    maxg = candidate["lightpeak"]["peak"]["g"][1]
                    ming = candidate["lightpeak"]["min_det"]["g"][1]

                if "r" in candidate["lightpeak"]["peak"]:
                    maxr = candidate["lightpeak"]["peak"]["r"][1]
                    minr = candidate["lightpeak"]["min_det"]["r"][1]

        all_observations=[]
        if "lightcurve" in candidate:
            for index, observation in enumerate(candidate["lightcurve"]["candidates"]):
                if "drb" in observation:
                    mjd = observation["mjd"]
                    magpsf = observation["magpsf"]
                    sigmapsf = observation["sigmapsf"]
                    fid = observation["fid"]
                    all_observations.append([mjd,magpsf,sigmapsf,fid])
        all_observations=np.array(all_observations)
        df_allobs=pd.DataFrame(all_observations,columns=["mjd","magpsf","sigmapsf","fid"])
        peaks={}
        for redshift in candidate["redshifts"]:

            if type(candidate["redshifts"][redshift]) is float:
                abs = Convertion.aparentToAbsoluteMagnitud(all_observations[:,1],z=candidate["redshifts"][redshift])

                if "g" in candidate["lightpeak"]["peak"]:
                    absPeakg = Convertion.aparentToAbsoluteMagnitud([maxg,  ming],
                                                                   z=candidate["redshifts"][redshift])
                    peaks[redshift] = {"peakg": absPeakg[0],  "ming": absPeakg[1]}

                if "r" in candidate["lightpeak"]["peak"]:
                    absPeakr = Convertion.aparentToAbsoluteMagnitud([maxr, minr],
                                                                   z=candidate["redshifts"][redshift])
                    peaks[redshift] = {"peakr": absPeakr[0], "ming": absPeakr[1]}


                df_allobs[redshift] = abs
        lightcurveab=df_allobs.to_dict(orient="list")
        best={"photo":{},"spec":{}}
        if "photo_z" in candidate.keys() and len(candidate["photo_z"].keys())>0 :
            best_photoz = candidate["photo_z"]["photo_z"]
            if "g" in candidate["lightpeak"]["peak"]:
                best_pmaggab = Convertion.aparentToAbsoluteMagnitud([maxg, ming],z=best_photoz)
                best["photo"]["g"] = best_pmaggab


            if "r" in candidate["lightpeak"]["peak"]:
                best_pmagrab = Convertion.aparentToAbsoluteMagnitud([maxr, minr],z=best_photoz)
                best["photo"]["r"] = best_pmagrab

        if "spec_z" in candidate.keys() and len(candidate["spec_z"].keys())>0 :
            best_specz = candidate["spec_z"]["spec_z"]
            if "g" in candidate["lightpeak"]["peak"]:
                best_smaggab = Convertion.aparentToAbsoluteMagnitud([maxg, ming], z=best_specz)
                best["spec"]["g"] = best_smaggab

            if "r" in candidate["lightpeak"]["peak"]:
                best_smagrab = Convertion.aparentToAbsoluteMagnitud([maxr, minr], z=best_specz)
                best["spec"]["r"] = best_smagrab

        abmags={"lightcurve":lightcurveab,"peak":peaks,"best":best}
        return abmags


def getRedshifts(candidate):

    result={}
    photo_z = []
    spec_z = []

    if "hsc" in candidate.keys():
        if len(candidate["hsc"])>0:
            for idx,row in enumerate(candidate["hsc"]):
                if "photoz_best" in row.keys() and row["photoz_best"] != None and row["photoz_best"] != "":
                    result["hsc_photoz_"+str(idx)] = float(row["photoz_best"])
                    prob=0
                    type = "galaxy"
                    if float(row["prob_gal"]) > prob:
                        type="galaxy"
                        prob=float(row["prob_gal"])

                    if float(row["prob_qso"]) > prob:
                        type="QSO_AGN"
                        prob=float(row["prob_qso"])

                    if float(row["prob_star"]) > prob:
                        type="STAR"

                    photo_z.append(["hsc", row["separation"], float(row["photoz_best"]), float(row["photoz_std_best"]), type,
                                    "hsc_photoz " + row["catalog"], int(idx)])



                if "specz_redshift" in row.keys() and row["specz_redshift"] != None and row["specz_redshift"] != "":
                    result["hsc_specz_" + str(idx)] = float(row["specz_redshift"])
                    spec_z.append(["hsc", row["separation"], float(row["specz_redshift"]), float(row["specz_redshift_err"]), "unknown",
                                    row["specz_name"], int(idx)])


    if "lasair" in candidate.keys():
        if len(candidate["lasair"])>0:
            for idx,row in enumerate(candidate["lasair"]):
                if "photoZ" in row.keys() and row["photoZ"] != None and row["photoZ"] != "":
                    result["lasair_photoz_"+str(idx)] = row["photoZ"]
                    photo_z.append(["lazair",row["separationArcsec"],row["photoZ"],float(0.),row["catalogue_object_type"],row["catalogue_table_name"],int(idx)])

    if "desi" in candidate.keys():
        if len(candidate["desi"])>0:
            for idx, row in enumerate(candidate["desi"]):
                if "photo_z" in row.keys():
                    result["desi_photoz_"+str(idx)] = row["photo_z"]
                    photo_z.append(["desi", row["separation"], row["photo_z"],row["photo_zerr"], row["type"],"desi_photoz",int(idx)])

    if "ned" in candidate.keys():
        #nomenclature for the type object
        # http://ned.ipac.caltech.edu/srcnom
        if len(candidate["ned"]) > 0:
            for idx,row in enumerate(candidate["ned"]):
                if "z" in row.keys() and row["z"] !=None and row["z"]!="":
                    nedtype=""
                    if row["zflag"] != "" and row["zflag"] != None:
                        if row["zflag"] == "SPEC":
                            nedtype="spec"
                            spec_z.append(["ned", row["separation"], row["z"],row["zunc"], row["pretype"], row["prefname"],int(idx)])
                        else:
                            nedtype = "photo"
                            photo_z.append(["ned", row["separation"], row["z"],row["zunc"], row["pretype"], row["prefname"],int(idx)])
                    result["ned_"+nedtype+"z_"+str(idx)] = row["z"]


    if "sdss" in candidate.keys():
        if len(candidate["sdss"]) > 0:
            for idx,row in enumerate(candidate["sdss"]):

                if row["spec_z"] !="":
                    result["sdss_specz_"+str(idx)] = row["spec_z"]
                    spec_z.append(["sdss", row["separation"], row["spec_z"], row["spec_zerr"], row["spec_type"], str(row["ra"])+" "+str(row["dec"]),int(idx)])
                if row["photo_z"] != "":
                    result["sdss_photoz_"+str(idx)] = row["photo_z"]
                    photo_z.append(["sdss", row["separation"], row["photo_z"], row["photo_zerr"], SDSSArchiveCP.getPhotoType(row["photo_type"]), str(row["ra"])+" "+str(row["dec"]),int(idx)])

    if "tns" in candidate.keys() :
        if len(candidate["tns"]) > 0:
            for idx, row in enumerate(candidate["tns"]):
                if row["redshift"]!= None and row["redshift"]!="":
                    result["tns_z_"+str(idx)] = row["redshift"]
                    spec_z.append(["tns", row["separation"], row["redshift"], float(0.0), row["name_prefix"],str(row["internal_names"]), int(idx)])


    photoz=[]
    specz=[]
    good_spec = {}
    good_photo = {}
    if len(spec_z)>0:
        pd_spec = pd.DataFrame(spec_z,
                               columns=["spec_zarchive", "spec_zsarcsec", "spec_z", "spec_zerr", "spec_zobjtype",
                                        "spec_zref", "spec_zidx_list"])
        best_pd_spec = pd_spec.loc[pd_spec["spec_z"]>0]
        if len(best_pd_spec)>0:
            good_spec = best_pd_spec.loc[best_pd_spec["spec_zsarcsec"]==best_pd_spec["spec_zsarcsec"].min()].iloc[0].to_dict()
            good_spec["spec_zidx_list"] = int(good_spec["spec_zidx_list"])
            specz = pd_spec.to_dict("r")
    if len(photo_z)>0:
        pd_photo = pd.DataFrame(photo_z,
                            columns=["photo_zarchive", "photo_zsarcsec", "photo_z", "photo_zerr", "photo_zobjtype",
                                     "photo_zref", "photo_zidx_list"])
        best_pd_photo = pd_photo.loc[pd_photo["photo_z"] > 0]
        if len(best_pd_photo)>0:
            good_photo = best_pd_photo.loc[best_pd_photo["photo_zsarcsec"] == best_pd_photo["photo_zsarcsec"].min()].iloc[0].to_dict()
            good_photo["photo_zidx_list"] = int(good_photo["photo_zidx_list"])
            photoz = pd_photo.to_dict("r")





    return good_spec,good_photo,{"photoz":photoz,"specz":specz},result

@dramatiq.actor(max_retries=2)
def followUpCandidates():
    logger.info("check follow up candidates and update ZTF light curves...")

def createPipeline(tasks):
    db = MongodbManager()
    config = Config()
    dbconfig = config.getDatabase("mongodb")
    db.setDatabase(dbconfig["dbname"])

    id = db.saveData(tasks,collection="pipelines")
    logger.info("createPipeline:: created PIPELINE {0} ".format(str(id)))
    return id

@dramatiq.actor(max_retries=2)
def runPipelines(id):
    filter={'_id': ObjectId(id)}

    db = MongodbManager()
    config = Config()
    dbconfig = config.getDatabase("mongodb")
    db.setDatabase(dbconfig["dbname"])
    data = db.query(filter=filter, collection="pipelines")

    tasks = data[0]["tasks"]
    df = pd.DataFrame(tasks)
    df.sort_values(by=["order"])

    current_task=-1
    for index,task in df.iterrows():
        if current_task == task["order"] or current_task<0:
            if task["state"] in STATES_PIPELINE:
                if task["state"] == STATE_COMPLETED or task["state"]==STATE_ERROR:
                    if task["state"]==STATE_ERROR and task["skip"]:
                        current_task = task["order"]+1
                    else:
                        current_task = task["order"] + 1
                elif task["state"] == STATE_RUNNING or task["state"] == STATE_INITIATED:
                    current_task = task["order"]

            else:
                #init task
                updatePipeline(id,task["action"],STATE_INITIATED)
                task["params"]["IDpipeLine"] = id
                logger.info("runPipelines:: Run {0} action ".format(task["action"]))
                globals()[task["action"]].send(**task["params"])
                if not task["skip"]:
                    break



def updatePipeline(pipelineID, taskname, status, error=""):
    #pipelineID=ObjectId(pipelineID)
    db = MongodbManager()
    config = Config()
    dbconfig = config.getDatabase("mongodb")
    db.setDatabase(dbconfig["dbname"])

    now= datetime.now().timestamp()

    filter={'_id': ObjectId(pipelineID),"tasks.action":taskname}
    query={"tasks.$.state":status}
    db.update(filter=filter, query={"$set":query}, collection="pipelines")

    msg=""
    filter = {'_id': ObjectId(pipelineID)}
    if error != "":
        msg=error
    query = {"$addToSet": {"activities": {"task": taskname, "state": status, "date": now,"msg":msg}}}
    taskup = db.update(filter=filter, query=query, collection="pipelines")
    logger.info("updatePipeline:: Updated {0} task to {1}".format(taskname,status))

    if status == STATE_COMPLETED or status == STATE_ERROR:
        #runPipelines(pipelineID)
        runPipelines.send(pipelineID)
