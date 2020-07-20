import datetime
from astropy.time import Time
from django.db import models

# Create your models here.

from frastro.frastro.core.database.mongodb.mongodb_manager import MongodbManager
from frastro.frastro.core.database.postgresql.postgresql_manager import PostgrestSQLManager
from frastro.frastro.core.utils.config import Config

import json


class Candidate():

    params={}

    def __init__(self):
        pass


    @staticmethod
    def getByID(id="",filter="",collection="lastdetections7"):
        db = MongodbManager()
        config = Config()
        dbconfig = config.getDatabase("mongodb")
        db.setDatabase(dbconfig["dbname"])
        db.setCollection(collection)
        if filter=="":
            filter={"id":{'$eq':id}}
        data=db.getData(filter=filter)

        return data

    @staticmethod
    def getAll(filter={},projection=""):
        db = MongodbManager()
        config = Config()
        dbconfig = config.getDatabase("mongodb")
        db.setDatabase(dbconfig["dbname"])
        db.setCollection("lastdetections7")
        data = db.getData(filter=filter,projection=projection)
        return data

    @staticmethod
    def getLightCurve(ztfid):
        db = MongodbManager()
        config = Config()
        dbconfig = config.getDatabase("mongodb")
        db.setDatabase(dbconfig["dbname"])
        db.setCollection("lastdetections7")
        filter={"id":ztfid}
        projection={"lightpeak.lightcurve":1}
        data = db.getData(filter=filter, projection=projection)
        return data




class ZTFSN():

    @staticmethod
    def getAll(filter={},projection=""):
        db = MongodbManager()
        config = Config()
        dbconfig = config.getDatabase("mongodb")
        db.setDatabase(dbconfig["dbname"])
        db.setCollection("tnssn")
        data = db.getData(filter=filter,projection=projection)
        return data


class Queries():

    FILTERS={'$project': {
                    'ztfid': '$id',
                    'tnsname': {
                        '$arrayElemAt': [
                            '$crossmatch.tns.name', 0
                        ]
                    },
                    'ra': '$ra',
                    'dec': '$dec',
                    'photoz': {
                        '$cond': [
                            {
                                '$gte': [
                                    '$best_photo_z.photo_z', 0
                                ]
                            }, '$best_photo_z.photo_z', -99
                        ]
                    },
                    'specz': {
                        '$cond': [
                            {
                                '$gte': [
                                    '$best_spec_z.spec_z', 0
                                ]
                            }, '$best_spec_z.spec_z', -99
                        ]
                    },
                    'parchive': {
                        '$cond': [
                            {
                                '$gte': [
                                    '$best_photo_z.photo_z', 0
                                ]
                            }, '$best_photo_z.photo_zarchive', 'any'
                        ]
                    },
                    'sarchive': {
                        '$cond': [
                            {
                                '$gte': [
                                    '$best_spec_z.spec_z', 0
                                ]
                            }, '$best_spec_z.spec_zarchive', 'any'
                        ]
                    },
                    'tns_type': {
                        '$arrayElemAt': [
                            '$crossmatch.tns.type', 0
                        ]
                    },
                    'tns_sn_z': {
                        '$arrayElemAt': [
                            '$crossmatch.tns.redshift', 0
                        ]
                    },
                    'lastmjd':{"$max":"$lightcurve.mjd"},
                    'alerce_cls': '$alerce_early_class',
                    'alerce_late_cls': '$alerce_late_class',
                    'lasair_cls': '$lasair_clas',
                    'gmax': {
                        '$arrayElemAt': [
                            '$lightpeak.stats.g.y', 1
                        ]
                    },
                    'rmax': {
                        '$arrayElemAt': [
                            '$lightpeak.stats.r.y', 1
                        ]
                    },
                    's_gmab': '$best_specz_gabmag',
                    's_rmab': '$best_specz_rabmag',
                    'p_rmab': '$best_photoz_rabmag',
                    'p_gmab': '$best_photoz_gabmag',
                    'g_state': '$lightpeak.status.g',
                    'r_state': '$lightpeak.status.r',

                    'broker': '$broker',
                    'nobsg': '$lightpeak.lightcurve.g.detections',
                    'nobsr': '$lightpeak.lightcurve.r.detections',

                    'sdis': {
                        '$cond': [
                            {
                                '$gte': [
                                    '$best_spec_z.spec_z', 0
                                ]
                            }, '$best_spec_z.spec_zsarcsec', -99
                        ]
                    },
                    'pdis': {
                        '$cond': [
                            {
                                '$gte': [
                                    '$best_photo_z.photo_z', 0
                                ]
                            }, '$best_photo_z.photo_zsarcsec', -99
                        ]
                    },
                    'ptype': {
                        '$cond': [
                            {
                                '$gte': [
                                    '$best_photo_z.photo_z', 0
                                ]
                            }, '$best_photo_z.photo_zobjtype', 'any'
                        ]
                    },
                    'stype': {
                        '$cond': [
                            {
                                '$gte': [
                                    '$best_spec_z.spec_z', 0
                                ]
                            }, '$best_spec_z.spec_zobjtype', 'any'
                        ]
                    },
                    "redshifts":"$redshift",
                    "redshifts_arc": "$redshift",
                    'mass':{"$arrayElemAt": ["$crossmatch.desi.mass_best", "$best_photo_z.photo_zidx_list"]},

                    'links': '$id'
                }
            }

    NEW_CANDIDATES = [
                {
                    '$match': {
                        'state': 'new',
                        "report.firstmjd":{"$gte": 0}
                    }
                },FILTERS
            ]
    ALL_CANDIDATES = [FILTERS]



    @staticmethod
    def get_new_candidates(days_ago=4):

        time = Time(datetime.datetime.now() - datetime.timedelta(days=int(days_ago)), format='datetime').to_value('mjd', 'long')

        query=[{
            '$match': {
                'state': 'new',
                "lightcurve.mjd": {"$gte": int(time)}

            }
        }, Queries().FILTERS]


        return query

    @staticmethod
    def get_massive_galaxies(days_ago=4):

        time = Time(datetime.datetime.now() - datetime.timedelta(days=int(days_ago)), format='datetime').to_value('mjd',
                                                                                                                  'long')

        query = [{
            '$match': {
                "$and":[{"best_photo_z.photo_zarchive":"desi"},
                {"crossmatch.desi.mass_best":{"$gte":10.5 }}],
                "report.firstmjd": {"$gte": int(time)}
            }
        }, Queries().FILTERS]
        query[1]['$project']["mass"] = {"$arrayElemAt": ["$crossmatch.desi.mass_best", "$best_photo_z.photo_zidx_list"]}
        query[1]['$project']["tns_snz"] = {"$arrayElemAt": ["$crossmatch.tns.redshift", 0]}
        query[1]['$project']["tns_sntype"] =  {"$arrayElemAt": ["$crossmatch.tns.object_type.name", 0]}
        return query

    @staticmethod
    def get_last_detections(days_ago=4):
        time = Time(datetime.datetime.now() - datetime.timedelta(days=int(days_ago)), format='datetime').to_value('mjd', 'long')
        #{"crossmatch.desi.mag_g": {$gte: 23}}
        query = [{
            '$match': {
                "lightcurve.mjd": {"$gte": int(time)}
            }
        }, Queries().FILTERS]

        return query

    @staticmethod
    def get_all_candidates():
        return Queries().ALL_CANDIDATES

    @staticmethod
    def get_all_detections():
        return Queries().ALL_CANDIDATES

    @staticmethod
    def get_best_candidates(days_ago=365,filter="lastdetections"):

        time = Time(datetime.datetime.now() - datetime.timedelta(days=int(days_ago)), format='datetime').to_value('mjd', 'long')

        filters=[]
        mainfilter={'$or': [
                    {
                        'best_specz_rabmag': {
                            '$lt': -20
                        }
                    }, {
                        'best_specz_gabmag': {
                            '$lt': -20
                        }
                    }, {
                        'best_photoz_gabmag': {
                            '$lt': -20
                        }
                    }, {
                        'best_photoz_rabmag': {
                            '$lt': -20
                        }
                    }
                ]
            }
        filters.append(mainfilter)
        if filter == "peak":
            peak_filter = {"$or": [
                {"lightpeak.stats.g.peakmjd": {"$gt": int(time)}},
                {"lightpeak.stats.r.peakmjd": {"$gt": int(time)}}
            ]}
            filters.append(peak_filter)

        elif filter == "lastdetection":
            lasobsfilter={'lastmjd': {'$gte': int(time)}}
            filters.append(lasobsfilter)


        matchfilter={
            "$match":{
                "$and":filters
            }
        }

        BEST_CANDIDATES = [matchfilter, Queries().FILTERS]

        return BEST_CANDIDATES


class QueryPostgresql():
    ps=None
    def __init__(self):
        self.ps = PostgrestSQLManager()

    @staticmethod
    def query(query):
        if query==None:
            print("error")
        else:
            query=QueryPostgresql()
            query.ps.select(query)

class QueryMongoDB():

    @staticmethod
    def getQuery(collection,filter={},projection={}):
        db = MongodbManager()
        config = Config()
        dbconfig = config.getDatabase("mongodb")
        db.setDatabase(dbconfig["dbname"])
        db.setCollection(collection)

        if filter != {} and filter != "":
            filter = json.loads(filter)
        else:
            filter = {}

        if projection != {} and projection != "":
            projection = json.loads(projection)
        else:
            projection={}

        data=db.getData(filter=filter,projection=projection)
        return data

    @staticmethod
    def getAggegation(collection,pipeline):
        db = MongodbManager()
        config = Config()
        dbconfig = config.getDatabase("mongodb")
        db.setDatabase(dbconfig["dbname"])


        data=db.command(collection=collection,pipeline=pipeline)
        return data

    @staticmethod
    def getLastDetections(collection, days_ago=5):
        return QueryMongoDB.getAggegation(collection=collection, pipeline=Queries.get_last_detections(days_ago=days_ago))

    @staticmethod
    def getNewCandidates(collection, days_ago=5):
        return QueryMongoDB.getAggegation(collection=collection, pipeline=Queries.get_new_candidates(days_ago=days_ago))

    @staticmethod
    def getAllCandidates(collection):
        return QueryMongoDB.getAggegation(collection=collection, pipeline=Queries.get_all_candidates())

    @staticmethod
    def getBestCandidates(collection, days_ago=5,filter=""):
        return QueryMongoDB.getAggegation(collection=collection, pipeline=Queries.get_best_candidates(days_ago=days_ago,filter=filter))

    @staticmethod
    def getMassiveGalaxies(collection, days_ago=5):
        return QueryMongoDB.getAggegation(collection=collection, pipeline=Queries.get_massive_galaxies(days_ago=days_ago))