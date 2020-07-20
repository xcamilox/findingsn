from django.shortcuts import render
from django.http import HttpResponse, JsonResponse
from rest_framework import viewsets
from rest_framework.decorators import api_view, renderer_classes

from rest_framework.response import Response
from rest_framework.renderers import JSONRenderer
# Create your views here.
from frastro.frastro.core.data.archive.lasair_archive_cp import LasairArchive
import numpy as np
from datamanager.models import Candidate,ZTFSN,QueryMongoDB
from datamanager.postgres_model import PosgrestModel


from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from rest_framework.parsers import JSONParser
from frastro.frastro.core.database.mongodb.mongodb_manager import MongodbManager
from frastro.frastro.core.utils.config import Config
from taskapp.tasks import crossMatch as datacrossmatch


@api_view(['GET','POST'])
@renderer_classes((JSONRenderer,))
def getLastLasair(request):
    lasair=LasairArchive()
    lastItems=lasair.getLastDetections(days_ago=4)
    if request.GET.__contains__('jobid'):
        jobid = request.GET['jobid']

    else:
        status = {"error": "job id is required"}
    newItems=lastItems.fillna('',axis=1)
    # lastItems.replace(np.nan, '', regex=True)
    dic_result=newItems.to_dict('records')
    # for index,row in enumerate(dic_result):
    #     for key in row.keys():
    #         if math.isnan(row[key]):
    #             row[key]="nan"
    return JsonResponse(dic_result,safe=False)


@csrf_exempt
def getByID(request,ztfid):
    objID=""
    print(request.method)
    if ztfid != None:
        objID = ztfid
    else:
        error = {"error": "ZTF ID is required"}
        return JsonResponse(error, safe=False)

    candidate=Candidate.getByID(objID)
    return JsonResponse(candidate, safe=False)

@api_view(['GET','POST'])
@renderer_classes((JSONRenderer,))
def getLightCurve(request):

    if request.POST.__contains__('ztfid'):
        ztfid = request.POST['ztfid']
    if request.GET.__contains__('ztfid'):
        ztfid = request.GET['ztfid']

    if ztfid != None:
        objID = ztfid
    else:
        error = {"error": "ZTF ID is required"}
        return JsonResponse(error, safe=False)

    #candidate=Candidate.getLightCurve(objID)
    model = PosgrestModel()
    candidate = model.getLighCurve(ztfid)

    return JsonResponse(candidate, safe=False)


@csrf_exempt
def crossMatch(request,ra,dec,radio):
    ra=float(ra)
    dec=float(dec)
    radio=int(radio)
    data=datacrossmatch(ra,dec,radio)

    return JsonResponse(data, safe=False)


@csrf_exempt
def lastcandidates(request):
    collection = "lastdetections7"
    days_ago = 5
    if request.GET.__contains__('days'):
        days_ago=request.GET["days"]
    if request.POST.__contains__('days'):
        days_ago=request.POST["days"]
    currentdata = QueryMongoDB.getNewCandidates(collection=collection,days_ago=days_ago)
    return JsonResponse(currentdata,safe=False)


@csrf_exempt
def lastDetections(request):
    collection = "lastdetections7"
    days_ago = 5
    if request.GET.__contains__('days'):
        days_ago=request.GET["days"]
    if request.POST.__contains__('days'):
        days_ago=request.POST["days"]
    currentdata = QueryMongoDB.getLastDetections(collection,days_ago=days_ago)
    return JsonResponse(currentdata,safe=False)


@csrf_exempt
def sqllastdetections(request):
    collection = "lastdetections7"
    days_ago = 5
    if request.GET.__contains__('days'):
        days_ago=request.GET["days"]
    if request.POST.__contains__('days'):
        days_ago=request.POST["days"]
    model = PosgrestModel()
    currentdata=model.getLasObjects(days_ago)
    return JsonResponse(currentdata,safe=False)


@csrf_exempt
def allcandidates(request):
    collection = "lastdetections7"
    currentdata = QueryMongoDB.getAllCandidates(collection=collection)
    return JsonResponse(currentdata,safe=False)


@csrf_exempt
def massiveGalaxies(request):
    collection = "lastdetections7"
    days_ago = 5
    if request.GET.__contains__('days'):
        days_ago = request.GET["days"]
    if request.POST.__contains__('days'):
        days_ago = request.POST["days"]

    currentdata = QueryMongoDB.getMassiveGalaxies(collection=collection, days_ago=days_ago)
    return JsonResponse(currentdata, safe=False)

@csrf_exempt
def SQLQuery(request):
    if request.GET.__contains__('query'):
        query=request.GET["query"]
    if request.POST.__contains__('query'):
        query=request.POST["query"]
    model = PosgrestModel()
    currentdata = model.getQuery(query)

    return JsonResponse(currentdata, safe=False)


@csrf_exempt
def bestcandidates(request):
    collection = "lastdetections7"
    days_ago = 5
    if request.GET.__contains__('days'):
        days_ago=request.GET["days"]
    if request.POST.__contains__('days'):
        days_ago=request.POST["days"]

    filter = ""
    if request.GET.__contains__('filter'):
        filter = request.GET["filter"]
    if request.POST.__contains__('filter'):
        filter = request.POST["filter"]
    currentdata = QueryMongoDB.getBestCandidates(collection=collection,days_ago=days_ago,filter=filter)
    return JsonResponse(currentdata,safe=False)

@csrf_exempt
def currentSN(request):
    currentdata=ZTFSN.getAll(filter={"Redshift":{'$gt':0},"DiscInternalName":{"$regex":'^ZTF'}},projection={"Name":1,"RA":1,"DEC":1,"ObjType":1,"Redshift":1,"HostName":1,"HostRedshift":1,"DiscInternalName":1,"lightcurve":1,"abmag":1,"Sender":1,"ClassifyingGroup":1})
    data={'data':currentdata}
    return JsonResponse(data,safe=False)

@api_view(['GET','POST'])
@renderer_classes((JSONRenderer,))
def runQuery(request):
    filter = {}
    projection = {}
    if request.GET.__contains__('collection') or request.POST.__contains__('collection'):
        colletion = request.GET['collection'] if request.GET.__contains__('collection') else request.POST['collection']
        if colletion =="":
            return JsonResponse({"error": "collection is required"}, safe=False)
    else:
        return JsonResponse({"error":"collection is required"}, safe=False)

    if request.GET.__contains__('filter') or request.POST.__contains__('filter'):
        filter = request.GET['filter'] if request.GET.__contains__('filter') else request.POST['filter']

    if request.GET.__contains__('projection') or request.POST.__contains__('projection'):
        projection = request.GET['projection'] if request.GET.__contains__('projection') else request.POST['projection']
    try:

        data=QueryMongoDB.getQuery(colletion,filter=filter,projection=projection)
        return JsonResponse(data, safe=False)
    except Exception as err:
        return JsonResponse({"error": "syntaxis error"}, safe=False)


@api_view(['GET', 'POST'])
@renderer_classes((JSONRenderer,))
def runAggregation(request):
    if request.GET.__contains__('collection') or request.POST.__contains__('collection'):
        colletion = request.GET['collection'] if request.GET.__contains__('collection') else request.POST['collection']
        if colletion == "":
            return JsonResponse({"error": "collection is required"}, safe=False)
    else:
        return JsonResponse({"error": "collection is required"}, safe=False)



    if request.GET.__contains__('pipeline') or request.POST.__contains__('pipeline'):
        pipeline = request.GET['pipeline'] if request.GET.__contains__('pipeline') else request.POST['pipeline']
        if pipeline == "":
            return JsonResponse({"error": "pipeline is required"}, safe=False)

    try:

        data = QueryMongoDB.getAggegation(colletion, pipeline)
        return JsonResponse(data, safe=False)
    except Exception as err:
        print(err)
        return JsonResponse({"error": "syntaxis error"}, safe=False)

