from django.shortcuts import render
from django.http import HttpResponse, JsonResponse
from rest_framework import viewsets
from rest_framework.decorators import api_view, renderer_classes

from rest_framework.response import Response
from rest_framework.renderers import JSONRenderer
# Create your views here.
from frastro.frastro.core.data.archive.lasair_archive_cp import LasairArchive
import numpy as np
from datamanager.models import Candidate


from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from rest_framework.parsers import JSONParser
from frastro.frastro.core.database.mongodb.mongodb_manager import MongodbManager
from frastro.frastro.core.utils.config import Config


@api_view(['GET','POST'])
@renderer_classes((JSONRenderer,))
def generateCatalog(request):
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