from django.shortcuts import render
from django.template import loader
from django.http import HttpResponse
# Create your views here.
from frastro.frastro.core.database.mongodb.mongodb_manager import MongodbManager
from frastro.frastro.core.utils.config import Config
from datamanager.models import Candidate
from django.views.decorators.csrf import csrf_exempt
import json
from astropy import units as u
from astropy.coordinates import SkyCoord
from django.urls import reverse
from django.contrib.auth.decorators import login_required



def index(request):
    return render(request,'gui/index.html',{"test":1})



def results(request):
    return render(request, 'gui/dataanalysis.html', {"test": 1})

def massivegalaxies(request):
    days_ago = 5
    if request.GET.__contains__('days'):
        days_ago = request.GET["days"]
    if request.POST.__contains__('days'):
        days_ago = request.POST["days"]

    url_request = reverse("getMassiveGalaxiesData")

    context = {
        'form': True,
        'days': int(days_ago),
        'url_request': url_request,
        "title": "Last Candidates"
    }
    template = loader.get_template('gui/dataanalysis.html')

    return HttpResponse(template.render(context, request))

def demo(request):
    return render(request, 'gui/demo.html')

def lastcandidates(request):

    days_ago = 5
    if request.GET.__contains__('days'):
        days_ago = request.GET["days"]
    if request.POST.__contains__('days'):
        days_ago = request.POST["days"]

    url_request=reverse("getLastCandidatesData")

    context = {
        'form':True,
        'days': int(days_ago),
        'url_request':url_request,
        "title":"Last Candidates"
    }
    template = loader.get_template('gui/dataanalysis.html')

    return HttpResponse(template.render(context, request))

def lastdetections(request):
    days_ago = 5
    if request.GET.__contains__('days'):
        days_ago = request.GET["days"]
    if request.POST.__contains__('days'):
        days_ago = request.POST["days"]


    url_request = reverse("getLastDetectionsData")

    context = {
        'form':True,
        'days': int(days_ago),
        'url_request': url_request,
        "title": "New Detections"
    }
    template = loader.get_template('gui/dataanalysis.html')

    return HttpResponse(template.render(context, request))

def sqlquery(request):
    query = 5
    if request.GET.__contains__('query'):
        query = request.GET["query"]
    if request.POST.__contains__('query'):
        query = request.POST["query"]

    url_request = reverse("sqlqueryData")

    context = {
        'query':query,
        'url_request': url_request

    }
    template = loader.get_template('gui/sqlquery.html')


    return HttpResponse(template.render(context, request))


def crossmatch(request):
    return render(request, 'gui/crossmatch.html')



def bestcandidates(request):

    days_ago = 5
    if request.GET.__contains__('days'):
        days_ago = request.GET["days"]
    if request.POST.__contains__('days'):
        days_ago = request.POST["days"]

    filter=""
    if request.GET.__contains__('filter'):
        filter = request.GET["filter"]
    if request.POST.__contains__('filter'):
        filter = request.POST["filter"]

    url_request = reverse("getBestCandidatesData")

    context = {
        'filter': filter,
        'form':False,
        'best':True,
        'days': days_ago,
        'url_request': url_request,
        "title": "Best Detections"
    }
    template = loader.get_template('gui/dataanalysis.html')

    return HttpResponse(template.render(context, request))



def allcandidates(request):

    days_ago = 5
    if request.GET.__contains__('days'):
        days_ago = request.GET["days"]
    if request.POST.__contains__('days'):
        days_ago = request.POST["days"]

    url_request = reverse("getAllCandidatesData")

    context = {
        'filter': False,
        'days':0,
        'url_request': url_request,
        "title": "ALL Detections"
    }
    template = loader.get_template('gui/dataanalysis.html')

    return HttpResponse(template.render(context, request))


def allObjects(request):

    days_ago = 5
    if request.GET.__contains__('days'):
        days_ago = request.GET["days"]
    if request.POST.__contains__('days'):
        days_ago = request.POST["days"]

    url_request = reverse("sqllastdetectionsData")

    context = {
        'form': True,
        'days': int(days_ago),
        'filter': True,
        'url_request': url_request,
        "title": "ALL Detections"
    }
    template = loader.get_template('gui/dataview.html')

    return HttpResponse(template.render(context, request))




def currentSN(request):
    return render(request, 'gui/currentZTFSN.html')


def query(request):
    return render(request, 'gui/querypage.html')

@csrf_exempt
def getObject(request,id):
    source = Candidate.getByID(id,collection="lastdetections7")

    template = loader.get_template('gui/detail.html')


    context = {
        'source': json.dumps(source[0])
    }
    return HttpResponse(template.render(context, request))
