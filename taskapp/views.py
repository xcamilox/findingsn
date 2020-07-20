from django.http import HttpResponse, JsonResponse
from rest_framework.decorators import api_view, renderer_classes
from rest_framework.renderers import JSONRenderer
from taskapp.tasks import checkLastDetections,followUpCandidates,crossMatchCollection,calcABMagnitud,calcRedshiftCandidates,calcABMagCandidates,getPeaks, classifyCandidate,runPipelines,createPipeline
from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from datetime import datetime

@api_view(['GET', 'POST'])
@renderer_classes((JSONRenderer,))
def getLastDetections(request):
    id = checkLastDetections()#.send()
    #id = checkLastDetections.send()
    jobs_list = {"jobId":id}
    return JsonResponse(jobs_list, safe=False)



@api_view(['GET', 'POST'])
@renderer_classes((JSONRenderer,))
def getFollowUpCandidates(request):
    id = followUpCandidates.send()
    jobs_list = {"jobId":id}
    return JsonResponse(jobs_list, safe=False)


@api_view(['GET', 'POST'])
@renderer_classes((JSONRenderer,))
def setABSN(request):

    id=calcABMagnitud.send()
    #id = calcABMagnitud()
    jobs_list = {"jobId": id}
    return JsonResponse(jobs_list, safe=False)


@api_view(['GET', 'POST'])
@renderer_classes((JSONRenderer,))
def getRedshift(request):
    #id = calcRedshiftCandidates.send()

    id = calcRedshiftCandidates()
    jobs_list = {"jobId": id}
    return JsonResponse(jobs_list, safe=False)


@api_view(['GET', 'POST'])
@renderer_classes((JSONRenderer,))
def setABmagCandidates(request):
    #id = getRedshifts.send()
    id = calcABMagCandidates()
    jobs_list = {"jobId": id}
    return JsonResponse(jobs_list, safe=False)

@api_view(['GET', 'POST'])
@renderer_classes((JSONRenderer,))
def getCrossMatch(request,collection):
    #filter={"id":"ZTF20aammnnm"}

    #id=crossMatchCollection(collection)
    id=crossMatchCollection(collection=collection)
    jobs_list = {"jobId": id}
    return JsonResponse(jobs_list, safe=False)


@api_view(['GET', 'POST'])
@renderer_classes((JSONRenderer,))
def getPeaksTask(request,collection):
    #filter={"id":"ZTF20aammnnm"}

    #id=crossMatchCollection(collection,filter=filter)
    #id=getPeaks.send(collection,filter={"Redshift":{"$gt":0},"lightcurve":{"$exists":True},"peak":{"$exists":False}})
    id = getPeaks(collection,
                       filter={ "lightcurve": {"$exists": True}})
    jobs_list = {"jobId": id}
    return JsonResponse(jobs_list, safe=False)


@api_view(['GET', 'POST'])
@renderer_classes((JSONRenderer,))
def runPipeline(request,collection):

    task=[{"order":0,"action":"checkLastDetections","params":{"collection":collection},"state":"","skip":False},
          {"order":1,"action":"crossMatchCollection","params":{"collection":collection},"state":"","skip":False},
          {"order":2,"action":"calcRedshiftCandidates","params":{"collection":collection},"state":"","skip":False},
          {"order":3,"action":"getPeaks","params":{"collection":collection},"state":"","skip":False}]
    tasks = {"tasks":task,"state":"created","date":datetime.now().timestamp(),"activities":[]}
    id = createPipeline(tasks=tasks)
    idjob = runPipelines.send(str(id))

    jobs_list = {"jobId": idjob}
    return JsonResponse(jobs_list, safe=False)




@api_view(['GET', 'POST'])
@renderer_classes((JSONRenderer,))
def classifierCandidates(request):
    #filter={"id":"ZTF20aammnnm"}

    #id=crossMatchCollection(collection,filter=filter)
    id=classifyCandidate()
    jobs_list = {"jobId": id}
    return JsonResponse(jobs_list, safe=False)
