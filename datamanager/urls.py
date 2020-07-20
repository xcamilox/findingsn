from django.urls import path

from . import views

urlpatterns = [
    path('getlastlasair/', views.getLastLasair, name='getLastLasairData'),
    path('getlastCandidates/', views.lastcandidates, name='getLastCandidatesData'),
    path('getlastDetections/', views.lastDetections, name='getLastDetectionsData'),
    path('getAllCandidates/', views.allcandidates, name='getAllCandidatesData'),
    path('getBestCandidates/', views.bestcandidates, name='getBestCandidatesData'),
    path('getMassiveGalaxies/', views.massiveGalaxies, name='getMassiveGalaxiesData'),
    path('currentTNSSN/', views.currentSN, name='currentTNSSNData'),
    path('sqlquery/', views.SQLQuery, name='sqlqueryData'),
    path('getbyid/<str:ztfid>', views.getByID, name='getByIDData'),
    path('getlightcurve/', views.getLightCurve, name='getLightcurveData'),
    path('crossmatch/<str:ra>/<str:dec>/<str:radio>', views.crossMatch, name='crossMatchData'),
    path('runquery/', views.runQuery, name='runQueryData'),
    path('runaggregation/', views.runAggregation, name='runaggregationData'),
    path('sqllastdetections/', views.sqllastdetections, name='sqllastdetectionsData'),


]