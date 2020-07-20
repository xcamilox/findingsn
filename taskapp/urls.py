from django.urls import path

from . import views

urlpatterns = [
    path('lastdetections', views.getLastDetections, name='lastdetectionstask'),
    path('followupcandidates', views.getFollowUpCandidates, name='followupCandidatestask'),
    path('crosshmatch/<str:collection>', views.getCrossMatch, name='crossMatchtask'),
    path('abmagSN', views.setABSN, name='abmagSNtask'),
    path('getRedshift', views.getRedshift, name='getRedshiftTask'),
    path('setAbMags', views.setABmagCandidates, name='setAbmagsTask'),
    path('getPeak/<str:collection>', views.getPeaksTask, name='getPeakTask'),
    path('classifier', views.classifierCandidates, name='classifierTask'),
    path('pipeline/<str:collection>', views.runPipeline, name='pipelineTask'),


]