from django.urls import path

from . import views

urlpatterns = [
    path('', views.lastcandidates, name='indexui'),
    path('search/', views.results, name='searchui'),
    path('query/', views.query, name='queryui'),
    path('sqlquery/', views.sqlquery, name='sqlqueryui'),
    path('lastcandidates/', views.lastcandidates, name='lastcandidatesui'),
    path('lastdetections/', views.lastdetections, name='lastdetectionsui'),
    path('bestcandidates/', views.bestcandidates, name='bestcandidatesui'),
    path('demo/', views.demo, name='demoui'),
    path('allcandidates/', views.allcandidates, name='allcandidatesui'),
    path('massivegalaxies/', views.massivegalaxies, name='massivegalaxiesui'),
    path('crossmatch/', views.crossmatch, name='crosmatchui'),
    path('currentsn/', views.currentSN, name='currentsnui'),
    path('allobjects/', views.allObjects, name='allobjectsui'),
    path('object/<str:id>', views.getObject, name='getobjectui')
]