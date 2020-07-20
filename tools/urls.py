from django.urls import path

from . import views

urlpatterns = [
    path('generatecatalogs', views.generateCatalog, name='index')
]