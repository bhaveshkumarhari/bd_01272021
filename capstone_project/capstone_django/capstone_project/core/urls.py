from django.urls import path
from . import views

app_name = 'core'

urlpatterns = [
    path('', views.core_home, name='home'),
    path('usa/', views.core_usa, name='usa'),
    path('usamap/', views.core_usamap, name='usamap'),
    path('worldmap/', views.core_worldmap, name='worldmap'),
    path('api/chart/data/', views.ChartData.as_view()),
]
