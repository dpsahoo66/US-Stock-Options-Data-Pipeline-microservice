# data-collector/collector/urls.py
from django.urls import path
from collector import views

urlpatterns = [
    path('fetch_each_day_data/', views.fetch_each_day_data, name='fetch_each_day_data'),
    path('fetch_last_15min_data/', views.fetch_last_15min_data, name='fetch_last_15min_data'),
    path('fetch_option_data/', views.fetch_option_data, name='fetch_option_data'),
]