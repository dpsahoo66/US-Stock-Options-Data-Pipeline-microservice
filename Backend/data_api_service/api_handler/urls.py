from django.urls import path
from .views import CombinedStockDataView

urlpatterns = [
    path('api/stock-data/', CombinedStockDataView.as_view(), name='combined-stock-data'),
]