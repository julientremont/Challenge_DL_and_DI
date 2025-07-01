from django.urls import path
from . import views

urlpatterns = [
    # Basic CRUD operations
    path('', views.TrendsFRListView.as_view(), name='trends-list'),
    path('<int:pk>/', views.TrendsFRDetailView.as_view(), name='trends-detail'),
    
    # Analytics endpoints
    path('summary/', views.trends_summary, name='trends-summary'),
    path('aggregated/', views.trends_aggregated, name='trends-aggregated'),
    path('time-series/', views.trends_time_series, name='trends-time-series'),
    path('top-keywords/', views.trends_top_keywords, name='trends-top-keywords'),
]