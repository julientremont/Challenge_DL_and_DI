from django.urls import path
from . import views

urlpatterns = [
    # Silver layer CRUD operations (main endpoints)
    path('', views.TrendsSilverListView.as_view(), name='trends-silver-list'),
    path('<int:pk>/', views.TrendsSilverDetailView.as_view(), name='trends-silver-detail'),
    
    # Enhanced analytics endpoints
    path('summary/', views.trends_summary, name='trends-summary'),
    path('keyword-analysis/', views.keyword_analysis, name='trends-keyword-analysis'),
    path('country-analysis/', views.country_trends_analysis, name='trends-country-analysis'),
    path('time-series/', views.trends_time_series, name='trends-time-series'),
    path('tech-analysis/', views.tech_trends_analysis, name='trends-tech-analysis'),
    
    # Legacy endpoints (for backward compatibility)
    path('legacy/', views.TrendsFRListView.as_view(), name='trends-legacy-list'),
    path('legacy/<int:pk>/', views.TrendsFRDetailView.as_view(), name='trends-legacy-detail'),
]