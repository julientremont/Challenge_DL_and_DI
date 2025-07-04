"""
URL configuration for Analysis API - Gold layer data warehouse endpoints.
"""

from django.urls import path
from . import views

urlpatterns = [
    # List and detail views
    path('tech-activity/', views.AnalysisTechActivityListView.as_view(), name='tech-activity-list'),
    path('tech-activity/<int:pk>/', views.AnalysisTechActivityDetailView.as_view(), name='tech-activity-detail'),
    path('job-market/', views.AnalysisJobDetailsListView.as_view(), name='job-market-list'),
    path('job-market/<int:pk>/', views.AnalysisJobDetailsDetailView.as_view(), name='job-market-detail'),
    
    # Analytics endpoints
    path('technology-stats/', views.analysis_technology_stats, name='technology-stats'),
    path('tech-trends/', views.analysis_tech_trends, name='tech-trends'),
    path('popularity-ranking/', views.analysis_popularity_ranking, name='popularity-ranking'),
    path('market-summary/', views.analysis_market_summary, name='market-summary'),
    path('country-stats/', views.analysis_country_stats, name='country-stats'),
    path('company-stats/', views.analysis_company_stats, name='company-stats'),
]