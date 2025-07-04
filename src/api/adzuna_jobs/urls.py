from django.urls import path
from . import views

urlpatterns = [
    # Basic CRUD operations for silver layer data
    path('', views.AdzunaJobSilverListView.as_view(), name='adzuna-jobs-list'),
    path('<int:pk>/', views.AdzunaJobSilverDetailView.as_view(), name='adzuna-jobs-detail'),
    
    # Analytics endpoints based on actual data
    path('summary/', views.job_market_summary, name='adzuna-job-market-summary'),
    path('by-country/', views.job_market_by_country, name='adzuna-job-market-by-country'),
    path('job-analysis/', views.job_title_analysis, name='adzuna-job-title-analysis'),
    path('salary-analysis/', views.salary_analysis, name='adzuna-salary-analysis'),
    path('time-series/', views.job_market_time_series, name='adzuna-job-market-time-series'),
]