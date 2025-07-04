from django.urls import path
from . import views

urlpatterns = [
    # Basic CRUD operations for silver layer data
    path('', views.AdzunaJobSilverListView.as_view(), name='adzuna-jobs-list'),
    path('<int:pk>/', views.AdzunaJobSilverDetailView.as_view(), name='adzuna-jobs-detail'),
    
    # Analytics endpoints based on actual table structure
    path('summary/', views.adzuna_summary, name='adzuna-job-market-summary'),
    path('by-country/', views.adzuna_by_country, name='adzuna-job-market-by-country'),
    path('job-analysis/', views.adzuna_job_title_analysis, name='adzuna-job-title-analysis'),
    path('salary-analysis/', views.adzuna_salary_analysis, name='adzuna-salary-analysis'),
    path('time-series/', views.adzuna_time_series, name='adzuna-job-market-time-series'),
]