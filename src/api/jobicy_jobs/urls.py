from django.urls import path
from . import views

urlpatterns = [
    # Basic CRUD operations for silver layer data
    path('', views.JobicyJobSilverListView.as_view(), name='jobicy-jobs-list'),
    path('<int:pk>/', views.JobicyJobSilverDetailView.as_view(), name='jobicy-jobs-detail'),
    
    # Analytics endpoints based on actual table structure
    path('summary/', views.jobicy_summary, name='jobicy-job-market-summary'),
    path('by-country/', views.jobicy_by_country, name='jobicy-job-market-by-country'),
    path('job-analysis/', views.jobicy_job_analysis, name='jobicy-job-title-analysis'),
    path('company-analysis/', views.jobicy_company_analysis, name='jobicy-company-analysis'),
    path('salary-analysis/', views.jobicy_salary_analysis, name='jobicy-salary-analysis'),
    path('time-series/', views.jobicy_time_series, name='jobicy-job-market-time-series'),
]