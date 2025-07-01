from django.urls import path
from . import views

urlpatterns = [
    # Basic CRUD operations
    path('', views.AdzunaJobListView.as_view(), name='adzuna-jobs-list'),
    path('<int:pk>/', views.AdzunaJobDetailView.as_view(), name='adzuna-jobs-detail'),
    
    # Analytics endpoints
    path('summary/', views.jobs_summary, name='adzuna-jobs-summary'),
    path('by-location/', views.jobs_by_location, name='adzuna-jobs-by-location'),
    path('by-company/', views.jobs_by_company, name='adzuna-jobs-by-company'),
    path('salary-trends/', views.salary_trends, name='adzuna-salary-trends'),
    path('skills-analysis/', views.skills_analysis, name='adzuna-skills-analysis'),
]