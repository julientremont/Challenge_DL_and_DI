from django.urls import path
from . import views

urlpatterns = [
    # Basic CRUD operations
    path('', views.GlassdoorJobListView.as_view(), name='glassdoor-jobs-list'),
    path('<int:pk>/', views.GlassdoorJobDetailView.as_view(), name='glassdoor-jobs-detail'),
    
    # Analytics endpoints
    path('summary/', views.jobs_summary, name='glassdoor-jobs-summary'),
    path('company-stats/', views.company_stats, name='glassdoor-company-stats'),
    path('industry-analysis/', views.industry_analysis, name='glassdoor-industry-analysis'),
    path('location-analysis/', views.location_analysis, name='glassdoor-location-analysis'),
    path('salary-rating-correlation/', views.salary_rating_correlation, name='glassdoor-salary-rating'),
    path('company-insights/', views.company_insights, name='glassdoor-company-insights'),
]