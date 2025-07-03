from django.urls import path
from . import views

urlpatterns = [
    # Basic CRUD operations
    path('', views.EuroTechJobsListView.as_view(), name='eurotechjobs-list'),
    path('<int:pk>/', views.EuroTechJobsDetailView.as_view(), name='eurotechjobs-detail'),
    
    # Analytics endpoints
    path('summary/', views.eurotechjobs_summary, name='eurotechjobs-summary'),
    path('country-stats/', views.eurotechjobs_country_stats, name='eurotechjobs-country-stats'),
    path('technology-stats/', views.eurotechjobs_technology_stats, name='eurotechjobs-technology-stats'),
    path('job-type-analysis/', views.eurotechjobs_job_type_analysis, name='eurotechjobs-job-type-analysis'),
    path('time-series/', views.eurotechjobs_time_series, name='eurotechjobs-time-series'),
    path('company-analysis/', views.eurotechjobs_company_analysis, name='eurotechjobs-company-analysis'),
]