from django.urls import path
from . import views

urlpatterns = [
    # Basic CRUD operations
    path('', views.StackOverflowSurveyListView.as_view(), name='stackoverflow-survey-list'),
    path('<int:pk>/', views.StackOverflowSurveyDetailView.as_view(), name='stackoverflow-survey-detail'),
    
    # Analytics endpoints
    path('summary/', views.stackoverflow_survey_summary, name='stackoverflow-survey-summary'),
    path('developer-roles/', views.developer_role_stats, name='stackoverflow-developer-roles'),
    path('technologies/', views.technology_stats, name='stackoverflow-technologies'),
    path('salary-analysis/', views.salary_analysis, name='stackoverflow-salary-analysis'),
    path('countries/', views.country_stats, name='stackoverflow-countries'),
]