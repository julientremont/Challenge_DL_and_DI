from django.urls import path
from . import views

urlpatterns = [
    # Basic CRUD operations
    path('', views.StackOverflowSurveyListView.as_view(), name='stackoverflow-survey-list'),
    path('<int:pk>/', views.StackOverflowSurveyDetailView.as_view(), name='stackoverflow-survey-detail'),
    
    # Analytics endpoints
    path('summary/', views.survey_summary, name='stackoverflow-survey-summary'),
    path('developer-types/', views.developer_type_stats, name='stackoverflow-developer-types'),
    path('technologies/', views.technology_stats, name='stackoverflow-technologies'),
    path('salary-analysis/', views.salary_analysis, name='stackoverflow-salary-analysis'),
]