from django.urls import path
from . import views

urlpatterns = [
    # Basic CRUD operations
    path('', views.GitHubReposListView.as_view(), name='github-repos-list'),
    path('<int:pk>/', views.GitHubReposDetailView.as_view(), name='github-repos-detail'),
    
    # Analytics endpoints
    path('summary/', views.github_repos_summary, name='github-repos-summary'),
    path('technology-stats/', views.github_technology_stats, name='github-technology-stats'),
    path('activity-analysis/', views.github_activity_analysis, name='github-activity-analysis'),
    path('time-series/', views.github_time_series, name='github-time-series'),
    path('popular/', views.github_popular_repos, name='github-popular-repos'),
]