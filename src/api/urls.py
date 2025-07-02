from django.contrib import admin
from django.urls import path, include
from django.http import JsonResponse
from drf_spectacular.views import SpectacularAPIView, SpectacularSwaggerView, SpectacularRedocView
from drf_spectacular.utils import extend_schema
from rest_framework.decorators import api_view


@extend_schema(tags=['API Info'])
@api_view(['GET'])
def api_root(request):
    """API root endpoint with available endpoints"""
    return JsonResponse({
        'message': 'Challenge DL and DI API',
        'version': '1.0',
        'endpoints': {
            'admin': '/admin/',
            'swagger': '/api/docs/',
            'redoc': '/api/redoc/',
            'trends': '/api/trends/',
            'stackoverflow_survey': '/api/stackoverflow-survey/',
            'adzuna_jobs': '/api/adzuna-jobs/',
            'glassdoor_jobs': '/api/glassdoor-jobs/',
            'github_repos': '/api/github-repos/',
        },
        'data_sources': {
            'google_trends': {
                'base_url': '/api/trends/',
                'endpoints': ['/', '/summary/', '/aggregated/', '/time-series/', '/top-keywords/']
            },
            'stackoverflow_survey': {
                'base_url': '/api/stackoverflow-survey/',
                'endpoints': ['/', '/summary/', '/developer-types/', '/technologies/', '/salary-analysis/']
            },
            'adzuna_jobs': {
                'base_url': '/api/adzuna-jobs/',
                'endpoints': ['/', '/summary/', '/by-location/', '/by-company/', '/salary-trends/', '/skills-analysis/']
            },
            'glassdoor_jobs': {
                'base_url': '/api/glassdoor-jobs/',
                'endpoints': ['/', '/summary/', '/company-stats/', '/industry-analysis/', '/location-analysis/', '/salary-rating-correlation/', '/company-insights/']
            },
            'github_repos': {
                'base_url': '/api/github-repos/',
                'endpoints': ['/', '/summary/', '/technology-stats/', '/activity-analysis/', '/time-series/', '/popular/']
            }
        }
    })


urlpatterns = [
    path('admin/', admin.site.urls),
    path('api/', api_root, name='api-root'),
    
    # Data source APIs
    path('api/trends/', include('src.api.trends.urls')),
    path('api/stackoverflow-survey/', include('src.api.stackoverflow_survey.urls')),
    path('api/adzuna-jobs/', include('src.api.adzuna_jobs.urls')),
    path('api/glassdoor-jobs/', include('src.api.glassdoor_jobs.urls')),
    path('api/github-repos/', include('src.api.github_jobs.urls')),
    
    # Swagger/OpenAPI URLs
    path('api/schema/', SpectacularAPIView.as_view(), name='schema'),
    path('api/docs/', SpectacularSwaggerView.as_view(url_name='schema'), name='swagger-ui'),
    path('api/redoc/', SpectacularRedocView.as_view(url_name='schema'), name='redoc'),
]