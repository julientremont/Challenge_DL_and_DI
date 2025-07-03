from django.contrib import admin
from django.urls import path, include
from django.http import JsonResponse
from drf_spectacular.views import SpectacularAPIView, SpectacularSwaggerView, SpectacularRedocView
from drf_spectacular.utils import extend_schema
from rest_framework.decorators import api_view
from .auth_views import api_login, api_logout, current_user, jwt_login, jwt_refresh


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
            'auth': {
                'session_login': '/api/auth/login/',
                'jwt_login': '/api/auth/jwt/login/',
                'jwt_refresh': '/api/auth/jwt/refresh/',
                'logout': '/api/auth/logout/',
                'current_user': '/api/auth/user/'
            },
            'trends': '/api/trends/',
            'stackoverflow_survey': '/api/stackoverflow-survey/',
            'adzuna_jobs': '/api/adzuna-jobs/',
            'github_repos': '/api/github-repos/',
            'eurotechjobs': '/api/eurotechjobs/',
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
            'github_repos': {
                'base_url': '/api/github-repos/',
                'endpoints': ['/', '/summary/', '/technology-stats/', '/activity-analysis/', '/time-series/', '/popular/']
            },
            'eurotechjobs': {
                'base_url': '/api/eurotechjobs/',
                'endpoints': ['/', '/summary/', '/country-stats/', '/technology-stats/', '/job-type-analysis/', '/time-series/', '/company-analysis/']
            }
        }
    })


urlpatterns = [
    path('admin/', admin.site.urls),
    path('api/', api_root, name='api-root'),
    
    # Authentication endpoints
    path('api/auth/login/', api_login, name='api-login'),
    path('api/auth/jwt/login/', jwt_login, name='jwt-login'),
    path('api/auth/jwt/refresh/', jwt_refresh, name='jwt-refresh'),
    path('api/auth/logout/', api_logout, name='api-logout'),
    path('api/auth/user/', current_user, name='current-user'),
    
    # Data source APIs
    path('api/trends/', include('src.api.trends.urls')),
    path('api/stackoverflow-survey/', include('src.api.stackoverflow_survey.urls')),
    path('api/adzuna-jobs/', include('src.api.adzuna_jobs.urls')),
    path('api/github-repos/', include('src.api.github_jobs.urls')),
    path('api/eurotechjobs/', include('src.api.eurotechjobs.urls')),
    
    # Swagger/OpenAPI URLs
    path('api/schema/', SpectacularAPIView.as_view(), name='schema'),
    path('api/docs/', SpectacularSwaggerView.as_view(url_name='schema'), name='swagger-ui'),
    path('api/redoc/', SpectacularRedocView.as_view(url_name='schema'), name='redoc'),
]