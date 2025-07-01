from django.contrib import admin
from django.urls import path, include
from django.http import JsonResponse


def api_root(request):
    """API root endpoint with available endpoints"""
    return JsonResponse({
        'message': 'Challenge DL and DI API',
        'version': '1.0',
        'endpoints': {
            'admin': '/admin/',
            'trends': '/api/trends/',
            'trends_summary': '/api/trends/summary/',
            'trends_aggregated': '/api/trends/aggregated/',
            'trends_time_series': '/api/trends/time-series/',
            'trends_top_keywords': '/api/trends/top-keywords/',
        },
        'documentation': {
            'trends_list': 'GET /api/trends/ - List trends with filtering',
            'trends_detail': 'GET /api/trends/{id}/ - Get specific trend',
            'trends_summary': 'GET /api/trends/summary/?keyword=python&country_code=FR',
            'trends_aggregated': 'GET /api/trends/aggregated/?date_from=2024-01-01&date_to=2024-12-31',
            'trends_time_series': 'GET /api/trends/time-series/?keyword=python&group_by=month',
            'trends_top_keywords': 'GET /api/trends/top-keywords/?limit=10',
        }
    })


urlpatterns = [
    path('admin/', admin.site.urls),
    path('api/', api_root, name='api-root'),
    path('api/trends/', include('src.api.trends.urls')),
]