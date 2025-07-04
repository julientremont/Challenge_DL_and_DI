"""
Analysis views for Gold layer data warehouse.
Provides comprehensive analytics endpoints across all data sources.
"""

from django.db.models import Count, Sum, Avg, Max, Min, Q, F
from rest_framework import generics, status
from rest_framework.decorators import api_view
from rest_framework.response import Response
from django_filters.rest_framework import DjangoFilterBackend
from django_filters import rest_framework as filters
from drf_spectacular.utils import extend_schema, OpenApiParameter
from drf_spectacular.openapi import OpenApiTypes

from .models import (
    DimCountry, DimTechnology, DimCompany, DimJobRole, DimDataSource,
    AnalysisTechActivity, AnalysisJobDetails
)
from .serializers import (
    AnalysisTechActivityListSerializer, AnalysisTechActivityDetailSerializer,
    AnalysisJobDetailsListSerializer, AnalysisJobDetailsDetailSerializer
)


class AnalysisTechActivityFilter(filters.FilterSet):
    """Filter for technology activity analysis"""
    date_from = filters.DateFilter(field_name='date_key', lookup_expr='gte')
    date_to = filters.DateFilter(field_name='date_key', lookup_expr='lte')
    year = filters.NumberFilter(field_name='date_key__year')
    month = filters.NumberFilter(field_name='date_key__month')
    country = filters.CharFilter(field_name='id_country__country_code', lookup_expr='iexact')
    technology = filters.CharFilter(field_name='id_technology__technology_name', lookup_expr='icontains')
    technology_type = filters.CharFilter(field_name='id_technology__technology_type', lookup_expr='iexact')
    source = filters.CharFilter(field_name='id_source__source_name', lookup_expr='icontains')
    min_popularity = filters.NumberFilter(field_name='popularity_score', lookup_expr='gte')
    min_github_stars = filters.NumberFilter(field_name='github_stars', lookup_expr='gte')
    min_search_volume = filters.NumberFilter(field_name='search_volume', lookup_expr='gte')

    class Meta:
        model = AnalysisTechActivity
        fields = []


class AnalysisJobDetailsFilter(filters.FilterSet):
    """Filter for job details analysis"""
    date_from = filters.DateFilter(field_name='date_key', lookup_expr='gte')
    date_to = filters.DateFilter(field_name='date_key', lookup_expr='lte')
    year = filters.NumberFilter(field_name='date_key__year')
    month = filters.NumberFilter(field_name='date_key__month')
    country = filters.CharFilter(field_name='id_country__country_code', lookup_expr='iexact')
    company = filters.CharFilter(field_name='id_company__company_name', lookup_expr='icontains')
    job_title = filters.CharFilter(field_name='job_title', lookup_expr='icontains')
    job_type = filters.CharFilter(field_name='job_type', lookup_expr='iexact')
    seniority = filters.CharFilter(field_name='seniority', lookup_expr='iexact')
    role_type = filters.CharFilter(field_name='id_job_role__role_type', lookup_expr='iexact')
    min_salary = filters.NumberFilter(field_name='salary_usd', lookup_expr='gte')
    max_salary = filters.NumberFilter(field_name='salary_usd', lookup_expr='lte')

    class Meta:
        model = AnalysisJobDetails
        fields = []


@extend_schema(tags=['Analysis'])
class AnalysisTechActivityListView(generics.ListAPIView):
    """List all technology activities with filtering and pagination"""
    queryset = AnalysisTechActivity.objects.using('gold').select_related(
        'id_country', 'id_technology', 'id_source'
    ).all()
    serializer_class = AnalysisTechActivityListSerializer
    filter_backends = [DjangoFilterBackend]
    filterset_class = AnalysisTechActivityFilter
    ordering_fields = ['date_key', 'popularity_score', 'github_stars', 'search_volume']
    ordering = ['-date_key', '-popularity_score']


@extend_schema(tags=['Analysis'])
class AnalysisTechActivityDetailView(generics.RetrieveAPIView):
    """Retrieve a specific technology activity"""
    queryset = AnalysisTechActivity.objects.using('gold').select_related(
        'id_country', 'id_technology', 'id_source'
    ).all()
    serializer_class = AnalysisTechActivityDetailSerializer


@extend_schema(
    tags=['Analysis'],
    summary='Technology Statistics',
    description='Get comprehensive statistics for all technologies'
)
@api_view(['GET'])
def analysis_technology_stats(request):
    """Get technology statistics across all sources"""
    try:
        stats = AnalysisTechActivity.objects.using('gold').values(
            'id_technology__technology_name',
            'id_technology__technology_type'
        ).annotate(
            technology_name=F('id_technology__technology_name'),
            technology_type=F('id_technology__technology_type'),
            total_jobs=Sum('job_count'),
            total_github_stars=Sum('github_stars'),
            total_search_volume=Sum('search_volume'),
            avg_popularity_score=Avg('popularity_score'),
            countries_count=Count('id_country', distinct=True)
        ).filter(
            total_jobs__gt=0
        ).order_by('-total_jobs')[:20]

        return Response({
            'technology_stats': [
                {
                    'technology_name': item['technology_name'],
                    'technology_type': item['technology_type'],
                    'total_jobs': item['total_jobs'],
                    'total_github_stars': item['total_github_stars'] or 0,
                    'total_search_volume': item['total_search_volume'] or 0,
                    'avg_popularity_score': round(item['avg_popularity_score'] or 0, 2),
                    'countries_count': item['countries_count']
                }
                for item in stats
            ]
        })
    except Exception as e:
        return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@extend_schema(
    tags=['Analysis'],
    summary='Technology Trends Over Time',
    description='Analyze technology trends with time series data',
    parameters=[
        OpenApiParameter('technology', OpenApiTypes.STR, description='Technology name filter'),
        OpenApiParameter('limit', OpenApiTypes.INT, description='Limit results (default: 10)')
    ]
)
@api_view(['GET'])
def analysis_tech_trends(request):
    """Get technology trends over time"""
    try:
        technology = request.GET.get('technology')
        limit = int(request.GET.get('limit', 10))

        queryset = AnalysisTechActivity.objects.using('gold').values(
            'date_key',
            'id_technology__technology_name'
        ).annotate(
            technology_name=F('id_technology__technology_name'),
            job_count=Sum('job_count'),
            github_stars=Sum('github_stars'),
            search_volume=Sum('search_volume')
        )

        if technology:
            queryset = queryset.filter(id_technology__technology_name__icontains=technology)

        trends = queryset.order_by('-date_key')[:limit * 50]
        
        return Response({
            'tech_trends': [
                {
                    'date_key': item['date_key'],
                    'technology_name': item['technology_name'],
                    'job_count': item['job_count'],
                    'github_stars': item['github_stars'] or 0,
                    'search_volume': item['search_volume'] or 0
                }
                for item in trends
            ]
        })
    except Exception as e:
        return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@extend_schema(
    tags=['Analysis'],
    summary='Technology Popularity Ranking',
    description='Get technology popularity ranking based on combined metrics'
)
@api_view(['GET'])
def analysis_popularity_ranking(request):
    """Get technology popularity ranking"""
    try:
        rankings = AnalysisTechActivity.objects.using('gold').values(
            'id_technology__technology_name',
            'id_technology__technology_type'
        ).annotate(
            technology_name=F('id_technology__technology_name'),
            technology_type=F('id_technology__technology_type'),
            total_github=Sum('github_stars'),
            total_trends=Sum('search_volume'),
            total_jobs=Sum('job_count')
        ).filter(
            total_jobs__gt=0
        ).order_by('-total_jobs')[:20]

        # Add rankings
        response_data = []
        for i, tech in enumerate(rankings):
            response_data.append({
                'popularity_rank': i + 1,
                'technology_name': tech['technology_name'],
                'technology_type': tech['technology_type'],
                'total_github': tech['total_github'] or 0,
                'total_trends': tech['total_trends'] or 0,
                'total_jobs': tech['total_jobs'] or 0
            })

        return Response({'popularity_ranking': response_data})
    except Exception as e:
        return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@extend_schema(tags=['Analysis'])
class AnalysisJobDetailsListView(generics.ListAPIView):
    """List all job details with filtering and pagination"""
    queryset = AnalysisJobDetails.objects.using('gold').select_related(
        'id_country', 'id_company', 'id_job_role', 'id_source'
    ).all()
    serializer_class = AnalysisJobDetailsListSerializer
    filter_backends = [DjangoFilterBackend]
    filterset_class = AnalysisJobDetailsFilter
    ordering_fields = ['date_key', 'salary_usd', 'data_quality_score']
    ordering = ['-date_key', '-salary_usd']


@extend_schema(tags=['Analysis'])
class AnalysisJobDetailsDetailView(generics.RetrieveAPIView):
    """Retrieve a specific job detail"""
    queryset = AnalysisJobDetails.objects.using('gold').select_related(
        'id_country', 'id_company', 'id_job_role', 'id_source'
    ).all()
    serializer_class = AnalysisJobDetailsDetailSerializer


@extend_schema(
    tags=['Analysis'],
    summary='Job Market Summary',
    description='Get comprehensive job market summary statistics'
)
@api_view(['GET'])
def analysis_market_summary(request):
    """Get job market summary"""
    try:
        total_jobs = AnalysisJobDetails.objects.using('gold').count()
        total_companies = AnalysisJobDetails.objects.using('gold').values('id_company').distinct().count()
        total_countries = AnalysisJobDetails.objects.using('gold').values('id_country').distinct().count()
        total_technologies = AnalysisTechActivity.objects.using('gold').values('id_technology').distinct().count()
        
        avg_salary = AnalysisJobDetails.objects.using('gold').filter(
            salary_usd__isnull=False
        ).aggregate(avg_salary=Avg('salary_usd'))['avg_salary'] or 0

        job_types = dict(AnalysisJobDetails.objects.using('gold').values('job_type').annotate(
            count=Count('job_type')
        ).values_list('job_type', 'count'))

        seniority_levels = dict(AnalysisJobDetails.objects.using('gold').values('seniority').annotate(
            count=Count('seniority')
        ).values_list('seniority', 'count'))

        data_sources = AnalysisJobDetails.objects.using('gold').values('id_source').distinct().count()

        summary = {
            'total_jobs': total_jobs,
            'total_companies': total_companies,
            'total_countries': total_countries,
            'total_technologies': total_technologies,
            'avg_salary_usd': round(avg_salary, 2),
            'top_job_types': job_types,
            'top_seniority_levels': seniority_levels,
            'data_sources_count': data_sources
        }

        return Response(summary)
    except Exception as e:
        return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@extend_schema(
    tags=['Analysis'],
    summary='Country Statistics',
    description='Get statistics by country'
)
@api_view(['GET'])
def analysis_country_stats(request):
    """Get country statistics"""
    try:
        countries_with_jobs = AnalysisJobDetails.objects.using('gold').values(
            'id_country__country_name',
            'id_country__country_code'
        ).annotate(
            country_name=F('id_country__country_name'),
            country_code=F('id_country__country_code'),
            total_jobs=Count('id_job'),
            total_companies=Count('id_company', distinct=True),
            avg_salary_usd=Avg('salary_usd')
        ).filter(
            id_country__isnull=False,
            total_jobs__gt=0
        ).order_by('-total_jobs')[:10]

        results = []
        for country in countries_with_jobs:
            # Get top technologies for this country
            top_techs = AnalysisTechActivity.objects.using('gold').filter(
                id_country__country_code=country['country_code']
            ).values_list(
                'id_technology__technology_name', flat=True
            ).distinct()[:5]

            results.append({
                'country_name': country['country_name'],
                'country_code': country['country_code'],
                'total_jobs': country['total_jobs'],
                'total_companies': country['total_companies'],
                'avg_salary_usd': round(float(country['avg_salary_usd']) if country['avg_salary_usd'] else 0, 2),
                'top_technologies': list(top_techs)
            })

        return Response({'country_stats': results})
    except Exception as e:
        return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@extend_schema(
    tags=['Analysis'],
    summary='Company Statistics',
    description='Get statistics by company',
    parameters=[
        OpenApiParameter('min_jobs', OpenApiTypes.INT, description='Minimum number of jobs (default: 2)')
    ]
)
@api_view(['GET'])
def analysis_company_stats(request):
    """Get company statistics"""
    try:
        min_jobs = int(request.GET.get('min_jobs', 2))
        
        companies_with_jobs = AnalysisJobDetails.objects.using('gold').values(
            'id_company__company_name'
        ).annotate(
            company_name=F('id_company__company_name'),
            total_jobs=Count('id_job'),
            avg_salary_usd=Avg('salary_usd')
        ).filter(
            id_company__isnull=False,
            total_jobs__gte=min_jobs
        ).order_by('-total_jobs')[:20]

        results = []
        for company in companies_with_jobs:
            countries = AnalysisJobDetails.objects.using('gold').filter(
                id_company__company_name=company['company_name']
            ).values_list(
                'id_country__country_name', flat=True
            ).distinct()

            results.append({
                'company_name': company['company_name'],
                'total_jobs': company['total_jobs'],
                'avg_salary_usd': round(float(company['avg_salary_usd']) if company['avg_salary_usd'] else 0, 2),
                'countries_present': list(countries)
            })

        return Response({'company_stats': results})
    except Exception as e:
        return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)