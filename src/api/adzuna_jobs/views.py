from rest_framework import generics, status
from rest_framework.decorators import api_view, permission_classes
from rest_framework.response import Response
from django_filters.rest_framework import DjangoFilterBackend
from django_filters import rest_framework as filters
from django.db.models import Avg, Count, Sum, Max, Min, Q, F
from django.db.models.functions import TruncDate, TruncMonth
from drf_spectacular.utils import extend_schema, OpenApiParameter
from drf_spectacular.openapi import OpenApiTypes
from .models import AdzunaJobSilver
from .serializers import (
    AdzunaJobSilverSerializer,
    AdzunaJobSummarySerializer,
    JobMarketByCountrySerializer,
    JobTitleAnalysisSerializer,
    SalaryAnalysisSerializer,
    TimeSeriesJobDataSerializer
)
from ..permissions import AdminOnlyPermission, AuthenticatedReadOnlyPermission


class AdzunaJobSilverFilter(filters.FilterSet):
    """Custom filter for Adzuna Jobs Silver data"""
    job_title = filters.CharFilter(field_name='job_title', lookup_expr='icontains')
    country_code = filters.CharFilter(field_name='country_code', lookup_expr='exact')
    country_name = filters.CharFilter(field_name='country_name', lookup_expr='icontains')
    salary_range = filters.CharFilter(field_name='salary_range', lookup_expr='exact')
    date_from = filters.DateFilter(field_name='date', lookup_expr='gte')
    date_to = filters.DateFilter(field_name='date', lookup_expr='lte')
    job_count_min = filters.NumberFilter(field_name='job_count', lookup_expr='gte')
    job_count_max = filters.NumberFilter(field_name='job_count', lookup_expr='lte')
    average_salary_min = filters.NumberFilter(field_name='average_salary', lookup_expr='gte')
    average_salary_max = filters.NumberFilter(field_name='average_salary', lookup_expr='lte')
    quality_score_min = filters.NumberFilter(field_name='data_quality_score', lookup_expr='gte')
    
    class Meta:
        model = AdzunaJobSilver
        fields = ['country_code', 'salary_range', 'date_from', 'date_to']


@extend_schema(tags=['Adzuna Jobs'])
class AdzunaJobSilverListView(generics.ListAPIView):
    """List Adzuna job market data with filtering"""
    queryset = AdzunaJobSilver.objects.all()
    serializer_class = AdzunaJobSilverSerializer
    filter_backends = [DjangoFilterBackend]
    filterset_class = AdzunaJobSilverFilter
    ordering_fields = ['date', 'job_count', 'average_salary', 'data_quality_score']
    ordering = ['-date']
    permission_classes = [AuthenticatedReadOnlyPermission]


@extend_schema(tags=['Adzuna Jobs'])
class AdzunaJobSilverDetailView(generics.RetrieveAPIView):
    """Retrieve a specific job market record"""
    queryset = AdzunaJobSilver.objects.all()
    serializer_class = AdzunaJobSilverSerializer
    permission_classes = [AuthenticatedReadOnlyPermission]


@extend_schema(
    tags=['Adzuna Jobs'],
    parameters=[
        OpenApiParameter('country_code', OpenApiTypes.STR, description='Filter by country code'),
        OpenApiParameter('date_from', OpenApiTypes.DATE, description='Start date filter'),
        OpenApiParameter('date_to', OpenApiTypes.DATE, description='End date filter'),
    ]
)
@api_view(['GET'])
@permission_classes([AuthenticatedReadOnlyPermission])
def job_market_summary(request):
    """Get comprehensive job market summary statistics"""
    try:
        # Get query parameters
        country_code = request.GET.get('country_code')
        date_from = request.GET.get('date_from')
        date_to = request.GET.get('date_to')
        
        # Build queryset
        queryset = AdzunaJobSilver.objects.all()
        
        if country_code:
            queryset = queryset.filter(country_code=country_code)
        if date_from:
            queryset = queryset.filter(date__gte=date_from)
        if date_to:
            queryset = queryset.filter(date__lte=date_to)
        
        # Calculate summary statistics
        summary = queryset.aggregate(
            total_records=Count('id'),
            unique_job_titles=Count('job_title', distinct=True),
            countries_covered=Count('country_code', distinct=True),
            total_job_count=Sum('job_count'),
            avg_salary=Avg('average_salary'),
            salary_ranges_available=Count('salary_range', distinct=True),
            data_quality_avg=Avg('data_quality_score'),
            earliest_date=Min('date'),
            latest_date=Max('date')
        )
        
        # Format response
        summary_data = {
            'total_records': summary['total_records'] or 0,
            'unique_job_titles': summary['unique_job_titles'] or 0,
            'countries_covered': summary['countries_covered'] or 0,
            'total_job_count': summary['total_job_count'] or 0,
            'avg_salary': summary['avg_salary'],
            'salary_ranges_available': summary['salary_ranges_available'] or 0,
            'data_quality_avg': round(summary['data_quality_avg'], 2) if summary['data_quality_avg'] else 0,
            'date_range': {
                'earliest': summary['earliest_date'],
                'latest': summary['latest_date']
            }
        }
        
        return Response(summary_data)
        
    except Exception as e:
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@extend_schema(
    tags=['Adzuna Jobs'],
    parameters=[
        OpenApiParameter('date_from', OpenApiTypes.DATE, description='Start date filter'),
        OpenApiParameter('date_to', OpenApiTypes.DATE, description='End date filter'),
        OpenApiParameter('limit', OpenApiTypes.INT, description='Number of results (default: 10)'),
    ]
)
@api_view(['GET'])
@permission_classes([AuthenticatedReadOnlyPermission])
def job_market_by_country(request):
    """Get job market statistics by country"""
    try:
        date_from = request.GET.get('date_from')
        date_to = request.GET.get('date_to')
        limit = int(request.GET.get('limit', 10))
        
        # Build queryset
        queryset = AdzunaJobSilver.objects.all()
        
        if date_from:
            queryset = queryset.filter(date__gte=date_from)
        if date_to:
            queryset = queryset.filter(date__lte=date_to)
        
        # Aggregate by country
        country_data = queryset.values('country_code', 'country_name').annotate(
            total_jobs=Sum('job_count'),
            unique_job_titles=Count('job_title', distinct=True),
            avg_salary=Avg('average_salary'),
            data_quality_avg=Avg('data_quality_score')
        ).order_by('-total_jobs')[:limit]
        
        # Add most common job for each country
        result = []
        for country in country_data:
            # Get most common job title for this country
            most_common_job = queryset.filter(
                country_code=country['country_code']
            ).values('job_title').annotate(
                job_sum=Sum('job_count')
            ).order_by('-job_sum').first()
            
            result.append({
                'country_code': country['country_code'],
                'country_name': country['country_name'],
                'total_jobs': country['total_jobs'],
                'unique_job_titles': country['unique_job_titles'],
                'avg_salary': country['avg_salary'],
                'most_common_job': most_common_job['job_title'] if most_common_job else 'N/A',
                'data_quality_avg': round(country['data_quality_avg'], 2)
            })
        
        return Response({
            'job_market_by_country': result,
            'filters': {
                'date_from': date_from,
                'date_to': date_to,
                'limit': limit
            }
        })
        
    except Exception as e:
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@extend_schema(
    tags=['Adzuna Jobs'],
    parameters=[
        OpenApiParameter('country_code', OpenApiTypes.STR, description='Filter by country code'),
        OpenApiParameter('date_from', OpenApiTypes.DATE, description='Start date filter'),
        OpenApiParameter('date_to', OpenApiTypes.DATE, description='End date filter'),
        OpenApiParameter('limit', OpenApiTypes.INT, description='Number of results (default: 10)'),
    ]
)
@api_view(['GET'])
@permission_classes([AuthenticatedReadOnlyPermission])
def job_title_analysis(request):
    """Get detailed analysis of job titles and opportunities"""
    try:
        country_code = request.GET.get('country_code')
        date_from = request.GET.get('date_from')
        date_to = request.GET.get('date_to')
        limit = int(request.GET.get('limit', 10))
        
        # Build queryset
        queryset = AdzunaJobSilver.objects.all()
        
        if country_code:
            queryset = queryset.filter(country_code=country_code)
        if date_from:
            queryset = queryset.filter(date__gte=date_from)
        if date_to:
            queryset = queryset.filter(date__lte=date_to)
        
        # Aggregate by job title
        job_data = queryset.values('job_title').annotate(
            total_opportunities=Sum('job_count'),
            countries_available=Count('country_code', distinct=True),
            avg_salary=Avg('average_salary'),
            data_quality_avg=Avg('data_quality_score')
        ).order_by('-total_opportunities')[:limit]
        
        # Add salary ranges for each job title
        result = []
        for job in job_data:
            salary_ranges = list(
                queryset.filter(job_title=job['job_title'])
                .exclude(salary_range__isnull=True)
                .values_list('salary_range', flat=True)
                .distinct()
            )
            
            result.append({
                'job_title': job['job_title'],
                'total_opportunities': job['total_opportunities'],
                'countries_available': job['countries_available'],
                'avg_salary': job['avg_salary'],
                'salary_ranges': salary_ranges,
                'data_quality_avg': round(job['data_quality_avg'], 2)
            })
        
        return Response({
            'job_title_analysis': result,
            'filters': {
                'country_code': country_code,
                'date_from': date_from,
                'date_to': date_to,
                'limit': limit
            }
        })
        
    except Exception as e:
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@extend_schema(
    tags=['Adzuna Jobs'],
    parameters=[
        OpenApiParameter('country_code', OpenApiTypes.STR, description='Filter by country code'),
        OpenApiParameter('date_from', OpenApiTypes.DATE, description='Start date filter'),
        OpenApiParameter('date_to', OpenApiTypes.DATE, description='End date filter'),
    ]
)
@api_view(['GET'])
@permission_classes([AuthenticatedReadOnlyPermission])
def salary_analysis(request):
    """Get comprehensive salary analysis across job market"""
    try:
        country_code = request.GET.get('country_code')
        date_from = request.GET.get('date_from')
        date_to = request.GET.get('date_to')
        
        # Build queryset
        queryset = AdzunaJobSilver.objects.exclude(salary_range__isnull=True)
        
        if country_code:
            queryset = queryset.filter(country_code=country_code)
        if date_from:
            queryset = queryset.filter(date__gte=date_from)
        if date_to:
            queryset = queryset.filter(date__lte=date_to)
        
        # Aggregate by salary range
        salary_data = queryset.values('salary_range').annotate(
            job_count=Sum('job_count'),
            avg_numerical_salary=Avg('average_salary')
        ).order_by('-job_count')
        
        # Add countries and popular job titles for each salary range
        result = []
        for salary in salary_data:
            salary_queryset = queryset.filter(salary_range=salary['salary_range'])
            
            countries = list(
                salary_queryset.values_list('country_name', flat=True).distinct()
            )
            
            popular_jobs = list(
                salary_queryset.values('job_title').annotate(
                    total_count=Sum('job_count')
                ).order_by('-total_count')[:3].values_list('job_title', flat=True)
            )
            
            result.append({
                'salary_range': salary['salary_range'],
                'job_count': salary['job_count'],
                'countries': countries,
                'popular_job_titles': popular_jobs,
                'avg_numerical_salary': salary['avg_numerical_salary']
            })
        
        return Response({
            'salary_analysis': result,
            'filters': {
                'country_code': country_code,
                'date_from': date_from,
                'date_to': date_to
            }
        })
        
    except Exception as e:
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@extend_schema(
    tags=['Adzuna Jobs'],
    parameters=[
        OpenApiParameter('country_code', OpenApiTypes.STR, description='Filter by country code'),
        OpenApiParameter('job_title', OpenApiTypes.STR, description='Filter by job title'),
        OpenApiParameter('group_by', OpenApiTypes.STR, description='Group by: day or month (default: month)'),
    ]
)
@api_view(['GET'])
@permission_classes([AuthenticatedReadOnlyPermission])
def job_market_time_series(request):
    """Get time series data showing job market evolution over time"""
    try:
        country_code = request.GET.get('country_code')
        job_title = request.GET.get('job_title')
        group_by = request.GET.get('group_by', 'month')
        
        # Build queryset
        queryset = AdzunaJobSilver.objects.all()
        
        if country_code:
            queryset = queryset.filter(country_code=country_code)
        if job_title:
            queryset = queryset.filter(job_title__icontains=job_title)
        
        # Group by time period
        if group_by == 'day':
            time_series = queryset.values('date').annotate(
                total_jobs=Sum('job_count'),
                unique_job_titles=Count('job_title', distinct=True),
                countries_active=Count('country_code', distinct=True),
                avg_salary=Avg('average_salary')
            ).order_by('date')
        else:  # month
            time_series = queryset.extra(
                select={'period': "DATE_FORMAT(date, '%%Y-%%m')"}
            ).values('period').annotate(
                total_jobs=Sum('job_count'),
                unique_job_titles=Count('job_title', distinct=True),
                countries_active=Count('country_code', distinct=True),
                avg_salary=Avg('average_salary')
            ).order_by('period')
        
        # Format response
        if group_by == 'month':
            response_data = [
                {
                    'date': item['period'],
                    'total_jobs': item['total_jobs'],
                    'unique_job_titles': item['unique_job_titles'],
                    'countries_active': item['countries_active'],
                    'avg_salary': item['avg_salary']
                }
                for item in time_series
            ]
        else:
            response_data = [
                {
                    'date': item['date'],
                    'total_jobs': item['total_jobs'],
                    'unique_job_titles': item['unique_job_titles'],
                    'countries_active': item['countries_active'],
                    'avg_salary': item['avg_salary']
                }
                for item in time_series
            ]
        
        return Response({
            'time_series': response_data,
            'group_by': group_by,
            'filters': {
                'country_code': country_code,
                'job_title': job_title
            },
            'count': len(response_data)
        })
        
    except Exception as e:
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )