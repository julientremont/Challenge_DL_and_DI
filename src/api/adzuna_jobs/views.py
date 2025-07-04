from rest_framework import generics, status
from rest_framework.decorators import api_view
from rest_framework.response import Response
from django_filters.rest_framework import DjangoFilterBackend
from django_filters import rest_framework as filters
from django.db.models import Avg, Count, Sum, Max, Min, Q, F
from django.db.models.functions import TruncDate, TruncMonth
from drf_spectacular.utils import extend_schema, OpenApiParameter
from drf_spectacular.openapi import OpenApiTypes
from .models import AdzunaJobSilver
from .serializers import AdzunaJobSilverSerializer


class AdzunaJobSilverFilter(filters.FilterSet):
    """Custom filter for Adzuna Jobs Silver data"""
    job_title = filters.CharFilter(field_name='job_title', lookup_expr='icontains')
    country_code = filters.CharFilter(field_name='country_code', lookup_expr='exact')
    date_from = filters.DateFilter(field_name='date', lookup_expr='gte')
    date_to = filters.DateFilter(field_name='date', lookup_expr='lte')
    average_salary_min = filters.NumberFilter(field_name='average_salary', lookup_expr='gte')
    average_salary_max = filters.NumberFilter(field_name='average_salary', lookup_expr='lte')
    quality_score_min = filters.NumberFilter(field_name='data_quality_score', lookup_expr='gte')
    
    class Meta:
        model = AdzunaJobSilver
        fields = ['job_title', 'country_code', 'date_from', 'date_to', 'average_salary_min', 'average_salary_max', 'quality_score_min']


@extend_schema(tags=['Adzuna Jobs'])
class AdzunaJobSilverListView(generics.ListAPIView):
    """List Adzuna job market data with filtering"""
    queryset = AdzunaJobSilver.objects.all()
    serializer_class = AdzunaJobSilverSerializer
    filter_backends = [DjangoFilterBackend]
    filterset_class = AdzunaJobSilverFilter
    ordering_fields = ['date', 'average_salary', 'data_quality_score']
    ordering = ['-date']


@extend_schema(tags=['Adzuna Jobs'])
class AdzunaJobSilverDetailView(generics.RetrieveAPIView):
    """Retrieve a specific job market record"""
    queryset = AdzunaJobSilver.objects.all()
    serializer_class = AdzunaJobSilverSerializer


@extend_schema(
    tags=['Adzuna Jobs'],
    parameters=[
        OpenApiParameter('country_code', OpenApiTypes.STR, description='Filter by country code'),
        OpenApiParameter('date_from', OpenApiTypes.DATE, description='Start date filter'),
        OpenApiParameter('date_to', OpenApiTypes.DATE, description='End date filter'),
    ]
)
@api_view(['GET'])
def adzuna_summary(request):
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
            avg_salary=Avg('average_salary'),
            max_salary=Max('average_salary'),
            min_salary=Min('average_salary'),
            data_quality_avg=Avg('data_quality_score'),
            earliest_date=Min('date'),
            latest_date=Max('date')
        )
        
        # Format response
        summary_data = {
            'total_records': summary['total_records'] or 0,
            'unique_job_titles': summary['unique_job_titles'] or 0,
            'countries_covered': summary['countries_covered'] or 0,
            'avg_salary': round(summary['avg_salary'], 2) if summary['avg_salary'] else None,
            'max_salary': summary['max_salary'],
            'min_salary': summary['min_salary'],
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
def adzuna_by_country(request):
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
        country_data = queryset.values('country_code').annotate(
            total_records=Count('id'),
            unique_job_titles=Count('job_title', distinct=True),
            avg_salary=Avg('average_salary'),
            max_salary=Max('average_salary'),
            data_quality_avg=Avg('data_quality_score')
        ).order_by('-total_records')[:limit]
        
        # Add most common job for each country
        result = []
        for country in country_data:
            # Get most common job title for this country
            most_common_job = queryset.filter(
                country_code=country['country_code']
            ).values('job_title').annotate(
                job_count=Count('job_title')
            ).order_by('-job_count').first()
            
            result.append({
                'country_code': country['country_code'],
                'total_records': country['total_records'],
                'unique_job_titles': country['unique_job_titles'],
                'avg_salary': round(country['avg_salary'], 2) if country['avg_salary'] else None,
                'max_salary': country['max_salary'],
                'most_common_job': most_common_job['job_title'] if most_common_job else 'N/A',
                'data_quality_avg': round(country['data_quality_avg'], 2)
            })
        
        return Response({
            'country_stats': result,
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
def adzuna_job_title_analysis(request):
    """Get detailed analysis of job titles"""
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
            total_records=Count('id'),
            countries_available=Count('country_code', distinct=True),
            avg_salary=Avg('average_salary'),
            max_salary=Max('average_salary'),
            data_quality_avg=Avg('data_quality_score')
        ).order_by('-total_records')[:limit]
        
        result = [
            {
                'job_title': job['job_title'],
                'total_records': job['total_records'],
                'countries_available': job['countries_available'],
                'avg_salary': round(job['avg_salary'], 2) if job['avg_salary'] else None,
                'max_salary': job['max_salary'],
                'data_quality_avg': round(job['data_quality_avg'], 2)
            }
            for job in job_data
        ]
        
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
def adzuna_salary_analysis(request):
    """Get comprehensive salary analysis across job market"""
    try:
        country_code = request.GET.get('country_code')
        date_from = request.GET.get('date_from')
        date_to = request.GET.get('date_to')
        
        # Build queryset - only records with salary data
        queryset = AdzunaJobSilver.objects.exclude(average_salary__isnull=True)
        
        if country_code:
            queryset = queryset.filter(country_code=country_code)
        if date_from:
            queryset = queryset.filter(date__gte=date_from)
        if date_to:
            queryset = queryset.filter(date__lte=date_to)
        
        # Calculate salary statistics
        salary_stats = queryset.aggregate(
            total_with_salary=Count('id'),
            avg_salary=Avg('average_salary'),
            median_salary=Avg('average_salary'),  # Simplified median
            min_salary=Min('average_salary'),
            max_salary=Max('average_salary')
        )
        
        # Get salary ranges by job title
        job_salary_data = queryset.values('job_title').annotate(
            record_count=Count('id'),
            avg_salary=Avg('average_salary'),
            min_salary=Min('average_salary'),
            max_salary=Max('average_salary')
        ).order_by('-avg_salary')[:10]
        
        # Get salary data by country
        country_salary_data = queryset.values('country_code').annotate(
            record_count=Count('id'),
            avg_salary=Avg('average_salary'),
            min_salary=Min('average_salary'),
            max_salary=Max('average_salary')
        ).order_by('-avg_salary')[:10]
        
        result = {
            'overall_stats': {
                'total_records_with_salary': salary_stats['total_with_salary'],
                'average_salary': round(salary_stats['avg_salary'], 2) if salary_stats['avg_salary'] else None,
                'minimum_salary': salary_stats['min_salary'],
                'maximum_salary': salary_stats['max_salary']
            },
            'top_paying_jobs': [
                {
                    'job_title': job['job_title'],
                    'record_count': job['record_count'],
                    'avg_salary': round(job['avg_salary'], 2),
                    'salary_range': f"{job['min_salary']:.2f} - {job['max_salary']:.2f}"
                }
                for job in job_salary_data
            ],
            'salary_by_country': [
                {
                    'country_code': country['country_code'],
                    'record_count': country['record_count'],
                    'avg_salary': round(country['avg_salary'], 2),
                    'salary_range': f"{country['min_salary']:.2f} - {country['max_salary']:.2f}"
                }
                for country in country_salary_data
            ]
        }
        
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
def adzuna_time_series(request):
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
                total_records=Count('id'),
                unique_job_titles=Count('job_title', distinct=True),
                countries_active=Count('country_code', distinct=True),
                avg_salary=Avg('average_salary')
            ).order_by('date')
        else:  # month
            time_series = queryset.extra(
                select={'period': "DATE_FORMAT(date, '%Y-%m')"}
            ).values('period').annotate(
                total_records=Count('id'),
                unique_job_titles=Count('job_title', distinct=True),
                countries_active=Count('country_code', distinct=True),
                avg_salary=Avg('average_salary')
            ).order_by('period')
        
        # Format response
        if group_by == 'month':
            response_data = [
                {
                    'date': item['period'],
                    'total_records': item['total_records'],
                    'unique_job_titles': item['unique_job_titles'],
                    'countries_active': item['countries_active'],
                    'avg_salary': round(item['avg_salary'], 2) if item['avg_salary'] else None
                }
                for item in time_series
            ]
        else:
            response_data = [
                {
                    'date': item['date'],
                    'total_records': item['total_records'],
                    'unique_job_titles': item['unique_job_titles'],
                    'countries_active': item['countries_active'],
                    'avg_salary': round(item['avg_salary'], 2) if item['avg_salary'] else None
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