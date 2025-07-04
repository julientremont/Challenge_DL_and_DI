from rest_framework import generics, status
from rest_framework.decorators import api_view
from rest_framework.response import Response
from django_filters.rest_framework import DjangoFilterBackend
from django_filters import rest_framework as filters
from django.db.models import Avg, Count, Sum, Max, Min, Q, F, Case, When, IntegerField
from django.db.models.functions import TruncDate, TruncMonth
from drf_spectacular.utils import extend_schema, OpenApiParameter
from drf_spectacular.openapi import OpenApiTypes
from .models import JobicyJobSilver
from .serializers import (
    JobicyJobSilverSerializer, JobicyJobSilverListSerializer,
    JobicyJobSummarySerializer, JobMarketByCountrySerializer,
    JobTitleAnalysisSerializer, CompanyAnalysisSerializer,
    SalaryAnalysisSerializer, TimeSeriesJobDataSerializer
)


class JobicyJobSilverFilter(filters.FilterSet):
    """Custom filter for Jobicy Jobs Silver data"""
    job_title = filters.CharFilter(field_name='job_title', lookup_expr='icontains')
    company_name = filters.CharFilter(field_name='company_name', lookup_expr='icontains')
    country_code = filters.CharFilter(field_name='country_code', lookup_expr='exact')
    job_level = filters.CharFilter(field_name='job_level_standardized', lookup_expr='exact')
    job_type = filters.CharFilter(field_name='job_type_standardized', lookup_expr='exact')
    has_salary = filters.BooleanFilter(field_name='has_salary_info')
    salary_min = filters.NumberFilter(field_name='salary_min', lookup_expr='gte')
    salary_max = filters.NumberFilter(field_name='salary_max', lookup_expr='lte')
    quality_score_min = filters.NumberFilter(field_name='data_quality_score', lookup_expr='gte')
    processed_from = filters.DateTimeFilter(field_name='processed_date', lookup_expr='gte')
    processed_to = filters.DateTimeFilter(field_name='processed_date', lookup_expr='lte')
    
    class Meta:
        model = JobicyJobSilver
        fields = [
            'job_title', 'company_name', 'country_code', 'job_level', 'job_type',
            'has_salary', 'salary_min', 'salary_max', 'quality_score_min',
            'processed_from', 'processed_to'
        ]


@extend_schema(tags=['Jobicy Jobs'])
class JobicyJobSilverListView(generics.ListAPIView):
    """List Jobicy job postings with comprehensive filtering"""
    queryset = JobicyJobSilver.objects.all()
    serializer_class = JobicyJobSilverListSerializer
    filter_backends = [DjangoFilterBackend]
    filterset_class = JobicyJobSilverFilter
    ordering_fields = ['processed_date', 'salary_avg', 'data_quality_score', 'company_name']
    ordering = ['-processed_date']


@extend_schema(tags=['Jobicy Jobs'])
class JobicyJobSilverDetailView(generics.RetrieveAPIView):
    """Retrieve a specific job posting with full details"""
    queryset = JobicyJobSilver.objects.all()
    serializer_class = JobicyJobSilverSerializer


@extend_schema(
    tags=['Jobicy Jobs'],
    parameters=[
        OpenApiParameter('country_code', OpenApiTypes.STR, description='Filter by country code'),
        OpenApiParameter('job_level', OpenApiTypes.STR, description='Filter by job level'),
        OpenApiParameter('job_type', OpenApiTypes.STR, description='Filter by job type'),
        OpenApiParameter('has_salary', OpenApiTypes.BOOL, description='Filter by salary availability'),
    ]
)
@api_view(['GET'])
def jobicy_summary(request):
    """Get comprehensive job market summary statistics"""
    try:
        # Get query parameters
        country_code = request.GET.get('country_code')
        job_level = request.GET.get('job_level')
        job_type = request.GET.get('job_type')
        has_salary = request.GET.get('has_salary')
        
        # Build queryset
        queryset = JobicyJobSilver.objects.all()
        
        if country_code:
            queryset = queryset.filter(country_code=country_code)
        if job_level:
            queryset = queryset.filter(job_level_standardized=job_level)
        if job_type:
            queryset = queryset.filter(job_type_standardized=job_type)
        if has_salary is not None:
            queryset = queryset.filter(has_salary_info=has_salary.lower() == 'true')
        
        # Calculate summary statistics
        summary = queryset.aggregate(
            total_records=Count('id'),
            unique_job_titles=Count('job_title', distinct=True),
            unique_companies=Count('company_name', distinct=True),
            countries_covered=Count('country_code', distinct=True),
            jobs_with_salary=Count('id', filter=Q(has_salary_info=True)),
            avg_salary=Avg('salary_avg'),
            data_quality_avg=Avg('data_quality_score'),
            earliest_processed=Min('processed_date'),
            latest_processed=Max('processed_date')
        )
        
        # Get job level distribution
        job_level_dist = queryset.values('job_level_standardized').annotate(
            count=Count('id')
        ).order_by('-count')
        
        # Get job type distribution
        job_type_dist = queryset.values('job_type_standardized').annotate(
            count=Count('id')
        ).order_by('-count')
        
        # Get top countries
        top_countries = queryset.values('country_code').annotate(
            job_count=Count('id')
        ).order_by('-job_count')[:5]
        
        # Get top companies
        top_companies = queryset.values('company_name').annotate(
            job_count=Count('id')
        ).order_by('-job_count')[:5]
        
        # Format response
        summary_data = {
            'total_records': summary['total_records'] or 0,
            'unique_job_titles': summary['unique_job_titles'] or 0,
            'unique_companies': summary['unique_companies'] or 0,
            'countries_covered': summary['countries_covered'] or 0,
            'jobs_with_salary': summary['jobs_with_salary'] or 0,
            'avg_salary': round(summary['avg_salary'], 2) if summary['avg_salary'] else None,
            'data_quality_avg': round(summary['data_quality_avg'], 2) if summary['data_quality_avg'] else 0,
            'job_level_distribution': {item['job_level_standardized']: item['count'] for item in job_level_dist},
            'job_type_distribution': {item['job_type_standardized']: item['count'] for item in job_type_dist},
            'top_countries': [{'country_code': item['country_code'], 'job_count': item['job_count']} for item in top_countries],
            'top_companies': [{'company_name': item['company_name'], 'job_count': item['job_count']} for item in top_companies]
        }
        
        return Response(summary_data)
        
    except Exception as e:
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@extend_schema(
    tags=['Jobicy Jobs'],
    parameters=[
        OpenApiParameter('job_level', OpenApiTypes.STR, description='Filter by job level'),
        OpenApiParameter('job_type', OpenApiTypes.STR, description='Filter by job type'),
        OpenApiParameter('limit', OpenApiTypes.INT, description='Number of results (default: 10)'),
    ]
)
@api_view(['GET'])
def jobicy_by_country(request):
    """Get job market statistics by country"""
    try:
        job_level = request.GET.get('job_level')
        job_type = request.GET.get('job_type')
        limit = int(request.GET.get('limit', 10))
        
        # Build queryset
        queryset = JobicyJobSilver.objects.all()
        
        if job_level:
            queryset = queryset.filter(job_level_standardized=job_level)
        if job_type:
            queryset = queryset.filter(job_type_standardized=job_type)
        
        # Aggregate by country
        country_data = queryset.values('country_code').annotate(
            total_jobs=Count('id'),
            unique_job_titles=Count('job_title', distinct=True),
            unique_companies=Count('company_name', distinct=True),
            avg_salary=Avg('salary_avg'),
            data_quality_avg=Avg('data_quality_score'),
            jobs_with_salary=Count('id', filter=Q(has_salary_info=True))
        ).order_by('-total_jobs')[:limit]
        
        # Add most common job and company for each country
        result = []
        for country in country_data:
            country_queryset = queryset.filter(country_code=country['country_code'])
            
            # Get most common job title
            most_common_job = country_queryset.values('job_title').annotate(
                job_count=Count('job_title')
            ).order_by('-job_count').first()
            
            # Get most common company
            most_common_company = country_queryset.values('company_name').annotate(
                company_count=Count('company_name')
            ).order_by('-company_count').first()
            
            # Get job level distribution for this country
            job_level_dist = country_queryset.values('job_level_standardized').annotate(
                count=Count('id')
            ).order_by('-count')
            
            result.append({
                'country_code': country['country_code'],
                'total_jobs': country['total_jobs'],
                'unique_job_titles': country['unique_job_titles'],
                'unique_companies': country['unique_companies'],
                'avg_salary': round(country['avg_salary'], 2) if country['avg_salary'] else None,
                'most_common_job': most_common_job['job_title'] if most_common_job else 'N/A',
                'most_common_company': most_common_company['company_name'] if most_common_company else 'N/A',
                'job_level_distribution': {item['job_level_standardized']: item['count'] for item in job_level_dist},
                'data_quality_avg': round(country['data_quality_avg'], 2),
                'jobs_with_salary': country['jobs_with_salary']
            })
        
        return Response({
            'country_stats': result,
            'filters': {
                'job_level': job_level,
                'job_type': job_type,
                'limit': limit
            }
        })
        
    except Exception as e:
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@extend_schema(
    tags=['Jobicy Jobs'],
    parameters=[
        OpenApiParameter('country_code', OpenApiTypes.STR, description='Filter by country code'),
        OpenApiParameter('job_level', OpenApiTypes.STR, description='Filter by job level'),
        OpenApiParameter('limit', OpenApiTypes.INT, description='Number of results (default: 10)'),
    ]
)
@api_view(['GET'])
def jobicy_job_analysis(request):
    """Get detailed analysis of job titles and opportunities"""
    try:
        country_code = request.GET.get('country_code')
        job_level = request.GET.get('job_level')
        limit = int(request.GET.get('limit', 10))
        
        # Build queryset
        queryset = JobicyJobSilver.objects.all()
        
        if country_code:
            queryset = queryset.filter(country_code=country_code)
        if job_level:
            queryset = queryset.filter(job_level_standardized=job_level)
        
        # Aggregate by job title
        job_data = queryset.values('job_title').annotate(
            total_opportunities=Count('id'),
            countries_available=Count('country_code', distinct=True),
            companies_hiring=Count('company_name', distinct=True),
            avg_salary=Avg('salary_avg'),
            min_salary=Min('salary_min'),
            max_salary=Max('salary_max'),
            data_quality_avg=Avg('data_quality_score'),
            jobs_with_salary=Count('id', filter=Q(has_salary_info=True))
        ).order_by('-total_opportunities')[:limit]
        
        # Add top countries and companies for each job
        result = []
        for job in job_data:
            job_queryset = queryset.filter(job_title=job['job_title'])
            
            # Get top countries for this job
            top_countries = job_queryset.values('country_code').annotate(
                count=Count('id')
            ).order_by('-count')[:3]
            
            # Get top companies for this job
            top_companies = job_queryset.values('company_name').annotate(
                count=Count('id')
            ).order_by('-count')[:3]
            
            result.append({
                'job_title': job['job_title'],
                'total_opportunities': job['total_opportunities'],
                'countries_available': job['countries_available'],
                'companies_hiring': job['companies_hiring'],
                'avg_salary': round(job['avg_salary'], 2) if job['avg_salary'] else None,
                'salary_range': {
                    'min': job['min_salary'],
                    'max': job['max_salary'],
                    'avg': round(job['avg_salary'], 2) if job['avg_salary'] else None
                },
                'top_countries': [item['country_code'] for item in top_countries],
                'top_companies': [item['company_name'] for item in top_companies],
                'data_quality_avg': round(job['data_quality_avg'], 2),
                'jobs_with_salary': job['jobs_with_salary']
            })
        
        return Response({
            'job_analysis': result,
            'filters': {
                'country_code': country_code,
                'job_level': job_level,
                'limit': limit
            }
        })
        
    except Exception as e:
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@extend_schema(
    tags=['Jobicy Jobs'],
    parameters=[
        OpenApiParameter('country_code', OpenApiTypes.STR, description='Filter by country code'),
        OpenApiParameter('job_level', OpenApiTypes.STR, description='Filter by job level'),
        OpenApiParameter('limit', OpenApiTypes.INT, description='Number of results (default: 10)'),
    ]
)
@api_view(['GET'])
def jobicy_company_analysis(request):
    """Get detailed analysis of companies and their hiring patterns"""
    try:
        country_code = request.GET.get('country_code')
        job_level = request.GET.get('job_level')
        limit = int(request.GET.get('limit', 10))
        
        # Build queryset
        queryset = JobicyJobSilver.objects.all()
        
        if country_code:
            queryset = queryset.filter(country_code=country_code)
        if job_level:
            queryset = queryset.filter(job_level_standardized=job_level)
        
        # Aggregate by company
        company_data = queryset.values('company_name').annotate(
            total_jobs=Count('id'),
            unique_job_titles=Count('job_title', distinct=True),
            countries_present=Count('country_code', distinct=True),
            avg_salary=Avg('salary_avg'),
            data_quality_avg=Avg('data_quality_score'),
            jobs_with_salary=Count('id', filter=Q(has_salary_info=True))
        ).order_by('-total_jobs')[:limit]
        
        # Add details for each company
        result = []
        for company in company_data:
            company_queryset = queryset.filter(company_name=company['company_name'])
            
            # Get most common job
            most_common_job = company_queryset.values('job_title').annotate(
                count=Count('id')
            ).order_by('-count').first()
            
            # Get job level distribution
            job_level_dist = company_queryset.values('job_level_standardized').annotate(
                count=Count('id')
            ).order_by('-count')
            
            # Get top countries
            top_countries = company_queryset.values('country_code').annotate(
                count=Count('id')
            ).order_by('-count')[:3]
            
            result.append({
                'company_name': company['company_name'],
                'total_jobs': company['total_jobs'],
                'unique_job_titles': company['unique_job_titles'],
                'countries_present': company['countries_present'],
                'avg_salary': round(company['avg_salary'], 2) if company['avg_salary'] else None,
                'most_common_job': most_common_job['job_title'] if most_common_job else 'N/A',
                'job_level_distribution': {item['job_level_standardized']: item['count'] for item in job_level_dist},
                'top_countries': [item['country_code'] for item in top_countries],
                'data_quality_avg': round(company['data_quality_avg'], 2),
                'jobs_with_salary': company['jobs_with_salary']
            })
        
        return Response({
            'company_analysis': result,
            'filters': {
                'country_code': country_code,
                'job_level': job_level,
                'limit': limit
            }
        })
        
    except Exception as e:
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@extend_schema(
    tags=['Jobicy Jobs'],
    parameters=[
        OpenApiParameter('country_code', OpenApiTypes.STR, description='Filter by country code'),
        OpenApiParameter('job_level', OpenApiTypes.STR, description='Filter by job level'),
        OpenApiParameter('job_type', OpenApiTypes.STR, description='Filter by job type'),
    ]
)
@api_view(['GET'])
def jobicy_salary_analysis(request):
    """Get comprehensive salary analysis across job market"""
    try:
        country_code = request.GET.get('country_code')
        job_level = request.GET.get('job_level')
        job_type = request.GET.get('job_type')
        
        # Build queryset - only records with salary data
        queryset = JobicyJobSilver.objects.filter(has_salary_info=True)
        
        if country_code:
            queryset = queryset.filter(country_code=country_code)
        if job_level:
            queryset = queryset.filter(job_level_standardized=job_level)
        if job_type:
            queryset = queryset.filter(job_type_standardized=job_type)
        
        # Calculate salary statistics
        salary_stats = queryset.aggregate(
            total_with_salary=Count('id'),
            avg_salary=Avg('salary_avg'),
            min_salary=Min('salary_min'),
            max_salary=Max('salary_max')
        )
        
        # Define salary ranges
        salary_ranges = [
            ('0-30000', 0, 30000),
            ('30000-50000', 30000, 50000),
            ('50000-70000', 50000, 70000),
            ('70000-100000', 70000, 100000),
            ('100000+', 100000, float('inf'))
        ]
        
        # Get salary distribution
        salary_distribution = []
        for range_name, min_val, max_val in salary_ranges:
            if max_val == float('inf'):
                range_queryset = queryset.filter(salary_avg__gte=min_val)
            else:
                range_queryset = queryset.filter(salary_avg__gte=min_val, salary_avg__lt=max_val)
            
            if range_queryset.exists():
                # Get details for this salary range
                countries = list(range_queryset.values_list('country_code', flat=True).distinct())
                job_titles = list(range_queryset.values('job_title').annotate(
                    count=Count('id')
                ).order_by('-count')[:5].values_list('job_title', flat=True))
                companies = list(range_queryset.values('company_name').annotate(
                    count=Count('id')
                ).order_by('-count')[:5].values_list('company_name', flat=True))
                
                # Get job level distribution
                job_level_dist = range_queryset.values('job_level_standardized').annotate(
                    count=Count('id')
                ).order_by('-count')
                
                salary_distribution.append({
                    'salary_range': range_name,
                    'job_count': range_queryset.count(),
                    'countries': countries,
                    'popular_job_titles': job_titles,
                    'popular_companies': companies,
                    'avg_numerical_salary': round(range_queryset.aggregate(avg=Avg('salary_avg'))['avg'], 2),
                    'job_level_distribution': {item['job_level_standardized']: item['count'] for item in job_level_dist}
                })
        
        # Get top paying jobs
        top_paying_jobs = queryset.values('job_title').annotate(
            avg_salary=Avg('salary_avg'),
            job_count=Count('id')
        ).order_by('-avg_salary')[:10]
        
        # Get salary by country
        salary_by_country = queryset.values('country_code').annotate(
            avg_salary=Avg('salary_avg'),
            job_count=Count('id')
        ).order_by('-avg_salary')[:10]
        
        result = {
            'overall_stats': {
                'total_records_with_salary': salary_stats['total_with_salary'],
                'average_salary': round(salary_stats['avg_salary'], 2) if salary_stats['avg_salary'] else None,
                'minimum_salary': salary_stats['min_salary'],
                'maximum_salary': salary_stats['max_salary']
            },
            'salary_distribution': salary_distribution,
            'top_paying_jobs': [
                {
                    'job_title': job['job_title'],
                    'avg_salary': round(job['avg_salary'], 2),
                    'job_count': job['job_count']
                }
                for job in top_paying_jobs
            ],
            'salary_by_country': [
                {
                    'country_code': country['country_code'],
                    'avg_salary': round(country['avg_salary'], 2),
                    'job_count': country['job_count']
                }
                for country in salary_by_country
            ]
        }
        
        return Response({
            'salary_analysis': result,
            'filters': {
                'country_code': country_code,
                'job_level': job_level,
                'job_type': job_type
            }
        })
        
    except Exception as e:
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@extend_schema(
    tags=['Jobicy Jobs'],
    parameters=[
        OpenApiParameter('country_code', OpenApiTypes.STR, description='Filter by country code'),
        OpenApiParameter('job_title', OpenApiTypes.STR, description='Filter by job title'),
        OpenApiParameter('company_name', OpenApiTypes.STR, description='Filter by company name'),
        OpenApiParameter('group_by', OpenApiTypes.STR, description='Group by: day or month (default: day)'),
    ]
)
@api_view(['GET'])
def jobicy_time_series(request):
    """Get time series data showing job market evolution over time"""
    try:
        country_code = request.GET.get('country_code')
        job_title = request.GET.get('job_title')
        company_name = request.GET.get('company_name')
        group_by = request.GET.get('group_by', 'day')
        
        # Build queryset
        queryset = JobicyJobSilver.objects.all()
        
        if country_code:
            queryset = queryset.filter(country_code=country_code)
        if job_title:
            queryset = queryset.filter(job_title__icontains=job_title)
        if company_name:
            queryset = queryset.filter(company_name__icontains=company_name)
        
        # Group by time period
        if group_by == 'day':
            time_series = queryset.extra(
                select={'period': "DATE(processed_date)"}
            ).values('period').annotate(
                total_jobs=Count('id'),
                unique_job_titles=Count('job_title', distinct=True),
                unique_companies=Count('company_name', distinct=True),
                countries_active=Count('country_code', distinct=True),
                avg_salary=Avg('salary_avg'),
                jobs_with_salary=Count('id', filter=Q(has_salary_info=True)),
                quality_score_avg=Avg('data_quality_score')
            ).order_by('period')
        else:  # month
            time_series = queryset.extra(
                select={'period': "DATE_FORMAT(processed_date, '%Y-%m')"}
            ).values('period').annotate(
                total_jobs=Count('id'),
                unique_job_titles=Count('job_title', distinct=True),
                unique_companies=Count('company_name', distinct=True),
                countries_active=Count('country_code', distinct=True),
                avg_salary=Avg('salary_avg'),
                jobs_with_salary=Count('id', filter=Q(has_salary_info=True)),
                quality_score_avg=Avg('data_quality_score')
            ).order_by('period')
        
        # Format response
        response_data = [
            {
                'date': item['period'],
                'total_jobs': item['total_jobs'],
                'unique_job_titles': item['unique_job_titles'],
                'unique_companies': item['unique_companies'],
                'countries_active': item['countries_active'],
                'avg_salary': round(item['avg_salary'], 2) if item['avg_salary'] else None,
                'jobs_with_salary': item['jobs_with_salary'],
                'quality_score_avg': round(item['quality_score_avg'], 2) if item['quality_score_avg'] else 0
            }
            for item in time_series
        ]
        
        return Response({
            'time_series': response_data,
            'group_by': group_by,
            'filters': {
                'country_code': country_code,
                'job_title': job_title,
                'company_name': company_name
            },
            'count': len(response_data)
        })
        
    except Exception as e:
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )