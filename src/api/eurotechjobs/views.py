from rest_framework import generics, status
from rest_framework.decorators import api_view
from rest_framework.response import Response
from django_filters.rest_framework import DjangoFilterBackend
from django_filters import rest_framework as filters
from django.db.models import Avg, Sum, Max, Min, Count, Q
from django.db.models.functions import TruncDate, TruncMonth, TruncYear
from drf_spectacular.utils import extend_schema, OpenApiParameter
from drf_spectacular.openapi import OpenApiTypes
from collections import Counter
from .models import EuroTechJobs
from .serializers import (
    EuroTechJobsSerializer, EuroTechJobsListSerializer, EuroTechCountryStatsSerializer,
    EuroTechTechnologyStatsSerializer, EuroTechJobTypeStatsSerializer, 
    EuroTechCompanyStatsSerializer, EuroTechTimeSeriesSerializer,
    EuroTechQualityStatsSerializer
)


class EuroTechJobsFilter(filters.FilterSet):
    """Custom filter for EuroTechJobs model"""
    job_title = filters.CharFilter(field_name='job_title', lookup_expr='icontains')
    job_title_category = filters.CharFilter(field_name='job_title_category', lookup_expr='icontains')
    company = filters.CharFilter(field_name='company', lookup_expr='icontains')
    country = filters.CharFilter(field_name='country_code', lookup_expr='exact')
    location = filters.CharFilter(field_name='location', lookup_expr='icontains')
    technology = filters.CharFilter(field_name='technologies', lookup_expr='icontains')
    primary_technology = filters.CharFilter(field_name='primary_technology', lookup_expr='exact')
    job_type = filters.CharFilter(field_name='job_type', lookup_expr='exact')
    category = filters.CharFilter(field_name='category', lookup_expr='icontains')
    tech_count_min = filters.NumberFilter(field_name='tech_count', lookup_expr='gte')
    tech_count_max = filters.NumberFilter(field_name='tech_count', lookup_expr='lte')
    quality_min = filters.NumberFilter(field_name='data_quality_score', lookup_expr='gte')
    processed_from = filters.DateTimeFilter(field_name='processed_at', lookup_expr='gte')
    processed_to = filters.DateTimeFilter(field_name='processed_at', lookup_expr='lte')
    
    class Meta:
        model = EuroTechJobs
        fields = [
            'job_title', 'job_title_category', 'company', 'country', 'location',
            'technology', 'primary_technology', 'job_type', 'category',
            'tech_count_min', 'tech_count_max', 'quality_min',
            'processed_from', 'processed_to'
        ]


@extend_schema(tags=['EuroTechJobs'])
class EuroTechJobsListView(generics.ListAPIView):
    """List all EuroTechJobs with filtering and pagination"""
    queryset = EuroTechJobs.objects.all()
    serializer_class = EuroTechJobsListSerializer
    filter_backends = [DjangoFilterBackend]
    filterset_class = EuroTechJobsFilter
    ordering_fields = ['processed_at', 'data_quality_score', 'tech_count', 'job_title']
    ordering = ['-data_quality_score', '-processed_at']


@extend_schema(tags=['EuroTechJobs'])
class EuroTechJobsDetailView(generics.RetrieveAPIView):
    """Retrieve a specific EuroTechJobs posting"""
    queryset = EuroTechJobs.objects.all()
    serializer_class = EuroTechJobsSerializer


@extend_schema(
    tags=['EuroTechJobs'],
    parameters=[
        OpenApiParameter('country', OpenApiTypes.STR, description='Filter by country code'),
        OpenApiParameter('technology', OpenApiTypes.STR, description='Filter by technology'),
        OpenApiParameter('job_type', OpenApiTypes.STR, description='Filter by job type'),
        OpenApiParameter('category', OpenApiTypes.STR, description='Filter by category'),
        OpenApiParameter('processed_from', OpenApiTypes.DATETIME, description='Processed after date'),
        OpenApiParameter('processed_to', OpenApiTypes.DATETIME, description='Processed before date'),
    ]
)
@api_view(['GET'])
def eurotechjobs_summary(request):
    """Get summary statistics for EuroTechJobs"""
    try:
        # Get query parameters
        country = request.GET.get('country')
        technology = request.GET.get('technology')
        job_type = request.GET.get('job_type')
        category = request.GET.get('category')
        processed_from = request.GET.get('processed_from')
        processed_to = request.GET.get('processed_to')
        
        # Build queryset
        queryset = EuroTechJobs.objects.all()
        
        if country:
            queryset = queryset.filter(country_code=country)
        if technology:
            queryset = queryset.filter(technologies__icontains=technology)
        if job_type:
            queryset = queryset.filter(job_type=job_type)
        if category:
            queryset = queryset.filter(category__icontains=category)
        if processed_from:
            queryset = queryset.filter(processed_at__gte=processed_from)
        if processed_to:
            queryset = queryset.filter(processed_at__lte=processed_to)
        
        # Calculate summary statistics
        summary = queryset.aggregate(
            total_jobs=Count('id'),
            avg_quality_score=Avg('data_quality_score'),
            max_quality_score=Max('data_quality_score'),
            min_quality_score=Min('data_quality_score'),
            avg_tech_count=Avg('tech_count'),
            max_tech_count=Max('tech_count'),
        )
        
        # Calculate unique counts
        summary['unique_countries'] = queryset.values('country_code').distinct().count()
        summary['unique_companies'] = queryset.values('company').distinct().count()
        summary['unique_technologies'] = queryset.values('primary_technology').distinct().count()
        summary['unique_categories'] = queryset.values('category').distinct().count()
        
        # Job type distribution
        job_type_distribution = queryset.values('job_type').annotate(
            count=Count('id')
        ).order_by('-count')
        
        # Country distribution
        country_distribution = queryset.values('country_code').annotate(
            count=Count('id')
        ).order_by('-count')[:10]
        
        return Response({
            'summary': summary,
            'job_type_distribution': list(job_type_distribution),
            'top_countries': list(country_distribution),
            'filters_applied': {
                'country': country,
                'technology': technology,
                'job_type': job_type,
                'category': category,
                'processed_from': processed_from,
                'processed_to': processed_to
            }
        })
        
    except Exception as e:
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@extend_schema(
    tags=['EuroTechJobs'],
    parameters=[
        OpenApiParameter('technology', OpenApiTypes.STR, description='Filter by technology'),
        OpenApiParameter('job_type', OpenApiTypes.STR, description='Filter by job type'),
        OpenApiParameter('processed_from', OpenApiTypes.DATETIME, description='Processed after date'),
        OpenApiParameter('processed_to', OpenApiTypes.DATETIME, description='Processed before date'),
        OpenApiParameter('limit', OpenApiTypes.INT, description='Number of results (default: 20)'),
    ]
)
@api_view(['GET'])
def eurotechjobs_country_stats(request):
    """Get statistics aggregated by country"""
    try:
        technology = request.GET.get('technology')
        job_type = request.GET.get('job_type')
        processed_from = request.GET.get('processed_from')
        processed_to = request.GET.get('processed_to')
        limit = int(request.GET.get('limit', 20))
        
        # Build queryset
        queryset = EuroTechJobs.objects.all()
        
        if technology:
            queryset = queryset.filter(technologies__icontains=technology)
        if job_type:
            queryset = queryset.filter(job_type=job_type)
        if processed_from:
            queryset = queryset.filter(processed_at__gte=processed_from)
        if processed_to:
            queryset = queryset.filter(processed_at__lte=processed_to)
        
        # Aggregate by country
        country_stats = queryset.values('country_code').annotate(
            job_count=Count('id'),
            avg_quality_score=Avg('data_quality_score'),
            company_count=Count('company', distinct=True),
            avg_tech_count=Avg('tech_count')
        ).order_by('-job_count')[:limit]
        
        # Add top technology and job type for each country
        response_data = []
        for country_stat in country_stats:
            country = country_stat['country_code']
            country_jobs = queryset.filter(country_code=country)
            
            # Get top technology
            top_tech = country_jobs.values('primary_technology').annotate(
                count=Count('id')
            ).order_by('-count').first()
            
            # Get top job type
            top_job_type = country_jobs.values('job_type').annotate(
                count=Count('id')
            ).order_by('-count').first()
            
            # Technology distribution
            tech_dist = country_jobs.values('primary_technology').annotate(
                count=Count('id')
            ).order_by('-count')[:5]
            
            technology_distribution = {item['primary_technology']: item['count'] for item in tech_dist}
            
            response_data.append({
                'country_code': country,
                'job_count': country_stat['job_count'],
                'avg_quality_score': round(country_stat['avg_quality_score'] or 0, 2),
                'company_count': country_stat['company_count'],
                'avg_tech_count': round(country_stat['avg_tech_count'] or 0, 2),
                'top_technology': top_tech['primary_technology'] if top_tech else 'unknown',
                'top_job_type': top_job_type['job_type'] if top_job_type else 'unknown',
                'technology_distribution': technology_distribution
            })
        
        return Response({
            'country_stats': response_data,
            'count': len(response_data),
            'filters': {
                'technology': technology,
                'job_type': job_type,
                'processed_from': processed_from,
                'processed_to': processed_to,
                'limit': limit
            }
        })
        
    except Exception as e:
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@extend_schema(
    tags=['EuroTechJobs'],
    parameters=[
        OpenApiParameter('country', OpenApiTypes.STR, description='Filter by country'),
        OpenApiParameter('job_type', OpenApiTypes.STR, description='Filter by job type'),
        OpenApiParameter('processed_from', OpenApiTypes.DATETIME, description='Processed after date'),
        OpenApiParameter('processed_to', OpenApiTypes.DATETIME, description='Processed before date'),
        OpenApiParameter('limit', OpenApiTypes.INT, description='Number of results (default: 20)'),
    ]
)
@api_view(['GET'])
def eurotechjobs_technology_stats(request):
    """Get statistics aggregated by technology"""
    try:
        country = request.GET.get('country')
        job_type = request.GET.get('job_type')
        processed_from = request.GET.get('processed_from')
        processed_to = request.GET.get('processed_to')
        limit = int(request.GET.get('limit', 20))
        
        # Build queryset
        queryset = EuroTechJobs.objects.all()
        
        if country:
            queryset = queryset.filter(country_code=country)
        if job_type:
            queryset = queryset.filter(job_type=job_type)
        if processed_from:
            queryset = queryset.filter(processed_at__gte=processed_from)
        if processed_to:
            queryset = queryset.filter(processed_at__lte=processed_to)
        
        # Aggregate by technology
        tech_stats = queryset.values('primary_technology').annotate(
            job_count=Count('id'),
            avg_quality_score=Avg('data_quality_score'),
            country_count=Count('country_code', distinct=True),
            avg_tech_count=Avg('tech_count')
        ).order_by('-job_count')[:limit]
        
        response_data = []
        for tech_stat in tech_stats:
            tech = tech_stat['primary_technology']
            tech_jobs = queryset.filter(primary_technology=tech)
            
            # Job type distribution
            job_type_dist = tech_jobs.values('job_type').annotate(
                count=Count('id')
            ).order_by('-count')
            job_type_distribution = {item['job_type']: item['count'] for item in job_type_dist}
            
            # Top countries
            top_countries = list(tech_jobs.values('country_code').annotate(
                count=Count('id')
            ).order_by('-count')[:3].values_list('country_code', flat=True))
            
            response_data.append({
                'primary_technology': tech,
                'job_count': tech_stat['job_count'],
                'avg_quality_score': round(tech_stat['avg_quality_score'] or 0, 2),
                'country_count': tech_stat['country_count'],
                'avg_tech_count': round(tech_stat['avg_tech_count'] or 0, 2),
                'job_type_distribution': job_type_distribution,
                'top_countries': top_countries
            })
        
        return Response({
            'technology_stats': response_data,
            'count': len(response_data),
            'filters': {
                'country': country,
                'job_type': job_type,
                'processed_from': processed_from,
                'processed_to': processed_to,
                'limit': limit
            }
        })
        
    except Exception as e:
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@extend_schema(
    tags=['EuroTechJobs'],
    parameters=[
        OpenApiParameter('country', OpenApiTypes.STR, description='Filter by country'),
        OpenApiParameter('technology', OpenApiTypes.STR, description='Filter by technology'),
        OpenApiParameter('processed_from', OpenApiTypes.DATETIME, description='Processed after date'),
        OpenApiParameter('processed_to', OpenApiTypes.DATETIME, description='Processed before date'),
    ]
)
@api_view(['GET'])
def eurotechjobs_job_type_analysis(request):
    """Get analysis of job types and seniority levels"""
    try:
        country = request.GET.get('country')
        technology = request.GET.get('technology')
        processed_from = request.GET.get('processed_from')
        processed_to = request.GET.get('processed_to')
        
        # Build queryset
        queryset = EuroTechJobs.objects.all()
        
        if country:
            queryset = queryset.filter(country_code=country)
        if technology:
            queryset = queryset.filter(technologies__icontains=technology)
        if processed_from:
            queryset = queryset.filter(processed_at__gte=processed_from)
        if processed_to:
            queryset = queryset.filter(processed_at__lte=processed_to)
        
        # Aggregate by job type
        job_type_stats = queryset.values('job_type').annotate(
            job_count=Count('id'),
            avg_quality_score=Avg('data_quality_score'),
            avg_tech_count=Avg('tech_count'),
            country_count=Count('country_code', distinct=True)
        ).order_by('-job_count')
        
        response_data = []
        for job_stat in job_type_stats:
            job_type = job_stat['job_type']
            job_type_jobs = queryset.filter(job_type=job_type)
            
            # Top technologies
            top_techs = list(job_type_jobs.values('primary_technology').annotate(
                count=Count('id')
            ).order_by('-count')[:5].values_list('primary_technology', flat=True))
            
            # Country distribution
            country_dist = job_type_jobs.values('country_code').annotate(
                count=Count('id')
            ).order_by('-count')[:5]
            country_distribution = {item['country_code']: item['count'] for item in country_dist}
            
            response_data.append({
                'job_type': job_type,
                'job_count': job_stat['job_count'],
                'avg_quality_score': round(job_stat['avg_quality_score'] or 0, 2),
                'avg_tech_count': round(job_stat['avg_tech_count'] or 0, 2),
                'country_count': job_stat['country_count'],
                'top_technologies': top_techs,
                'country_distribution': country_distribution
            })
        
        return Response({
            'job_type_analysis': response_data,
            'filters': {
                'country': country,
                'technology': technology,
                'processed_from': processed_from,
                'processed_to': processed_to
            }
        })
        
    except Exception as e:
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@extend_schema(
    tags=['EuroTechJobs'],
    parameters=[
        OpenApiParameter('country', OpenApiTypes.STR, description='Filter by country'),
        OpenApiParameter('technology', OpenApiTypes.STR, description='Filter by technology'),
        OpenApiParameter('job_type', OpenApiTypes.STR, description='Filter by job type'),
        OpenApiParameter('group_by', OpenApiTypes.STR, description='Group by: day, month, or year (default: month)'),
        OpenApiParameter('processed_from', OpenApiTypes.DATETIME, description='Processed after date'),
        OpenApiParameter('processed_to', OpenApiTypes.DATETIME, description='Processed before date'),
    ]
)
@api_view(['GET'])
def eurotechjobs_time_series(request):
    """Get time series data for job posting trends"""
    try:
        country = request.GET.get('country')
        technology = request.GET.get('technology')
        job_type = request.GET.get('job_type')
        group_by = request.GET.get('group_by', 'month')  # day, month, year
        processed_from = request.GET.get('processed_from')
        processed_to = request.GET.get('processed_to')
        
        # Build queryset
        queryset = EuroTechJobs.objects.all()
        
        if country:
            queryset = queryset.filter(country_code=country)
        if technology:
            queryset = queryset.filter(technologies__icontains=technology)
        if job_type:
            queryset = queryset.filter(job_type=job_type)
        if processed_from:
            queryset = queryset.filter(processed_at__gte=processed_from)
        if processed_to:
            queryset = queryset.filter(processed_at__lte=processed_to)
        
        # Group by time period
        if group_by == 'year':
            time_series = queryset.extra(
                select={'period': "DATE_FORMAT(processed_at, '%%Y')"}
            ).values('period').annotate(
                job_count=Count('id'),
                avg_quality_score=Avg('data_quality_score'),
                technology_diversity=Count('primary_technology', distinct=True),
                country_diversity=Count('country_code', distinct=True)
            ).order_by('period')
        elif group_by == 'day':
            time_series = queryset.extra(
                select={'period': "DATE_FORMAT(processed_at, '%%Y-%%m-%%d')"}
            ).values('period').annotate(
                job_count=Count('id'),
                avg_quality_score=Avg('data_quality_score'),
                technology_diversity=Count('primary_technology', distinct=True),
                country_diversity=Count('country_code', distinct=True)
            ).order_by('period')
        else:  # month
            time_series = queryset.extra(
                select={'period': "DATE_FORMAT(processed_at, '%%Y-%%m')"}
            ).values('period').annotate(
                job_count=Count('id'),
                avg_quality_score=Avg('data_quality_score'),
                technology_diversity=Count('primary_technology', distinct=True),
                country_diversity=Count('country_code', distinct=True)
            ).order_by('period')
        
        # Format response
        response_data = [
            {
                'period': item['period'],
                'job_count': item['job_count'],
                'avg_quality_score': round(item['avg_quality_score'] or 0, 2),
                'technology_diversity': item['technology_diversity'],
                'country_diversity': item['country_diversity']
            }
            for item in time_series
        ]
        
        return Response({
            'time_series': response_data,
            'group_by': group_by,
            'filters': {
                'country': country,
                'technology': technology,
                'job_type': job_type,
                'processed_from': processed_from,
                'processed_to': processed_to
            },
            'count': len(response_data)
        })
        
    except Exception as e:
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@extend_schema(
    tags=['EuroTechJobs'],
    parameters=[
        OpenApiParameter('country', OpenApiTypes.STR, description='Filter by country'),
        OpenApiParameter('technology', OpenApiTypes.STR, description='Filter by technology'),
        OpenApiParameter('job_type', OpenApiTypes.STR, description='Filter by job type'),
        OpenApiParameter('processed_from', OpenApiTypes.DATETIME, description='Processed after date'),
        OpenApiParameter('processed_to', OpenApiTypes.DATETIME, description='Processed before date'),
        OpenApiParameter('limit', OpenApiTypes.INT, description='Number of results (default: 20)'),
        OpenApiParameter('sort_by', OpenApiTypes.STR, description='Sort by: job_count, quality, or tech_diversity (default: job_count)'),
    ]
)
@api_view(['GET'])
def eurotechjobs_company_analysis(request):
    """Get analysis of companies and their hiring patterns"""
    try:
        country = request.GET.get('country')
        technology = request.GET.get('technology')
        job_type = request.GET.get('job_type')
        processed_from = request.GET.get('processed_from')
        processed_to = request.GET.get('processed_to')
        limit = int(request.GET.get('limit', 20))
        sort_by = request.GET.get('sort_by', 'job_count')  # job_count, quality, tech_diversity
        
        # Build queryset
        queryset = EuroTechJobs.objects.all()
        
        if country:
            queryset = queryset.filter(country_code=country)
        if technology:
            queryset = queryset.filter(technologies__icontains=technology)
        if job_type:
            queryset = queryset.filter(job_type=job_type)
        if processed_from:
            queryset = queryset.filter(processed_at__gte=processed_from)
        if processed_to:
            queryset = queryset.filter(processed_at__lte=processed_to)
        
        # Aggregate by company
        company_stats = queryset.values('company').annotate(
            job_count=Count('id'),
            avg_quality_score=Avg('data_quality_score'),
            technology_count=Count('primary_technology', distinct=True),
            country_count=Count('country_code', distinct=True)
        )
        
        # Sort by specified field
        if sort_by == 'quality':
            company_stats = company_stats.order_by('-avg_quality_score')
        elif sort_by == 'tech_diversity':
            company_stats = company_stats.order_by('-technology_count')
        else:  # job_count
            company_stats = company_stats.order_by('-job_count')
        
        company_stats = company_stats[:limit]
        
        response_data = []
        for company_stat in company_stats:
            company = company_stat['company']
            company_jobs = queryset.filter(company=company)
            
            # Technologies used
            technologies = list(company_jobs.values('primary_technology').annotate(
                count=Count('id')
            ).order_by('-count').values_list('primary_technology', flat=True))
            
            # Countries
            countries = list(company_jobs.values('country_code').distinct().values_list('country_code', flat=True))
            
            # Job types
            job_types = list(company_jobs.values('job_type').annotate(
                count=Count('id')
            ).order_by('-count').values_list('job_type', flat=True))
            
            response_data.append({
                'company': company,
                'job_count': company_stat['job_count'],
                'avg_quality_score': round(company_stat['avg_quality_score'] or 0, 2),
                'technology_count': company_stat['technology_count'],
                'country_count': company_stat['country_count'],
                'technologies_used': technologies,
                'countries': countries,
                'job_types': job_types
            })
        
        return Response({
            'company_analysis': response_data,
            'sort_by': sort_by,
            'limit': limit,
            'filters': {
                'country': country,
                'technology': technology,
                'job_type': job_type,
                'processed_from': processed_from,
                'processed_to': processed_to
            }
        })
        
    except Exception as e:
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )