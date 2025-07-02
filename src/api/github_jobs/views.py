from rest_framework import generics, status
from rest_framework.decorators import api_view
from rest_framework.response import Response
from django_filters.rest_framework import DjangoFilterBackend
from django_filters import rest_framework as filters
from django.db.models import Avg, Sum, Max, Min, Count, Q
from django.db.models.functions import TruncDate, TruncMonth, TruncYear
from drf_spectacular.utils import extend_schema, OpenApiParameter
from drf_spectacular.openapi import OpenApiTypes
from .models import GitHubRepos
from .serializers import (
    GitHubReposSerializer, GitHubReposListSerializer, GitHubTechnologyStatsSerializer,
    GitHubActivityStatsSerializer, GitHubTimeSeriesSerializer, GitHubPopularReposSerializer
)


class GitHubReposFilter(filters.FilterSet):
    """Custom filter for GitHubRepos model"""
    technology = filters.CharFilter(field_name='technology_normalized', lookup_expr='icontains')
    search_type = filters.CharFilter(field_name='search_type', lookup_expr='exact')
    activity_level = filters.CharFilter(field_name='activity_level', lookup_expr='exact')
    stars_min = filters.NumberFilter(field_name='stars_count', lookup_expr='gte')
    stars_max = filters.NumberFilter(field_name='stars_count', lookup_expr='lte')
    forks_min = filters.NumberFilter(field_name='forks_count', lookup_expr='gte')
    forks_max = filters.NumberFilter(field_name='forks_count', lookup_expr='lte')
    popularity_min = filters.NumberFilter(field_name='popularity_score', lookup_expr='gte')
    popularity_max = filters.NumberFilter(field_name='popularity_score', lookup_expr='lte')
    created_from = filters.DateTimeFilter(field_name='created_at_cleaned', lookup_expr='gte')
    created_to = filters.DateTimeFilter(field_name='created_at_cleaned', lookup_expr='lte')
    quality_min = filters.NumberFilter(field_name='data_quality_score', lookup_expr='gte')
    
    class Meta:
        model = GitHubRepos
        fields = [
            'technology', 'search_type', 'activity_level', 
            'stars_min', 'stars_max', 'forks_min', 'forks_max',
            'popularity_min', 'popularity_max', 'created_from', 'created_to',
            'quality_min'
        ]


@extend_schema(tags=['GitHub Repositories'])
class GitHubReposListView(generics.ListAPIView):
    """List all GitHub repositories with filtering and pagination"""
    queryset = GitHubRepos.objects.all()
    serializer_class = GitHubReposListSerializer
    filter_backends = [DjangoFilterBackend]
    filterset_class = GitHubReposFilter
    ordering_fields = ['created_at_cleaned', 'stars_count', 'popularity_score', 'activity_score']
    ordering = ['-popularity_score']


@extend_schema(tags=['GitHub Repositories'])
class GitHubReposDetailView(generics.RetrieveAPIView):
    """Retrieve a specific GitHub repository"""
    queryset = GitHubRepos.objects.all()
    serializer_class = GitHubReposSerializer


@extend_schema(
    tags=['GitHub Repositories'],
    parameters=[
        OpenApiParameter('technology', OpenApiTypes.STR, description='Filter by technology'),
        OpenApiParameter('activity_level', OpenApiTypes.STR, description='Filter by activity level (low/medium/high)'),
        OpenApiParameter('created_from', OpenApiTypes.DATETIME, description='Created after date'),
        OpenApiParameter('created_to', OpenApiTypes.DATETIME, description='Created before date'),
    ]
)
@api_view(['GET'])
def github_repos_summary(request):
    """Get summary statistics for GitHub repositories"""
    try:
        # Get query parameters
        technology = request.GET.get('technology')
        activity_level = request.GET.get('activity_level')
        created_from = request.GET.get('created_from')
        created_to = request.GET.get('created_to')
        
        # Build queryset
        queryset = GitHubRepos.objects.all()
        
        if technology:
            queryset = queryset.filter(technology_normalized__icontains=technology)
        if activity_level:
            queryset = queryset.filter(activity_level=activity_level)
        if created_from:
            queryset = queryset.filter(created_at_cleaned__gte=created_from)
        if created_to:
            queryset = queryset.filter(created_at_cleaned__lte=created_to)
        
        # Calculate summary statistics
        summary = queryset.aggregate(
            total_repositories=Count('id'),
            avg_stars=Avg('stars_count'),
            max_stars=Max('stars_count'),
            min_stars=Min('stars_count'),
            avg_forks=Avg('forks_count'),
            avg_popularity_score=Avg('popularity_score'),
            avg_quality_score=Avg('data_quality_score')
        )
        
        # Calculate unique technologies separately
        summary['unique_technologies'] = queryset.values('technology_normalized').distinct().count()
        
        # Activity level distribution
        activity_distribution = queryset.values('activity_level').annotate(
            count=Count('id')
        ).order_by('activity_level')
        
        return Response({
            'summary': summary,
            'activity_distribution': list(activity_distribution),
            'filters_applied': {
                'technology': technology,
                'activity_level': activity_level,
                'created_from': created_from,
                'created_to': created_to
            }
        })
        
    except Exception as e:
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@extend_schema(
    tags=['GitHub Repositories'],
    parameters=[
        OpenApiParameter('activity_level', OpenApiTypes.STR, description='Filter by activity level'),
        OpenApiParameter('created_from', OpenApiTypes.DATETIME, description='Created after date'),
        OpenApiParameter('created_to', OpenApiTypes.DATETIME, description='Created before date'),
        OpenApiParameter('limit', OpenApiTypes.INT, description='Number of results (default: 20)'),
    ]
)
@api_view(['GET'])
def github_technology_stats(request):
    """Get statistics aggregated by technology"""
    try:
        activity_level = request.GET.get('activity_level')
        created_from = request.GET.get('created_from')
        created_to = request.GET.get('created_to')
        limit = int(request.GET.get('limit', 20))
        
        # Build queryset
        queryset = GitHubRepos.objects.all()
        
        if activity_level:
            queryset = queryset.filter(activity_level=activity_level)
        if created_from:
            queryset = queryset.filter(created_at_cleaned__gte=created_from)
        if created_to:
            queryset = queryset.filter(created_at_cleaned__lte=created_to)
        
        # Aggregate by technology
        technology_stats = queryset.values('technology_normalized').annotate(
            repository_count=Count('id'),
            avg_stars=Avg('stars_count'),
            avg_forks=Avg('forks_count'),
            avg_popularity_score=Avg('popularity_score'),
            total_stars=Sum('stars_count'),
            max_stars=Max('stars_count')
        ).order_by('-repository_count')[:limit]
        
        # Add activity distribution for each technology
        response_data = []
        for tech_stat in technology_stats:
            tech_name = tech_stat['technology_normalized']
            activity_dist = queryset.filter(
                technology_normalized=tech_name
            ).values('activity_level').annotate(
                count=Count('id')
            )
            
            activity_distribution = {item['activity_level']: item['count'] for item in activity_dist}
            
            response_data.append({
                'technology_normalized': tech_name,
                'repository_count': tech_stat['repository_count'],
                'avg_stars': round(tech_stat['avg_stars'] or 0, 2),
                'avg_forks': round(tech_stat['avg_forks'] or 0, 2),
                'avg_popularity_score': round(tech_stat['avg_popularity_score'] or 0, 2),
                'total_stars': tech_stat['total_stars'] or 0,
                'max_stars': tech_stat['max_stars'] or 0,
                'activity_distribution': activity_distribution
            })
        
        return Response({
            'technology_stats': response_data,
            'count': len(response_data),
            'filters': {
                'activity_level': activity_level,
                'created_from': created_from,
                'created_to': created_to,
                'limit': limit
            }
        })
        
    except Exception as e:
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@extend_schema(
    tags=['GitHub Repositories'],
    parameters=[
        OpenApiParameter('technology', OpenApiTypes.STR, description='Filter by technology'),
        OpenApiParameter('created_from', OpenApiTypes.DATETIME, description='Created after date'),
        OpenApiParameter('created_to', OpenApiTypes.DATETIME, description='Created before date'),
    ]
)
@api_view(['GET'])
def github_activity_analysis(request):
    """Get analysis of repository activity levels"""
    try:
        technology = request.GET.get('technology')
        created_from = request.GET.get('created_from')
        created_to = request.GET.get('created_to')
        
        # Build queryset
        queryset = GitHubRepos.objects.all()
        
        if technology:
            queryset = queryset.filter(technology_normalized__icontains=technology)
        if created_from:
            queryset = queryset.filter(created_at_cleaned__gte=created_from)
        if created_to:
            queryset = queryset.filter(created_at_cleaned__lte=created_to)
        
        # Aggregate by activity level
        activity_stats = queryset.values('activity_level').annotate(
            repository_count=Count('id'),
            avg_stars=Avg('stars_count'),
            avg_forks=Avg('forks_count'),
            avg_popularity_score=Avg('popularity_score'),
            avg_days_since_creation=Avg('days_since_creation'),
            avg_quality_score=Avg('data_quality_score')
        ).order_by('activity_level')
        
        return Response({
            'activity_analysis': [
                {
                    'activity_level': item['activity_level'],
                    'repository_count': item['repository_count'],
                    'avg_stars': round(item['avg_stars'] or 0, 2),
                    'avg_forks': round(item['avg_forks'] or 0, 2),
                    'avg_popularity_score': round(item['avg_popularity_score'] or 0, 2),
                    'avg_days_since_creation': round(item['avg_days_since_creation'] or 0, 2),
                    'avg_quality_score': round(item['avg_quality_score'] or 0, 2)
                }
                for item in activity_stats
            ],
            'filters': {
                'technology': technology,
                'created_from': created_from,
                'created_to': created_to
            }
        })
        
    except Exception as e:
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@extend_schema(
    tags=['GitHub Repositories'],
    parameters=[
        OpenApiParameter('technology', OpenApiTypes.STR, description='Filter by technology'),
        OpenApiParameter('activity_level', OpenApiTypes.STR, description='Filter by activity level'),
        OpenApiParameter('group_by', OpenApiTypes.STR, description='Group by: month or year (default: month)'),
        OpenApiParameter('created_from', OpenApiTypes.DATETIME, description='Created after date'),
        OpenApiParameter('created_to', OpenApiTypes.DATETIME, description='Created before date'),
    ]
)
@api_view(['GET'])
def github_time_series(request):
    """Get time series data for repository creation trends"""
    try:
        technology = request.GET.get('technology')
        activity_level = request.GET.get('activity_level')
        group_by = request.GET.get('group_by', 'month')  # month, year
        created_from = request.GET.get('created_from')
        created_to = request.GET.get('created_to')
        
        # Build queryset
        queryset = GitHubRepos.objects.all()
        
        if technology:
            queryset = queryset.filter(technology_normalized__icontains=technology)
        if activity_level:
            queryset = queryset.filter(activity_level=activity_level)
        if created_from:
            queryset = queryset.filter(created_at_cleaned__gte=created_from)
        if created_to:
            queryset = queryset.filter(created_at_cleaned__lte=created_to)
        
        # Group by time period
        if group_by == 'year':
            time_series = queryset.extra(
                select={'period': "DATE_FORMAT(created_at_cleaned, '%%Y')"}
            ).values('period').annotate(
                repository_count=Count('id'),
                avg_stars=Avg('stars_count'),
                avg_popularity_score=Avg('popularity_score')
            ).order_by('period')
        else:  # month
            time_series = queryset.extra(
                select={'period': "DATE_FORMAT(created_at_cleaned, '%%Y-%%m')"}
            ).values('period').annotate(
                repository_count=Count('id'),
                avg_stars=Avg('stars_count'),
                avg_popularity_score=Avg('popularity_score')
            ).order_by('period')
        
        # Format response
        response_data = [
            {
                'period': item['period'],
                'repository_count': item['repository_count'],
                'avg_stars': round(item['avg_stars'] or 0, 2),
                'avg_popularity_score': round(item['avg_popularity_score'] or 0, 2),
                'technology': technology if technology else 'all'
            }
            for item in time_series
        ]
        
        return Response({
            'time_series': response_data,
            'group_by': group_by,
            'filters': {
                'technology': technology,
                'activity_level': activity_level,
                'created_from': created_from,
                'created_to': created_to
            },
            'count': len(response_data)
        })
        
    except Exception as e:
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@extend_schema(
    tags=['GitHub Repositories'],
    parameters=[
        OpenApiParameter('technology', OpenApiTypes.STR, description='Filter by technology'),
        OpenApiParameter('activity_level', OpenApiTypes.STR, description='Filter by activity level'),
        OpenApiParameter('created_from', OpenApiTypes.DATETIME, description='Created after date'),
        OpenApiParameter('created_to', OpenApiTypes.DATETIME, description='Created before date'),
        OpenApiParameter('limit', OpenApiTypes.INT, description='Number of results (default: 20)'),
        OpenApiParameter('sort_by', OpenApiTypes.STR, description='Sort by: stars, forks, or popularity (default: popularity)'),
    ]
)
@api_view(['GET'])
def github_popular_repos(request):
    """Get most popular repositories"""
    try:
        technology = request.GET.get('technology')
        activity_level = request.GET.get('activity_level')
        created_from = request.GET.get('created_from')
        created_to = request.GET.get('created_to')
        limit = int(request.GET.get('limit', 20))
        sort_by = request.GET.get('sort_by', 'popularity')  # stars, forks, popularity
        
        # Build queryset
        queryset = GitHubRepos.objects.all()
        
        if technology:
            queryset = queryset.filter(technology_normalized__icontains=technology)
        if activity_level:
            queryset = queryset.filter(activity_level=activity_level)
        if created_from:
            queryset = queryset.filter(created_at_cleaned__gte=created_from)
        if created_to:
            queryset = queryset.filter(created_at_cleaned__lte=created_to)
        
        # Sort by specified field
        if sort_by == 'stars':
            queryset = queryset.order_by('-stars_count')
        elif sort_by == 'forks':
            queryset = queryset.order_by('-forks_count')
        else:  # popularity
            queryset = queryset.order_by('-popularity_score')
        
        # Get top repositories
        popular_repos = queryset[:limit]
        
        response_data = [
            {
                'id': repo.id,
                'name_cleaned': repo.name_cleaned,
                'technology_normalized': repo.technology_normalized,
                'stars_count': repo.stars_count,
                'forks_count': repo.forks_count,
                'popularity_score': float(repo.popularity_score),
                'activity_level': repo.activity_level,
                'created_at_cleaned': repo.created_at_cleaned
            }
            for repo in popular_repos
        ]
        
        return Response({
            'popular_repositories': response_data,
            'sort_by': sort_by,
            'limit': limit,
            'filters': {
                'technology': technology,
                'activity_level': activity_level,
                'created_from': created_from,
                'created_to': created_to
            }
        })
        
    except Exception as e:
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )