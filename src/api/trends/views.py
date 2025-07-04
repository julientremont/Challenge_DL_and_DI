from rest_framework import generics, status
from rest_framework.decorators import api_view, permission_classes
from rest_framework.response import Response
from django_filters.rest_framework import DjangoFilterBackend
from django_filters import rest_framework as filters
from django.db.models import Avg, Sum, Max, Min, Count, Q, F
from django.db.models.functions import TruncDate, TruncMonth
from drf_spectacular.utils import extend_schema, OpenApiParameter
from drf_spectacular.openapi import OpenApiTypes
from .models import TrendsSilver, TrendsFR
from .serializers import (
    TrendsSilverSerializer,
    TrendsSummarySerializer,
    KeywordAnalysisSerializer,
    CountryTrendsSerializer,
    TrendsTimeSeriesSerializer,
    TechTrendsAnalysisSerializer,
    TrendsFRSerializer
)
from ..permissions import AdminOnlyPermission, AuthenticatedReadOnlyPermission


class TrendsSilverFilter(filters.FilterSet):
    """Custom filter for TrendsSilver model"""
    date_from = filters.DateFilter(field_name='date', lookup_expr='gte')
    date_to = filters.DateFilter(field_name='date', lookup_expr='lte')
    keyword = filters.CharFilter(field_name='keyword', lookup_expr='icontains')
    country_code = filters.CharFilter(field_name='country_code', lookup_expr='exact')
    search_frequency_min = filters.NumberFilter(field_name='search_frequency', lookup_expr='gte')
    search_frequency_max = filters.NumberFilter(field_name='search_frequency', lookup_expr='lte')
    quality_score_min = filters.NumberFilter(field_name='data_quality_score', lookup_expr='gte')
    
    class Meta:
        model = TrendsSilver
        fields = ['country_code', 'keyword', 'date_from', 'date_to', 'search_frequency_min', 'search_frequency_max']


# Legacy filter for backward compatibility
class TrendsFRFilter(filters.FilterSet):
    """Custom filter for TrendsFR model"""
    date_from = filters.DateFilter(field_name='date', lookup_expr='gte')
    date_to = filters.DateFilter(field_name='date', lookup_expr='lte')
    keyword = filters.CharFilter(field_name='keyword', lookup_expr='icontains')
    country_code = filters.CharFilter(field_name='country_code', lookup_expr='exact')
    search_frequency_min = filters.NumberFilter(field_name='search_frequency', lookup_expr='gte')
    search_frequency_max = filters.NumberFilter(field_name='search_frequency', lookup_expr='lte')
    
    class Meta:
        model = TrendsFR
        fields = ['country_code', 'keyword', 'date_from', 'date_to', 'search_frequency_min', 'search_frequency_max']


@extend_schema(tags=['Google Trends'])
class TrendsSilverListView(generics.ListAPIView):
    """List all Google Trends silver data with filtering and pagination"""
    queryset = TrendsSilver.objects.all()
    serializer_class = TrendsSilverSerializer
    filter_backends = [DjangoFilterBackend]
    filterset_class = TrendsSilverFilter
    ordering_fields = ['date', 'search_frequency', 'keyword', 'data_quality_score']
    ordering = ['-date']
    permission_classes = [AuthenticatedReadOnlyPermission]


@extend_schema(tags=['Google Trends'])
class TrendsSilverDetailView(generics.RetrieveAPIView):
    """Retrieve a specific trend record from silver layer"""
    queryset = TrendsSilver.objects.all()
    serializer_class = TrendsSilverSerializer
    permission_classes = [AuthenticatedReadOnlyPermission]


# Legacy views for backward compatibility
@extend_schema(tags=['Google Trends - Legacy'])
class TrendsFRListView(generics.ListAPIView):
    """List all trends data with filtering and pagination (Legacy)"""
    queryset = TrendsFR.objects.all()
    serializer_class = TrendsFRSerializer
    filter_backends = [DjangoFilterBackend]
    filterset_class = TrendsFRFilter
    ordering_fields = ['date', 'search_frequency', 'keyword']
    ordering = ['-date']
    permission_classes = [AuthenticatedReadOnlyPermission]


@extend_schema(tags=['Google Trends - Legacy'])
class TrendsFRDetailView(generics.RetrieveAPIView):
    """Retrieve a specific trend record (Legacy)"""
    queryset = TrendsFR.objects.all()
    serializer_class = TrendsFRSerializer
    permission_classes = [AuthenticatedReadOnlyPermission]


@extend_schema(
    tags=['Google Trends'],
    parameters=[
        OpenApiParameter('keyword', OpenApiTypes.STR, description='Filter by keyword'),
        OpenApiParameter('country_code', OpenApiTypes.STR, description='Country code filter'),
        OpenApiParameter('date_from', OpenApiTypes.DATE, description='Start date filter'),
        OpenApiParameter('date_to', OpenApiTypes.DATE, description='End date filter'),
    ]
)
@api_view(['GET'])
@permission_classes([AuthenticatedReadOnlyPermission])
def trends_summary(request):
    """Get comprehensive summary statistics for Google Trends data"""
    try:
        # Get query parameters
        keyword = request.GET.get('keyword')
        country_code = request.GET.get('country_code')
        date_from = request.GET.get('date_from')
        date_to = request.GET.get('date_to')
        
        # Build queryset
        queryset = TrendsSilver.objects.all()
        
        if keyword:
            queryset = queryset.filter(keyword__icontains=keyword)
        if country_code:
            queryset = queryset.filter(country_code=country_code)
        if date_from:
            queryset = queryset.filter(date__gte=date_from)
        if date_to:
            queryset = queryset.filter(date__lte=date_to)
        
        # Calculate summary statistics
        summary = queryset.aggregate(
            total_records=Count('id'),
            unique_keywords=Count('keyword', distinct=True),
            countries_covered=Count('country_code', distinct=True),
            avg_search_frequency=Avg('search_frequency'),
            max_search_frequency=Max('search_frequency'),
            min_search_frequency=Min('search_frequency'),
            data_quality_avg=Avg('data_quality_score'),
            earliest_date=Min('date'),
            latest_date=Max('date')
        )
        
        # Format response
        summary_data = {
            'total_records': summary['total_records'] or 0,
            'unique_keywords': summary['unique_keywords'] or 0,
            'countries_covered': summary['countries_covered'] or 0,
            'avg_search_frequency': round(summary['avg_search_frequency'], 2) if summary['avg_search_frequency'] else 0,
            'max_search_frequency': summary['max_search_frequency'] or 0,
            'min_search_frequency': summary['min_search_frequency'] or 0,
            'data_quality_avg': round(summary['data_quality_avg'], 2) if summary['data_quality_avg'] else 0,
            'date_range': {
                'earliest': summary['earliest_date'],
                'latest': summary['latest_date']
            }
        }
        
        return Response({
            'summary': summary_data,
            'filters_applied': {
                'keyword': keyword,
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
    tags=['Google Trends'],
    parameters=[
        OpenApiParameter('country_code', OpenApiTypes.STR, description='Filter by country code'),
        OpenApiParameter('date_from', OpenApiTypes.DATE, description='Start date filter'),
        OpenApiParameter('date_to', OpenApiTypes.DATE, description='End date filter'),
        OpenApiParameter('limit', OpenApiTypes.INT, description='Number of results (default: 20)'),
    ]
)
@api_view(['GET'])
@permission_classes([AuthenticatedReadOnlyPermission])
def keyword_analysis(request):
    """Get detailed analysis of keyword performance and trends"""
    try:
        country_code = request.GET.get('country_code')
        date_from = request.GET.get('date_from')
        date_to = request.GET.get('date_to')
        limit = int(request.GET.get('limit', 20))
        
        # Build queryset
        queryset = TrendsSilver.objects.all()
        
        if country_code:
            queryset = queryset.filter(country_code=country_code)
        if date_from:
            queryset = queryset.filter(date__gte=date_from)
        if date_to:
            queryset = queryset.filter(date__lte=date_to)
        
        # Aggregate by keyword
        keyword_data = queryset.values('keyword').annotate(
            avg_search_frequency=Avg('search_frequency'),
            total_data_points=Count('id'),
            countries_tracked=Count('country_code', distinct=True),
            peak_frequency=Max('search_frequency'),
            data_quality_avg=Avg('data_quality_score')
        ).order_by('-avg_search_frequency')[:limit]
        
        # Add trend analysis for each keyword
        result = []
        for keyword in keyword_data:
            # Get peak date and trend direction
            keyword_queryset = queryset.filter(keyword=keyword['keyword'])
            
            peak_record = keyword_queryset.filter(
                search_frequency=keyword['peak_frequency']
            ).first()
            peak_date = peak_record.date if peak_record else None
            
            # Simple trend direction calculation
            recent_avg = keyword_queryset.filter(
                date__gte=peak_date
            ).aggregate(Avg('search_frequency'))['search_frequency__avg'] if peak_date else 0
            
            older_avg = keyword_queryset.filter(
                date__lt=peak_date
            ).aggregate(Avg('search_frequency'))['search_frequency__avg'] if peak_date else 0
            
            if recent_avg and older_avg:
                if recent_avg > older_avg * 1.1:
                    trend_direction = 'growing'
                elif recent_avg < older_avg * 0.9:
                    trend_direction = 'declining'
                else:
                    trend_direction = 'stable'
            else:
                trend_direction = 'unknown'
            
            result.append({
                'keyword': keyword['keyword'],
                'avg_search_frequency': round(keyword['avg_search_frequency'], 2),
                'total_data_points': keyword['total_data_points'],
                'countries_tracked': keyword['countries_tracked'],
                'peak_frequency': keyword['peak_frequency'],
                'peak_date': peak_date,
                'trend_direction': trend_direction,
                'data_quality_avg': round(keyword['data_quality_avg'], 2)
            })
        
        return Response({
            'keyword_analysis': result,
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
    tags=['Google Trends'],
    parameters=[
        OpenApiParameter('date_from', OpenApiTypes.DATE, description='Start date filter'),
        OpenApiParameter('date_to', OpenApiTypes.DATE, description='End date filter'),
        OpenApiParameter('limit', OpenApiTypes.INT, description='Number of results (default: 10)'),
    ]
)
@api_view(['GET'])
@permission_classes([AuthenticatedReadOnlyPermission])
def country_trends_analysis(request):
    """Get trends analysis by country"""
    try:
        date_from = request.GET.get('date_from')
        date_to = request.GET.get('date_to')
        limit = int(request.GET.get('limit', 10))
        
        # Build queryset
        queryset = TrendsSilver.objects.all()
        
        if date_from:
            queryset = queryset.filter(date__gte=date_from)
        if date_to:
            queryset = queryset.filter(date__lte=date_to)
        
        # Aggregate by country
        country_data = queryset.values('country_code').annotate(
            unique_keywords=Count('keyword', distinct=True),
            avg_search_frequency=Avg('search_frequency'),
            data_quality_avg=Avg('data_quality_score')
        ).order_by('-avg_search_frequency')[:limit]
        
        # Add most searched keyword and trending keywords for each country
        result = []
        for country in country_data:
            country_queryset = queryset.filter(country_code=country['country_code'])
            
            # Most searched keyword
            most_searched = country_queryset.values('keyword').annotate(
                avg_freq=Avg('search_frequency')
            ).order_by('-avg_freq').first()
            
            # Top trending keywords
            trending_keywords = list(
                country_queryset.values('keyword').annotate(
                    avg_freq=Avg('search_frequency')
                ).order_by('-avg_freq')[:5].values_list('keyword', flat=True)
            )
            
            result.append({
                'country_code': country['country_code'],
                'unique_keywords': country['unique_keywords'],
                'avg_search_frequency': round(country['avg_search_frequency'], 2),
                'most_searched_keyword': most_searched['keyword'] if most_searched else 'N/A',
                'trending_keywords': trending_keywords,
                'data_quality_avg': round(country['data_quality_avg'], 2)
            })
        
        return Response({
            'country_trends_analysis': result,
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
    tags=['Google Trends'],
    parameters=[
        OpenApiParameter('keyword', OpenApiTypes.STR, description='Keyword (required)', required=True),
        OpenApiParameter('country_code', OpenApiTypes.STR, description='Filter by country code'),
        OpenApiParameter('date_from', OpenApiTypes.DATE, description='Start date filter'),
        OpenApiParameter('date_to', OpenApiTypes.DATE, description='End date filter'),
        OpenApiParameter('group_by', OpenApiTypes.STR, description='Group by: day or month (default: day)'),
    ]
)
@api_view(['GET'])
@permission_classes([AuthenticatedReadOnlyPermission])
def trends_time_series(request):
    """Get time series data for a specific keyword"""
    try:
        keyword = request.GET.get('keyword')
        country_code = request.GET.get('country_code')
        date_from = request.GET.get('date_from')
        date_to = request.GET.get('date_to')
        group_by = request.GET.get('group_by', 'day')  # day, month
        
        if not keyword:
            return Response(
                {'error': 'keyword parameter is required'}, 
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Build queryset
        queryset = TrendsSilver.objects.filter(keyword__iexact=keyword)
        
        if country_code:
            queryset = queryset.filter(country_code=country_code)
        if date_from:
            queryset = queryset.filter(date__gte=date_from)
        if date_to:
            queryset = queryset.filter(date__lte=date_to)
        
        # Group by time period
        if group_by == 'month':
            time_series = queryset.extra(
                select={'period': "DATE_FORMAT(date, '%%Y-%%m')"}
            ).values('period').annotate(
                avg_search_frequency=Avg('search_frequency'),
                countries_tracked=Count('country_code', distinct=True),
                data_quality_avg=Avg('data_quality_score')
            ).order_by('period')
        else:  # day
            time_series = queryset.values('date', 'country_code').annotate(
                search_frequency=Avg('search_frequency')
            ).order_by('date')
        
        # Format response
        if group_by == 'month':
            response_data = [
                {
                    'period': item['period'],
                    'avg_search_frequency': round(item['avg_search_frequency'], 2),
                    'countries_tracked': item['countries_tracked'],
                    'data_quality_avg': round(item['data_quality_avg'], 2)
                }
                for item in time_series
            ]
        else:
            response_data = [
                {
                    'date': item['date'],
                    'search_frequency': round(item['search_frequency'], 2),
                    'keyword': keyword,
                    'country_code': item['country_code']
                }
                for item in time_series
            ]
        
        return Response({
            'time_series': response_data,
            'keyword': keyword,
            'country_code': country_code,
            'group_by': group_by,
            'count': len(response_data)
        })
        
    except Exception as e:
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@extend_schema(
    tags=['Google Trends'],
    parameters=[
        OpenApiParameter('country_code', OpenApiTypes.STR, description='Filter by country code'),
        OpenApiParameter('date_from', OpenApiTypes.DATE, description='Start date filter'),
        OpenApiParameter('date_to', OpenApiTypes.DATE, description='End date filter'),
        OpenApiParameter('limit', OpenApiTypes.INT, description='Number of results (default: 15)'),
    ]
)
@api_view(['GET'])
@permission_classes([AuthenticatedReadOnlyPermission])
def tech_trends_analysis(request):
    """Get technology trends analysis with categorization"""
    try:
        country_code = request.GET.get('country_code')
        date_from = request.GET.get('date_from')
        date_to = request.GET.get('date_to')
        limit = int(request.GET.get('limit', 15))
        
        # Define technology categories
        tech_categories = {
            'programming_languages': ['python', 'javascript', 'java', 'typescript', 'c++', 'c#', 'go', 'rust', 'kotlin', 'swift'],
            'frameworks': ['react', 'angular', 'vue', 'django', 'spring', 'express', 'laravel'],
            'databases': ['mysql', 'postgresql', 'mongodb', 'redis', 'elasticsearch'],
            'cloud_platforms': ['aws', 'azure', 'gcp', 'docker', 'kubernetes'],
            'tools': ['git', 'jenkins', 'terraform', 'ansible']
        }
        
        # Build queryset
        queryset = TrendsSilver.objects.all()
        
        if country_code:
            queryset = queryset.filter(country_code=country_code)
        if date_from:
            queryset = queryset.filter(date__gte=date_from)
        if date_to:
            queryset = queryset.filter(date__lte=date_to)
        
        # Filter for tech keywords
        all_tech_keywords = [keyword for category_keywords in tech_categories.values() for keyword in category_keywords]
        tech_queryset = queryset.filter(keyword__in=all_tech_keywords)
        
        # Analyze each tech keyword
        tech_data = tech_queryset.values('keyword').annotate(
            avg_popularity=Avg('search_frequency'),
            countries_interested=Count('country_code', distinct=True),
            data_points=Count('id')
        ).order_by('-avg_popularity')[:limit]
        
        # Add category and growth analysis
        result = []
        for tech in tech_data:
            keyword = tech['keyword']
            
            # Determine category
            category = 'other'
            for cat, keywords in tech_categories.items():
                if keyword in keywords:
                    category = cat
                    break
            
            # Calculate growth rate (simplified)
            keyword_queryset = tech_queryset.filter(keyword=keyword)
            
            # Get recent vs older data for growth calculation
            recent_data = keyword_queryset.filter(
                date__gte=date_from if date_from else '2023-01-01'
            ).aggregate(Avg('search_frequency'))['search_frequency__avg'] or 0
            
            older_data = keyword_queryset.filter(
                date__lt=date_from if date_from else '2023-01-01'
            ).aggregate(Avg('search_frequency'))['search_frequency__avg'] or 1
            
            growth_rate = ((recent_data - older_data) / older_data * 100) if older_data > 0 else 0
            
            # Get countries interested
            countries = list(
                keyword_queryset.values_list('country_code', flat=True).distinct()
            )
            
            # Get peak periods (simplified)
            peak_periods = list(
                keyword_queryset.filter(
                    search_frequency__gte=tech['avg_popularity'] * 1.5
                ).values_list('date', flat=True).distinct()[:3]
            )
            
            result.append({
                'keyword': keyword,
                'category': category,
                'avg_popularity': round(tech['avg_popularity'], 2),
                'growth_rate': round(growth_rate, 2),
                'countries_interested': countries,
                'peak_periods': peak_periods
            })
        
        return Response({
            'tech_trends_analysis': result,
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