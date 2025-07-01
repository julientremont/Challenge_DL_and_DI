from rest_framework import generics, status
from rest_framework.decorators import api_view
from rest_framework.response import Response
from django_filters.rest_framework import DjangoFilterBackend
from django_filters import rest_framework as filters
from django.db.models import Avg, Sum, Max, Min
from django.db.models.functions import TruncDate, TruncMonth
from drf_spectacular.utils import extend_schema, OpenApiParameter
from drf_spectacular.openapi import OpenApiTypes
from .models import TrendsFR
from .serializers import TrendsFRSerializer, TrendsAggregatedSerializer, TrendsTimeSeriesSerializer


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
class TrendsFRListView(generics.ListAPIView):
    """List all trends data with filtering and pagination"""
    queryset = TrendsFR.objects.all()
    serializer_class = TrendsFRSerializer
    filter_backends = [DjangoFilterBackend]
    filterset_class = TrendsFRFilter
    ordering_fields = ['date', 'search_frequency', 'keyword']
    ordering = ['-date']


@extend_schema(tags=['Google Trends'])
class TrendsFRDetailView(generics.RetrieveAPIView):
    """Retrieve a specific trend record"""
    queryset = TrendsFR.objects.all()
    serializer_class = TrendsFRSerializer


@extend_schema(
    tags=['Google Trends'],
    parameters=[
        OpenApiParameter('keyword', OpenApiTypes.STR, description='Filter by keyword'),
        OpenApiParameter('country_code', OpenApiTypes.STR, description='Country code (default: FR)'),
        OpenApiParameter('date_from', OpenApiTypes.DATE, description='Start date filter'),
        OpenApiParameter('date_to', OpenApiTypes.DATE, description='End date filter'),
    ]
)
@api_view(['GET'])
def trends_summary(request):
    """Get summary statistics for trends data"""
    try:
        # Get query parameters
        keyword = request.GET.get('keyword')
        country_code = request.GET.get('country_code', 'FR')
        date_from = request.GET.get('date_from')
        date_to = request.GET.get('date_to')
        
        # Build queryset
        queryset = TrendsFR.objects.filter(country_code=country_code)
        
        if keyword:
            queryset = queryset.filter(keyword__icontains=keyword)
        if date_from:
            queryset = queryset.filter(date__gte=date_from)
        if date_to:
            queryset = queryset.filter(date__lte=date_to)
        
        # Calculate summary statistics
        summary = queryset.aggregate(
            total_records=Sum('id'),
            avg_search_frequency=Avg('search_frequency'),
            max_search_frequency=Max('search_frequency'),
            min_search_frequency=Min('search_frequency'),
            unique_keywords=TrendsFR.objects.filter(
                country_code=country_code
            ).values('keyword').distinct().count()
        )
        
        return Response({
            'summary': summary,
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
        OpenApiParameter('country_code', OpenApiTypes.STR, description='Country code (default: FR)'),
        OpenApiParameter('date_from', OpenApiTypes.DATE, description='Start date filter'),
        OpenApiParameter('date_to', OpenApiTypes.DATE, description='End date filter'),
    ]
)
@api_view(['GET'])
def trends_aggregated(request):
    """Get aggregated trends data by keyword"""
    try:
        country_code = request.GET.get('country_code', 'FR')
        date_from = request.GET.get('date_from')
        date_to = request.GET.get('date_to')
        
        # Build queryset
        queryset = TrendsFR.objects.filter(country_code=country_code)
        
        if date_from:
            queryset = queryset.filter(date__gte=date_from)
        if date_to:
            queryset = queryset.filter(date__lte=date_to)
        
        # Aggregate by keyword
        aggregated_data = queryset.values('keyword').annotate(
            avg_search_frequency=Avg('search_frequency'),
            total_searches=Sum('search_frequency'),
            max_frequency=Max('search_frequency'),
            min_frequency=Min('search_frequency')
        ).order_by('-avg_search_frequency')
        
        # Format date range for response
        date_range = f"{date_from or 'start'} to {date_to or 'end'}"
        
        # Prepare response data
        response_data = []
        for item in aggregated_data:
            response_data.append({
                'keyword': item['keyword'],
                'avg_search_frequency': round(item['avg_search_frequency'], 2),
                'total_searches': item['total_searches'],
                'max_frequency': item['max_frequency'],
                'min_frequency': item['min_frequency'],
                'date_range': date_range
            })
        
        return Response({
            'aggregated_trends': response_data,
            'count': len(response_data),
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
    tags=['Google Trends'],
    parameters=[
        OpenApiParameter('keyword', OpenApiTypes.STR, description='Keyword (required)', required=True),
        OpenApiParameter('country_code', OpenApiTypes.STR, description='Country code (default: FR)'),
        OpenApiParameter('date_from', OpenApiTypes.DATE, description='Start date filter'),
        OpenApiParameter('date_to', OpenApiTypes.DATE, description='End date filter'),
        OpenApiParameter('group_by', OpenApiTypes.STR, description='Group by: day or month (default: day)'),
    ]
)
@api_view(['GET'])
def trends_time_series(request):
    """Get time series data for a specific keyword"""
    try:
        keyword = request.GET.get('keyword')
        country_code = request.GET.get('country_code', 'FR')
        date_from = request.GET.get('date_from')
        date_to = request.GET.get('date_to')
        group_by = request.GET.get('group_by', 'day')  # day, month
        
        if not keyword:
            return Response(
                {'error': 'keyword parameter is required'}, 
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Build queryset
        queryset = TrendsFR.objects.filter(
            keyword__iexact=keyword,
            country_code=country_code
        )
        
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
                total_searches=Sum('search_frequency')
            ).order_by('period')
        else:  # day
            time_series = queryset.values('date').annotate(
                search_frequency=Avg('search_frequency')
            ).order_by('date')
        
        # Format response
        if group_by == 'month':
            response_data = [
                {
                    'period': item['period'],
                    'avg_search_frequency': round(item['avg_search_frequency'], 2),
                    'total_searches': item['total_searches']
                }
                for item in time_series
            ]
        else:
            response_data = [
                {
                    'date': item['date'],
                    'search_frequency': round(item['search_frequency'], 2),
                    'keyword': keyword
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
        OpenApiParameter('country_code', OpenApiTypes.STR, description='Country code (default: FR)'),
        OpenApiParameter('date_from', OpenApiTypes.DATE, description='Start date filter'),
        OpenApiParameter('date_to', OpenApiTypes.DATE, description='End date filter'),
        OpenApiParameter('limit', OpenApiTypes.INT, description='Number of results (default: 10)'),
    ]
)
@api_view(['GET'])
def trends_top_keywords(request):
    """Get top keywords by search frequency"""
    try:
        country_code = request.GET.get('country_code', 'FR')
        date_from = request.GET.get('date_from')
        date_to = request.GET.get('date_to')
        limit = int(request.GET.get('limit', 10))
        
        # Build queryset
        queryset = TrendsFR.objects.filter(country_code=country_code)
        
        if date_from:
            queryset = queryset.filter(date__gte=date_from)
        if date_to:
            queryset = queryset.filter(date__lte=date_to)
        
        # Get top keywords
        top_keywords = queryset.values('keyword').annotate(
            avg_search_frequency=Avg('search_frequency'),
            total_searches=Sum('search_frequency'),
            max_frequency=Max('search_frequency')
        ).order_by('-avg_search_frequency')[:limit]
        
        return Response({
            'top_keywords': list(top_keywords),
            'limit': limit,
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