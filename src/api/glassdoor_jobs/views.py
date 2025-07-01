from rest_framework import generics, status
from rest_framework.decorators import api_view
from rest_framework.response import Response
from django_filters.rest_framework import DjangoFilterBackend
from django_filters import rest_framework as filters
from django.db.models import Avg, Count, Q
from .models import GlassdoorJob, GlassdoorCompanyReview
from .serializers import (
    GlassdoorJobSerializer,
    GlassdoorJobSummarySerializer,
    CompanyStatsSerializer,
    IndustryStatsSerializer,
    LocationStatsSerializer,
    SalaryRatingCorrelationSerializer,
    GlassdoorCompanyReviewSerializer
)


class GlassdoorJobFilter(filters.FilterSet):
    """Custom filter for Glassdoor Jobs data"""
    title = filters.CharFilter(field_name='title', lookup_expr='icontains')
    company = filters.CharFilter(field_name='company', lookup_expr='icontains')
    location = filters.CharFilter(field_name='location', lookup_expr='icontains')
    country = filters.CharFilter(field_name='country', lookup_expr='exact')
    industry = filters.CharFilter(field_name='industry', lookup_expr='icontains')
    sector = filters.CharFilter(field_name='sector', lookup_expr='icontains')
    company_size = filters.CharFilter(field_name='company_size', lookup_expr='exact')
    company_type = filters.CharFilter(field_name='company_type', lookup_expr='exact')
    rating_min = filters.NumberFilter(field_name='rating', lookup_expr='gte')
    rating_max = filters.NumberFilter(field_name='rating', lookup_expr='lte')
    salary_min = filters.NumberFilter(field_name='salary_min', lookup_expr='gte')
    salary_max = filters.NumberFilter(field_name='salary_max', lookup_expr='lte')
    founded_from = filters.NumberFilter(field_name='founded', lookup_expr='gte')
    founded_to = filters.NumberFilter(field_name='founded', lookup_expr='lte')
    posted_from = filters.DateTimeFilter(field_name='posted_date', lookup_expr='gte')
    posted_to = filters.DateTimeFilter(field_name='posted_date', lookup_expr='lte')
    easy_apply = filters.BooleanFilter(field_name='easy_apply')
    
    class Meta:
        model = GlassdoorJob
        fields = ['country', 'industry', 'company_size', 'company_type', 'easy_apply']


class GlassdoorJobListView(generics.ListAPIView):
    """List Glassdoor jobs with filtering"""
    queryset = GlassdoorJob.objects.all()
    serializer_class = GlassdoorJobSerializer
    filter_backends = [DjangoFilterBackend]
    filterset_class = GlassdoorJobFilter
    ordering_fields = ['posted_date', 'rating', 'salary_min', 'salary_max']
    ordering = ['-posted_date']


class GlassdoorJobDetailView(generics.RetrieveAPIView):
    """Retrieve a specific job posting"""
    queryset = GlassdoorJob.objects.all()
    serializer_class = GlassdoorJobSerializer


@api_view(['GET'])
def jobs_summary(request):
    """Get summary statistics for Glassdoor job data"""
    try:
        # TODO: Implement actual aggregation logic when schema is finalized
        summary_data = {
            'message': 'Glassdoor Jobs summary endpoint - TODO: Implement when schema available',
            'total_jobs': 0,
            'total_companies': 0,
            'avg_company_rating': 0.0,
            'salary_range': {
                'min': 0,
                'max': 0,
                'currency': 'USD'
            }
        }
        
        return Response(summary_data)
        
    except Exception as e:
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@api_view(['GET'])
def company_stats(request):
    """Get statistics by company"""
    try:
        # TODO: Implement actual company statistics
        return Response({
            'message': 'Company statistics - TODO: Implement when schema available',
            'companies': []
        })
        
    except Exception as e:
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@api_view(['GET'])
def industry_analysis(request):
    """Get industry-based analysis"""
    try:
        # TODO: Implement industry analysis
        return Response({
            'message': 'Industry analysis - TODO: Implement when schema available',
            'industries': []
        })
        
    except Exception as e:
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@api_view(['GET'])
def location_analysis(request):
    """Get location-based job analysis"""
    try:
        # TODO: Implement location analysis
        return Response({
            'message': 'Location analysis - TODO: Implement when schema available',
            'locations': []
        })
        
    except Exception as e:
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@api_view(['GET'])
def salary_rating_correlation(request):
    """Analyze correlation between company rating and salary"""
    try:
        # TODO: Implement salary vs rating analysis
        return Response({
            'message': 'Salary vs Rating correlation - TODO: Implement when schema available',
            'correlation_data': []
        })
        
    except Exception as e:
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@api_view(['GET'])
def company_insights(request):
    """Get detailed company insights including reviews"""
    try:
        # TODO: Implement company insights combining jobs and reviews
        return Response({
            'message': 'Company insights - TODO: Implement when schema available',
            'companies': []
        })
        
    except Exception as e:
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )