from rest_framework import generics, status
from rest_framework.decorators import api_view
from rest_framework.response import Response
from django_filters.rest_framework import DjangoFilterBackend
from django_filters import rest_framework as filters
from django.db.models import Avg, Count, Q
from drf_spectacular.utils import extend_schema, OpenApiParameter
from drf_spectacular.openapi import OpenApiTypes
from .models import StackOverflowSurvey
from .serializers import (
    StackOverflowSurveySerializer, 
    StackOverflowSurveySummarySerializer,
    DeveloperTypeStatsSerializer,
    TechnologyStatsSerializer
)


class StackOverflowSurveyFilter(filters.FilterSet):
    """Custom filter for StackOverflow Survey data"""
    survey_year = filters.NumberFilter(field_name='survey_year')
    country = filters.CharFilter(field_name='country', lookup_expr='icontains')
    developer_type = filters.CharFilter(field_name='developer_type', lookup_expr='icontains')
    experience_min = filters.NumberFilter(field_name='experience_years', lookup_expr='gte')
    experience_max = filters.NumberFilter(field_name='experience_years', lookup_expr='lte')
    salary_min = filters.NumberFilter(field_name='salary', lookup_expr='gte')
    salary_max = filters.NumberFilter(field_name='salary', lookup_expr='lte')
    
    class Meta:
        model = StackOverflowSurvey
        fields = ['survey_year', 'country', 'developer_type', 'gender', 'industry']


@extend_schema(tags=['StackOverflow Survey'])
class StackOverflowSurveyListView(generics.ListAPIView):
    """List StackOverflow survey responses with filtering"""
    queryset = StackOverflowSurvey.objects.all()
    serializer_class = StackOverflowSurveySerializer
    filter_backends = [DjangoFilterBackend]
    filterset_class = StackOverflowSurveyFilter
    ordering_fields = ['survey_year', 'salary', 'experience_years']
    ordering = ['-survey_year']


@extend_schema(tags=['StackOverflow Survey'])
class StackOverflowSurveyDetailView(generics.RetrieveAPIView):
    """Retrieve a specific survey response"""
    queryset = StackOverflowSurvey.objects.all()
    serializer_class = StackOverflowSurveySerializer


@extend_schema(tags=['StackOverflow Survey'])
@api_view(['GET'])
def survey_summary(request):
    """Get summary statistics for survey data"""
    try:
        # TODO: Implement actual aggregation logic when schema is finalized
        summary_data = {
            'message': 'StackOverflow Survey summary endpoint - TODO: Implement when schema available',
            'total_years': 0,
            'total_respondents': 0,
            'latest_year': None,
            'available_years': []
        }
        
        return Response(summary_data)
        
    except Exception as e:
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@extend_schema(tags=['StackOverflow Survey'])
@api_view(['GET'])
def developer_type_stats(request):
    """Get statistics by developer type"""
    try:
        # TODO: Implement actual developer type statistics
        return Response({
            'message': 'Developer type statistics - TODO: Implement when schema available',
            'developer_types': []
        })
        
    except Exception as e:
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@extend_schema(tags=['StackOverflow Survey'])
@api_view(['GET'])
def technology_stats(request):
    """Get technology usage statistics"""
    try:
        # TODO: Implement actual technology statistics
        return Response({
            'message': 'Technology usage statistics - TODO: Implement when schema available',
            'technologies': []
        })
        
    except Exception as e:
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@extend_schema(tags=['StackOverflow Survey'])
@api_view(['GET'])
def salary_analysis(request):
    """Get salary analysis by various factors"""
    try:
        # TODO: Implement salary analysis
        return Response({
            'message': 'Salary analysis - TODO: Implement when schema available',
            'salary_by_experience': [],
            'salary_by_country': [],
            'salary_by_technology': []
        })
        
    except Exception as e:
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )