from rest_framework import generics, status
from rest_framework.decorators import api_view, permission_classes
from rest_framework.response import Response
from django_filters.rest_framework import DjangoFilterBackend
from django_filters import rest_framework as filters
from django.db.models import Avg, Count, Q
from drf_spectacular.utils import extend_schema, OpenApiParameter
from drf_spectacular.openapi import OpenApiTypes
from .models import AdzunaJob, AdzunaJobCategory
from .serializers import (
    AdzunaJobSerializer,
    AdzunaJobSummarySerializer,
    JobsByLocationSerializer,
    JobsByCompanySerializer,
    SalaryTrendsSerializer,
    AdzunaJobCategorySerializer
)
from ..permissions import AdminOnlyPermission, AuthenticatedReadOnlyPermission


class AdzunaJobFilter(filters.FilterSet):
    """Custom filter for Adzuna Jobs data"""
    title = filters.CharFilter(field_name='title', lookup_expr='icontains')
    company = filters.CharFilter(field_name='company', lookup_expr='icontains')
    location = filters.CharFilter(field_name='location', lookup_expr='icontains')
    country = filters.CharFilter(field_name='country', lookup_expr='exact')
    job_type = filters.CharFilter(field_name='job_type', lookup_expr='exact')
    experience_level = filters.CharFilter(field_name='experience_level', lookup_expr='exact')
    salary_min = filters.NumberFilter(field_name='salary_min', lookup_expr='gte')
    salary_max = filters.NumberFilter(field_name='salary_max', lookup_expr='lte')
    posted_from = filters.DateTimeFilter(field_name='posted_date', lookup_expr='gte')
    posted_to = filters.DateTimeFilter(field_name='posted_date', lookup_expr='lte')
    remote_allowed = filters.BooleanFilter(field_name='remote_allowed')
    
    class Meta:
        model = AdzunaJob
        fields = ['country', 'job_type', 'experience_level', 'remote_allowed']


@extend_schema(tags=['Adzuna Jobs'])
class AdzunaJobListView(generics.ListAPIView):
    """List Adzuna jobs with filtering"""
    queryset = AdzunaJob.objects.all()
    serializer_class = AdzunaJobSerializer
    filter_backends = [DjangoFilterBackend]
    filterset_class = AdzunaJobFilter
    ordering_fields = ['posted_date', 'salary_min', 'salary_max']
    ordering = ['-posted_date']
    permission_classes = [AuthenticatedReadOnlyPermission]


@extend_schema(tags=['Adzuna Jobs'])
class AdzunaJobDetailView(generics.RetrieveAPIView):
    """Retrieve a specific job posting"""
    queryset = AdzunaJob.objects.all()
    serializer_class = AdzunaJobSerializer
    permission_classes = [AuthenticatedReadOnlyPermission]


@extend_schema(tags=['Adzuna Jobs'])
@api_view(['GET'])
@permission_classes([AdminOnlyPermission])
def jobs_summary(request):
    """Get summary statistics for job data"""
    try:
        # TODO: Implement actual aggregation logic when schema is finalized
        summary_data = {
            'message': 'Adzuna Jobs summary endpoint - TODO: Implement when schema available',
            'total_jobs': 0,
            'total_companies': 0,
            'total_locations': 0,
            'date_range': {
                'earliest': None,
                'latest': None
            }
        }
        
        return Response(summary_data)
        
    except Exception as e:
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@extend_schema(tags=['Adzuna Jobs'])
@api_view(['GET'])
@permission_classes([AdminOnlyPermission])
def jobs_by_location(request):
    """Get job statistics by location"""
    try:
        # TODO: Implement actual location-based statistics
        return Response({
            'message': 'Jobs by location statistics - TODO: Implement when schema available',
            'locations': []
        })
        
    except Exception as e:
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@extend_schema(tags=['Adzuna Jobs'])
@api_view(['GET'])
@permission_classes([AdminOnlyPermission])
def jobs_by_company(request):
    """Get job statistics by company"""
    try:
        # TODO: Implement actual company-based statistics
        return Response({
            'message': 'Jobs by company statistics - TODO: Implement when schema available',
            'companies': []
        })
        
    except Exception as e:
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@extend_schema(tags=['Adzuna Jobs'])
@api_view(['GET'])
@permission_classes([AdminOnlyPermission])
def salary_trends(request):
    """Get salary trend analysis"""
    try:
        # TODO: Implement salary trend analysis
        return Response({
            'message': 'Salary trends analysis - TODO: Implement when schema available',
            'salary_by_experience': [],
            'salary_by_location': [],
            'salary_by_job_type': []
        })
        
    except Exception as e:
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@extend_schema(tags=['Adzuna Jobs'])
@api_view(['GET'])
@permission_classes([AdminOnlyPermission])
def skills_analysis(request):
    """Get skills demand analysis"""
    try:
        # TODO: Implement skills analysis
        return Response({
            'message': 'Skills analysis - TODO: Implement when schema available',
            'top_skills': [],
            'emerging_skills': [],
            'skills_by_salary': []
        })
        
    except Exception as e:
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )