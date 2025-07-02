from rest_framework import generics, status
from rest_framework.decorators import api_view
from rest_framework.response import Response
from django_filters.rest_framework import DjangoFilterBackend
from django_filters import rest_framework as filters
from django.db.models import Avg, Count, Q, F
from django.db.models.functions import Cast
from django.db.models.fields import FloatField
from drf_spectacular.utils import extend_schema, OpenApiParameter
from drf_spectacular.openapi import OpenApiTypes
from collections import Counter
import re

from .models import StackOverflowSurvey
from .serializers import (
    StackOverflowSurveySerializer, StackOverflowSurveyListSerializer,
    StackOverflowSurveySummarySerializer, DeveloperRoleStatsSerializer,
    TechnologyStatsSerializer, SalaryAnalysisSerializer, CountryStatsSerializer
)


class StackOverflowSurveyFilter(filters.FilterSet):
    """Custom filter for StackOverflow Survey model"""
    survey_year = filters.NumberFilter(field_name='survey_year', lookup_expr='exact')
    survey_year_min = filters.NumberFilter(field_name='survey_year', lookup_expr='gte')
    survey_year_max = filters.NumberFilter(field_name='survey_year', lookup_expr='lte')
    country = filters.CharFilter(field_name='country_normalized', lookup_expr='icontains')
    role = filters.CharFilter(field_name='primary_role', lookup_expr='icontains')
    education = filters.CharFilter(field_name='education_normalized', lookup_expr='icontains')
    salary_min = filters.NumberFilter(field_name='salary_usd_cleaned', lookup_expr='gte')
    salary_max = filters.NumberFilter(field_name='salary_usd_cleaned', lookup_expr='lte')
    salary_range = filters.CharFilter(field_name='salary_range', lookup_expr='exact')
    language = filters.CharFilter(field_name='primary_language', lookup_expr='icontains')
    technology = filters.CharFilter(field_name='technologies_used', lookup_expr='icontains')
    quality_min = filters.NumberFilter(field_name='data_quality_score', lookup_expr='gte')
    
    class Meta:
        model = StackOverflowSurvey
        fields = [
            'survey_year', 'survey_year_min', 'survey_year_max', 'country', 'role',
            'education', 'salary_min', 'salary_max', 'salary_range', 'language',
            'technology', 'quality_min'
        ]


@extend_schema(tags=['StackOverflow Survey'])
class StackOverflowSurveyListView(generics.ListAPIView):
    """List all StackOverflow survey responses with filtering and pagination"""
    queryset = StackOverflowSurvey.objects.all()
    serializer_class = StackOverflowSurveyListSerializer
    filter_backends = [DjangoFilterBackend]
    filterset_class = StackOverflowSurveyFilter
    ordering_fields = ['survey_year', 'salary_usd_cleaned', 'data_quality_score', 'processed_at']
    ordering = ['-survey_year', '-data_quality_score']


@extend_schema(tags=['StackOverflow Survey'])
class StackOverflowSurveyDetailView(generics.RetrieveAPIView):
    """Retrieve a specific StackOverflow survey response"""
    queryset = StackOverflowSurvey.objects.all()
    serializer_class = StackOverflowSurveySerializer


@extend_schema(
    tags=['StackOverflow Survey'],
    parameters=[
        OpenApiParameter('survey_year', OpenApiTypes.INT, description='Filter by survey year'),
        OpenApiParameter('country', OpenApiTypes.STR, description='Filter by country'),
        OpenApiParameter('role', OpenApiTypes.STR, description='Filter by developer role'),
        OpenApiParameter('salary_min', OpenApiTypes.NUMBER, description='Minimum salary'),
        OpenApiParameter('salary_max', OpenApiTypes.NUMBER, description='Maximum salary'),
    ]
)
@api_view(['GET'])
def stackoverflow_survey_summary(request):
    """Get comprehensive summary statistics for StackOverflow survey data"""
    try:
        # Get query parameters
        survey_year = request.GET.get('survey_year')
        country = request.GET.get('country')
        role = request.GET.get('role')
        salary_min = request.GET.get('salary_min')
        salary_max = request.GET.get('salary_max')
        
        # Build queryset
        queryset = StackOverflowSurvey.objects.all()
        
        if survey_year:
            queryset = queryset.filter(survey_year=survey_year)
        if country:
            queryset = queryset.filter(country_normalized__icontains=country)
        if role:
            queryset = queryset.filter(primary_role__icontains=role)
        if salary_min:
            queryset = queryset.filter(salary_usd_cleaned__gte=salary_min)
        if salary_max:
            queryset = queryset.filter(salary_usd_cleaned__lte=salary_max)
        
        # Basic statistics
        total_responses = queryset.count()
        survey_years = list(queryset.values_list('survey_year', flat=True).distinct().order_by('survey_year'))
        
        # Salary statistics
        salary_data = queryset.filter(salary_usd_cleaned__isnull=False)
        avg_salary = salary_data.aggregate(avg=Avg('salary_usd_cleaned'))['avg']
        median_salary = None
        if salary_data.exists():
            median_salary = salary_data.order_by('salary_usd_cleaned')[salary_data.count() // 2].salary_usd_cleaned
        
        # Unique counts
        unique_countries = queryset.values('country_normalized').distinct().count()
        unique_roles = queryset.values('primary_role').distinct().count()
        
        # Technology count (estimate from non-null technologies_used)
        tech_responses = queryset.filter(technologies_used__isnull=False)
        unique_technologies = 0
        if tech_responses.exists():
            all_techs = set()
            for response in tech_responses[:1000]:  # Sample for performance
                if response.technologies_used:
                    techs = [t.strip() for t in response.technologies_used.split(';') if t.strip()]
                    all_techs.update(techs)
            unique_technologies = len(all_techs)
        
        # Quality score
        avg_quality_score = queryset.aggregate(avg_quality=Avg('data_quality_score'))['avg_quality'] or 0
        
        summary_data = {
            'total_responses': total_responses,
            'survey_years': survey_years,
            'avg_salary': avg_salary,
            'median_salary': median_salary,
            'unique_countries': unique_countries,
            'unique_roles': unique_roles,
            'unique_technologies': unique_technologies,
            'avg_quality_score': round(avg_quality_score, 2)
        }
        
        serializer = StackOverflowSurveySummarySerializer(summary_data)
        return Response({
            'summary': serializer.data,
            'filters_applied': {
                'survey_year': survey_year,
                'country': country,
                'role': role,
                'salary_min': salary_min,
                'salary_max': salary_max
            }
        })
        
    except Exception as e:
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@extend_schema(
    tags=['StackOverflow Survey'],
    parameters=[
        OpenApiParameter('survey_year', OpenApiTypes.INT, description='Filter by survey year'),
        OpenApiParameter('country', OpenApiTypes.STR, description='Filter by country'),
        OpenApiParameter('salary_min', OpenApiTypes.NUMBER, description='Minimum salary'),
        OpenApiParameter('limit', OpenApiTypes.INT, description='Number of results (default: 20)'),
    ]
)
@api_view(['GET'])
def developer_role_stats(request):
    """Get statistics aggregated by developer role"""
    try:
        # Get query parameters
        survey_year = request.GET.get('survey_year')
        country = request.GET.get('country')
        salary_min = request.GET.get('salary_min')
        limit = int(request.GET.get('limit', 20))
        
        # Build queryset
        queryset = StackOverflowSurvey.objects.all()
        
        if survey_year:
            queryset = queryset.filter(survey_year=survey_year)
        if country:
            queryset = queryset.filter(country_normalized__icontains=country)
        if salary_min:
            queryset = queryset.filter(salary_usd_cleaned__gte=salary_min)
        
        # Aggregate by role
        role_stats = queryset.values('primary_role').annotate(
            count=Count('id'),
            avg_salary=Avg('salary_usd_cleaned'),
        ).filter(primary_role__isnull=False).order_by('-count')[:limit]
        
        # Enhance with additional data
        response_data = []
        for role_stat in role_stats:
            role_name = role_stat['primary_role']
            role_responses = queryset.filter(primary_role=role_name)
            
            # Calculate median salary
            salary_responses = role_responses.filter(salary_usd_cleaned__isnull=False)
            median_salary = None
            if salary_responses.exists():
                median_salary = salary_responses.order_by('salary_usd_cleaned')[salary_responses.count() // 2].salary_usd_cleaned
            
            # Get top technologies for this role
            top_technologies = []
            tech_responses = role_responses.filter(technologies_used__isnull=False)[:200]  # Sample for performance
            all_techs = []
            for response in tech_responses:
                if response.technologies_used:
                    techs = [t.strip() for t in response.technologies_used.split(';') if t.strip()]
                    all_techs.extend(techs)
            
            if all_techs:
                tech_counts = Counter(all_techs)
                top_technologies = [tech for tech, count in tech_counts.most_common(5)]
            
            # Get top countries for this role
            countries = list(role_responses.values_list('country_normalized', flat=True).distinct()[:5])
            
            response_data.append({
                'primary_role': role_name,
                'count': role_stat['count'],
                'avg_salary': round(role_stat['avg_salary'], 2) if role_stat['avg_salary'] else None,
                'median_salary': float(median_salary) if median_salary else None,
                'top_technologies': top_technologies,
                'countries': [c for c in countries if c]
            })
        
        return Response({
            'role_stats': response_data,
            'count': len(response_data),
            'filters': {
                'survey_year': survey_year,
                'country': country,
                'salary_min': salary_min,
                'limit': limit
            }
        })
        
    except Exception as e:
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@extend_schema(
    tags=['StackOverflow Survey'],
    parameters=[
        OpenApiParameter('survey_year', OpenApiTypes.INT, description='Filter by survey year'),
        OpenApiParameter('role', OpenApiTypes.STR, description='Filter by developer role'),
        OpenApiParameter('country', OpenApiTypes.STR, description='Filter by country'),
        OpenApiParameter('limit', OpenApiTypes.INT, description='Number of results (default: 30)'),
    ]
)
@api_view(['GET'])
def technology_stats(request):
    """Get statistics for technology usage and associated salaries"""
    try:
        # Get query parameters
        survey_year = request.GET.get('survey_year')
        role = request.GET.get('role')
        country = request.GET.get('country')
        limit = int(request.GET.get('limit', 30))
        
        # Build queryset
        queryset = StackOverflowSurvey.objects.filter(technologies_used__isnull=False)
        
        if survey_year:
            queryset = queryset.filter(survey_year=survey_year)
        if role:
            queryset = queryset.filter(primary_role__icontains=role)
        if country:
            queryset = queryset.filter(country_normalized__icontains=country)
        
        # Extract all technologies and count usage
        all_technologies = {}
        total_responses = queryset.count()
        
        # Sample for performance on large datasets
        sample_size = min(5000, total_responses)
        sample_responses = queryset[:sample_size]
        
        for response in sample_responses:
            if response.technologies_used:
                techs = [t.strip() for t in response.technologies_used.split(';') if t.strip()]
                for tech in techs:
                    if tech not in all_technologies:
                        all_technologies[tech] = {
                            'count': 0,
                            'salaries': [],
                            'roles': [],
                            'countries': []
                        }
                    all_technologies[tech]['count'] += 1
                    if response.salary_usd_cleaned:
                        all_technologies[tech]['salaries'].append(float(response.salary_usd_cleaned))
                    if response.primary_role:
                        all_technologies[tech]['roles'].append(response.primary_role)
                    if response.country_normalized:
                        all_technologies[tech]['countries'].append(response.country_normalized)
        
        # Calculate statistics and sort by usage
        tech_stats = []
        for tech, data in all_technologies.items():
            avg_salary = sum(data['salaries']) / len(data['salaries']) if data['salaries'] else None
            percentage = (data['count'] / sample_size) * 100
            
            # Get top roles and countries
            role_counts = Counter(data['roles'])
            country_counts = Counter(data['countries'])
            
            tech_stats.append({
                'technology': tech,
                'usage_count': data['count'],
                'percentage': round(percentage, 2),
                'avg_salary': round(avg_salary, 2) if avg_salary else None,
                'top_roles': [role for role, count in role_counts.most_common(3)],
                'top_countries': [country for country, count in country_counts.most_common(3)]
            })
        
        # Sort by usage count and limit
        tech_stats.sort(key=lambda x: x['usage_count'], reverse=True)
        tech_stats = tech_stats[:limit]
        
        return Response({
            'technology_stats': tech_stats,
            'total_technologies': len(all_technologies),
            'sample_size': sample_size,
            'total_responses': total_responses,
            'filters': {
                'survey_year': survey_year,
                'role': role,
                'country': country,
                'limit': limit
            }
        })
        
    except Exception as e:
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@extend_schema(
    tags=['StackOverflow Survey'],
    parameters=[
        OpenApiParameter('survey_year', OpenApiTypes.INT, description='Filter by survey year'),
        OpenApiParameter('role', OpenApiTypes.STR, description='Filter by developer role'),
        OpenApiParameter('country', OpenApiTypes.STR, description='Filter by country'),
    ]
)
@api_view(['GET'])
def salary_analysis(request):
    """Get detailed salary analysis by ranges and associated data"""
    try:
        # Get query parameters
        survey_year = request.GET.get('survey_year')
        role = request.GET.get('role')
        country = request.GET.get('country')
        
        # Build queryset
        queryset = StackOverflowSurvey.objects.filter(salary_range__isnull=False)
        
        if survey_year:
            queryset = queryset.filter(survey_year=survey_year)
        if role:
            queryset = queryset.filter(primary_role__icontains=role)
        if country:
            queryset = queryset.filter(country_normalized__icontains=country)
        
        # Analyze by salary range
        total_with_salary = queryset.count()
        salary_ranges = queryset.values('salary_range').annotate(
            count=Count('id')
        ).order_by('salary_range')
        
        response_data = []
        for range_stat in salary_ranges:
            range_name = range_stat['salary_range']
            range_responses = queryset.filter(salary_range=range_name)
            
            # Calculate percentage
            percentage = (range_stat['count'] / total_with_salary) * 100
            
            # Average technology count
            avg_tech_count = 0
            tech_responses = range_responses.filter(technologies_used__isnull=False)
            if tech_responses.exists():
                tech_counts = []
                for response in tech_responses[:500]:  # Sample for performance
                    if response.technologies_used:
                        count = len([t.strip() for t in response.technologies_used.split(';') if t.strip()])
                        tech_counts.append(count)
                avg_tech_count = sum(tech_counts) / len(tech_counts) if tech_counts else 0
            
            # Top technologies for this salary range
            top_technologies = []
            all_techs = []
            for response in tech_responses[:300]:  # Sample for performance
                if response.technologies_used:
                    techs = [t.strip() for t in response.technologies_used.split(';') if t.strip()]
                    all_techs.extend(techs)
            
            if all_techs:
                tech_counts = Counter(all_techs)
                top_technologies = [tech for tech, count in tech_counts.most_common(5)]
            
            # Top roles for this salary range
            top_roles = list(range_responses.values_list('primary_role', flat=True).distinct()[:5])
            
            response_data.append({
                'salary_range': range_name,
                'count': range_stat['count'],
                'percentage': round(percentage, 2),
                'avg_technologies_count': round(avg_tech_count, 2),
                'top_technologies': top_technologies,
                'top_roles': [role for role in top_roles if role]
            })
        
        return Response({
            'salary_analysis': response_data,
            'total_with_salary_data': total_with_salary,
            'filters': {
                'survey_year': survey_year,
                'role': role,
                'country': country
            }
        })
        
    except Exception as e:
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@extend_schema(
    tags=['StackOverflow Survey'],
    parameters=[
        OpenApiParameter('survey_year', OpenApiTypes.INT, description='Filter by survey year'),
        OpenApiParameter('role', OpenApiTypes.STR, description='Filter by developer role'),
        OpenApiParameter('limit', OpenApiTypes.INT, description='Number of results (default: 20)'),
    ]
)
@api_view(['GET'])
def country_stats(request):
    """Get statistics aggregated by country"""
    try:
        # Get query parameters
        survey_year = request.GET.get('survey_year')
        role = request.GET.get('role')
        limit = int(request.GET.get('limit', 20))
        
        # Build queryset
        queryset = StackOverflowSurvey.objects.filter(country_normalized__isnull=False)
        
        if survey_year:
            queryset = queryset.filter(survey_year=survey_year)
        if role:
            queryset = queryset.filter(primary_role__icontains=role)
        
        # Aggregate by country
        country_stats = queryset.values('country_normalized').annotate(
            count=Count('id'),
            avg_salary=Avg('salary_usd_cleaned')
        ).order_by('-count')[:limit]
        
        # Enhance with additional data
        response_data = []
        for country_stat in country_stats:
            country_name = country_stat['country_normalized']
            country_responses = queryset.filter(country_normalized=country_name)
            
            # Top roles
            top_roles = list(country_responses.values_list('primary_role', flat=True).distinct()[:5])
            
            # Top technologies
            top_technologies = []
            tech_responses = country_responses.filter(technologies_used__isnull=False)[:200]
            all_techs = []
            for response in tech_responses:
                if response.technologies_used:
                    techs = [t.strip() for t in response.technologies_used.split(';') if t.strip()]
                    all_techs.extend(techs)
            
            if all_techs:
                tech_counts = Counter(all_techs)
                top_technologies = [tech for tech, count in tech_counts.most_common(5)]
            
            # Top languages
            top_languages = list(country_responses.filter(primary_language__isnull=False)
                                .values_list('primary_language', flat=True).distinct()[:5])
            
            response_data.append({
                'country_normalized': country_name,
                'count': country_stat['count'],
                'avg_salary': round(country_stat['avg_salary'], 2) if country_stat['avg_salary'] else None,
                'top_roles': [role for role in top_roles if role],
                'top_technologies': top_technologies,
                'top_languages': [lang for lang in top_languages if lang]
            })
        
        return Response({
            'country_stats': response_data,
            'count': len(response_data),
            'filters': {
                'survey_year': survey_year,
                'role': role,
                'limit': limit
            }
        })
        
    except Exception as e:
        return Response(
            {'error': str(e)}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )