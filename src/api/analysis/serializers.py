"""
Serializers for Analysis API - Gold layer data warehouse endpoints.
"""

from rest_framework import serializers
from .models import AnalysisTechActivity, AnalysisJobDetails


class AnalysisTechActivityListSerializer(serializers.ModelSerializer):
    """Serializer for technology activity analysis (list view)"""
    country_name = serializers.CharField(source='id_country.country_name', read_only=True)
    technology_name = serializers.CharField(source='id_technology.technology_name', read_only=True)
    technology_type = serializers.CharField(source='id_technology.technology_type', read_only=True)
    source_name = serializers.CharField(source='id_source.source_name', read_only=True)

    class Meta:
        model = AnalysisTechActivity
        fields = [
            'id_activity', 'date_key', 'country_name', 'technology_name', 'technology_type',
            'source_name', 'job_count', 'github_stars', 'github_forks', 'search_volume',
            'popularity_score', 'data_quality_score'
        ]


class AnalysisTechActivityDetailSerializer(serializers.ModelSerializer):
    """Serializer for technology activity analysis (detail view)"""
    country_name = serializers.CharField(source='id_country.country_name', read_only=True)
    country_code = serializers.CharField(source='id_country.country_code', read_only=True)
    technology_name = serializers.CharField(source='id_technology.technology_name', read_only=True)
    technology_type = serializers.CharField(source='id_technology.technology_type', read_only=True)
    source_name = serializers.CharField(source='id_source.source_name', read_only=True)

    class Meta:
        model = AnalysisTechActivity
        fields = '__all__'


class AnalysisJobDetailsListSerializer(serializers.ModelSerializer):
    """Serializer for job details analysis (list view)"""
    country_name = serializers.CharField(source='id_country.country_name', read_only=True)
    company_name = serializers.CharField(source='id_company.company_name', read_only=True)
    role_title = serializers.CharField(source='id_job_role.role_title', read_only=True)
    role_type = serializers.CharField(source='id_job_role.role_type', read_only=True)
    source_name = serializers.CharField(source='id_source.source_name', read_only=True)

    class Meta:
        model = AnalysisJobDetails
        fields = [
            'id_job', 'date_key', 'country_name', 'company_name', 'role_title', 'role_type',
            'job_title', 'salary_usd', 'salary_range', 'job_type', 'seniority',
            'source_name', 'data_quality_score'
        ]


class AnalysisJobDetailsDetailSerializer(serializers.ModelSerializer):
    """Serializer for job details analysis (detail view)"""
    country_name = serializers.CharField(source='id_country.country_name', read_only=True)
    country_code = serializers.CharField(source='id_country.country_code', read_only=True)
    company_name = serializers.CharField(source='id_company.company_name', read_only=True)
    role_title = serializers.CharField(source='id_job_role.role_title', read_only=True)
    role_type = serializers.CharField(source='id_job_role.role_type', read_only=True)
    source_name = serializers.CharField(source='id_source.source_name', read_only=True)

    class Meta:
        model = AnalysisJobDetails
        fields = '__all__'