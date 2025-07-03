from rest_framework import serializers
from .models import EuroTechJobs


class EuroTechJobsSerializer(serializers.ModelSerializer):
    class Meta:
        model = EuroTechJobs
        fields = [
            'id', 'job_title', 'job_title_category', 'company', 'location', 'country_code',
            'technologies', 'primary_technology', 'tech_count', 'job_type', 'category',
            'url', 'processed_at', 'data_quality_score'
        ]
        read_only_fields = ['id']


class EuroTechJobsListSerializer(serializers.ModelSerializer):
    """Simplified serializer for list views"""
    class Meta:
        model = EuroTechJobs
        fields = [
            'id', 'job_title', 'job_title_category', 'company', 'country_code',
            'primary_technology', 'job_type', 'data_quality_score', 'processed_at'
        ]
        read_only_fields = ['id']


class EuroTechCountryStatsSerializer(serializers.Serializer):
    country_code = serializers.CharField()
    job_count = serializers.IntegerField()
    avg_quality_score = serializers.FloatField()
    top_technology = serializers.CharField()
    top_job_type = serializers.CharField()
    company_count = serializers.IntegerField()
    technology_distribution = serializers.DictField()


class EuroTechTechnologyStatsSerializer(serializers.Serializer):
    primary_technology = serializers.CharField()
    job_count = serializers.IntegerField()
    avg_quality_score = serializers.FloatField()
    country_count = serializers.IntegerField()
    avg_tech_count = serializers.FloatField()
    job_type_distribution = serializers.DictField()
    top_countries = serializers.ListField()


class EuroTechJobTypeStatsSerializer(serializers.Serializer):
    job_type = serializers.CharField()
    job_count = serializers.IntegerField()
    avg_quality_score = serializers.FloatField()
    avg_tech_count = serializers.FloatField()
    top_technologies = serializers.ListField()
    country_distribution = serializers.DictField()


class EuroTechCompanyStatsSerializer(serializers.Serializer):
    company = serializers.CharField()
    job_count = serializers.IntegerField()
    technologies_used = serializers.ListField()
    countries = serializers.ListField()
    avg_quality_score = serializers.FloatField()
    job_types = serializers.ListField()


class EuroTechTimeSeriesSerializer(serializers.Serializer):
    period = serializers.CharField()
    job_count = serializers.IntegerField()
    avg_quality_score = serializers.FloatField()
    technology_diversity = serializers.FloatField()
    country_diversity = serializers.FloatField()


class EuroTechQualityStatsSerializer(serializers.Serializer):
    quality_range = serializers.CharField()
    job_count = serializers.IntegerField()
    percentage = serializers.FloatField()
    avg_tech_count = serializers.FloatField()
    top_technologies = serializers.ListField()
    top_countries = serializers.ListField()