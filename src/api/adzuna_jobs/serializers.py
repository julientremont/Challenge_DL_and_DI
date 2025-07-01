from rest_framework import serializers
from .models import AdzunaJob, AdzunaJobCategory


class AdzunaJobSerializer(serializers.ModelSerializer):
    class Meta:
        model = AdzunaJob
        fields = '__all__'
        read_only_fields = ['id', 'created_at', 'updated_at']


class AdzunaJobSummarySerializer(serializers.Serializer):
    """Serializer for aggregated job data"""
    total_jobs = serializers.IntegerField()
    countries_count = serializers.IntegerField()
    avg_salary_min = serializers.DecimalField(max_digits=12, decimal_places=2)
    avg_salary_max = serializers.DecimalField(max_digits=12, decimal_places=2)
    top_companies = serializers.ListField(child=serializers.CharField())
    top_locations = serializers.ListField(child=serializers.CharField())


class JobsByLocationSerializer(serializers.Serializer):
    """Serializer for jobs grouped by location"""
    location = serializers.CharField()
    country = serializers.CharField()
    job_count = serializers.IntegerField()
    avg_salary_min = serializers.DecimalField(max_digits=12, decimal_places=2)
    avg_salary_max = serializers.DecimalField(max_digits=12, decimal_places=2)


class JobsByCompanySerializer(serializers.Serializer):
    """Serializer for jobs grouped by company"""
    company = serializers.CharField()
    job_count = serializers.IntegerField()
    avg_salary_min = serializers.DecimalField(max_digits=12, decimal_places=2)
    locations = serializers.ListField(child=serializers.CharField())


class SalaryTrendsSerializer(serializers.Serializer):
    """Serializer for salary trend analysis"""
    job_type = serializers.CharField()
    experience_level = serializers.CharField()
    avg_salary_min = serializers.DecimalField(max_digits=12, decimal_places=2)
    avg_salary_max = serializers.DecimalField(max_digits=12, decimal_places=2)
    job_count = serializers.IntegerField()


class AdzunaJobCategorySerializer(serializers.ModelSerializer):
    class Meta:
        model = AdzunaJobCategory
        fields = '__all__'