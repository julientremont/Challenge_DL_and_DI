from rest_framework import serializers
from .models import AdzunaJobSilver


class AdzunaJobSilverSerializer(serializers.ModelSerializer):
    """Serializer for Adzuna Jobs Silver data"""
    has_salary_data = serializers.ReadOnlyField()
    quality_level = serializers.ReadOnlyField()
    
    class Meta:
        model = AdzunaJobSilver
        fields = [
            'id', 'job_title', 'country_code', 'date', 'average_salary', 
            'processed_at', 'data_quality_score', 'has_salary_data', 'quality_level'
        ]
        read_only_fields = ['id', 'processed_at']


class AdzunaJobSummarySerializer(serializers.Serializer):
    """Serializer for aggregated job market data"""
    total_records = serializers.IntegerField()
    unique_job_titles = serializers.IntegerField()
    countries_covered = serializers.IntegerField()
    total_job_count = serializers.IntegerField()
    avg_salary = serializers.DecimalField(max_digits=12, decimal_places=2, allow_null=True)
    data_quality_avg = serializers.FloatField()
    date_range = serializers.DictField()


class JobMarketByCountrySerializer(serializers.Serializer):
    """Serializer for job market statistics by country"""
    country_code = serializers.CharField()
    total_jobs = serializers.IntegerField()
    unique_job_titles = serializers.IntegerField()
    avg_salary = serializers.DecimalField(max_digits=12, decimal_places=2, allow_null=True)
    most_common_job = serializers.CharField()
    data_quality_avg = serializers.FloatField()


class JobTitleAnalysisSerializer(serializers.Serializer):
    """Serializer for job title market analysis"""
    job_title = serializers.CharField()
    total_opportunities = serializers.IntegerField()
    countries_available = serializers.IntegerField()
    avg_salary = serializers.DecimalField(max_digits=12, decimal_places=2, allow_null=True)
    data_quality_avg = serializers.FloatField()


class SalaryAnalysisSerializer(serializers.Serializer):
    """Serializer for salary trend analysis"""
    job_count = serializers.IntegerField()
    countries = serializers.ListField(child=serializers.CharField())
    popular_job_titles = serializers.ListField(child=serializers.CharField())
    avg_numerical_salary = serializers.DecimalField(max_digits=12, decimal_places=2, allow_null=True)


class TimeSeriesJobDataSerializer(serializers.Serializer):
    """Serializer for time series job market data"""
    date = serializers.DateField()
    total_jobs = serializers.IntegerField()
    unique_job_titles = serializers.IntegerField()
    countries_active = serializers.IntegerField()
    avg_salary = serializers.DecimalField(max_digits=12, decimal_places=2, allow_null=True)