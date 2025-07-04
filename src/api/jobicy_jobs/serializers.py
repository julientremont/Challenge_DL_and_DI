from rest_framework import serializers
from .models import JobicyJobSilver


class JobicyJobSilverSerializer(serializers.ModelSerializer):
    """Serializer for Jobicy Jobs Silver data"""
    has_salary_data = serializers.ReadOnlyField()
    quality_level = serializers.ReadOnlyField()
    salary_range_text = serializers.ReadOnlyField()
    experience_level = serializers.ReadOnlyField()
    employment_type = serializers.ReadOnlyField()
    
    class Meta:
        model = JobicyJobSilver
        fields = [
            'id', 'job_id', 'job_title', 'company_name', 'company_logo', 
            'job_location', 'country_code', 'job_level', 'job_level_standardized',
            'job_type', 'job_type_standardized', 'publication_date', 
            'job_description', 'job_url', 'job_tags', 'job_industry',
            'salary_min', 'salary_max', 'salary_avg', 'salary_currency',
            'has_salary_info', 'tags_count', 'processed_date', 'data_quality_score',
            'has_salary_data', 'quality_level', 'salary_range_text', 
            'experience_level', 'employment_type'
        ]
        read_only_fields = ['id', 'processed_date']


class JobicyJobSilverListSerializer(serializers.ModelSerializer):
    """Simplified serializer for list views"""
    has_salary_data = serializers.ReadOnlyField()
    quality_level = serializers.ReadOnlyField()
    salary_range_text = serializers.ReadOnlyField()
    experience_level = serializers.ReadOnlyField()
    employment_type = serializers.ReadOnlyField()
    
    class Meta:
        model = JobicyJobSilver
        fields = [
            'id', 'job_id', 'job_title', 'company_name', 'job_location', 
            'country_code', 'job_level_standardized', 'job_type_standardized',
            'salary_range_text', 'has_salary_data', 'quality_level',
            'experience_level', 'employment_type', 'processed_date'
        ]


class JobicyJobSummarySerializer(serializers.Serializer):
    """Serializer for aggregated job market data"""
    total_records = serializers.IntegerField()
    unique_job_titles = serializers.IntegerField()
    unique_companies = serializers.IntegerField()
    countries_covered = serializers.IntegerField()
    jobs_with_salary = serializers.IntegerField()
    avg_salary = serializers.DecimalField(max_digits=12, decimal_places=2, allow_null=True)
    data_quality_avg = serializers.FloatField()
    job_level_distribution = serializers.DictField()
    job_type_distribution = serializers.DictField()
    top_countries = serializers.ListField(child=serializers.DictField())
    top_companies = serializers.ListField(child=serializers.DictField())


class JobMarketByCountrySerializer(serializers.Serializer):
    """Serializer for job market statistics by country"""
    country_code = serializers.CharField()
    total_jobs = serializers.IntegerField()
    unique_job_titles = serializers.IntegerField()
    unique_companies = serializers.IntegerField()
    avg_salary = serializers.DecimalField(max_digits=12, decimal_places=2, allow_null=True)
    most_common_job = serializers.CharField()
    most_common_company = serializers.CharField()
    job_level_distribution = serializers.DictField()
    data_quality_avg = serializers.FloatField()


class JobTitleAnalysisSerializer(serializers.Serializer):
    """Serializer for job title market analysis"""
    job_title = serializers.CharField()
    total_opportunities = serializers.IntegerField()
    countries_available = serializers.IntegerField()
    companies_hiring = serializers.IntegerField()
    avg_salary = serializers.DecimalField(max_digits=12, decimal_places=2, allow_null=True)
    salary_range = serializers.DictField()
    top_countries = serializers.ListField(child=serializers.CharField())
    top_companies = serializers.ListField(child=serializers.CharField())
    data_quality_avg = serializers.FloatField()


class CompanyAnalysisSerializer(serializers.Serializer):
    """Serializer for company hiring analysis"""
    company_name = serializers.CharField()
    total_jobs = serializers.IntegerField()
    unique_job_titles = serializers.IntegerField()
    countries_present = serializers.IntegerField()
    avg_salary = serializers.DecimalField(max_digits=12, decimal_places=2, allow_null=True)
    most_common_job = serializers.CharField()
    job_level_distribution = serializers.DictField()
    top_countries = serializers.ListField(child=serializers.CharField())
    data_quality_avg = serializers.FloatField()


class SalaryAnalysisSerializer(serializers.Serializer):
    """Serializer for salary trend analysis"""
    salary_range = serializers.CharField()
    job_count = serializers.IntegerField()
    countries = serializers.ListField(child=serializers.CharField())
    popular_job_titles = serializers.ListField(child=serializers.CharField())
    popular_companies = serializers.ListField(child=serializers.CharField())
    avg_numerical_salary = serializers.DecimalField(max_digits=12, decimal_places=2, allow_null=True)
    job_level_distribution = serializers.DictField()


class TimeSeriesJobDataSerializer(serializers.Serializer):
    """Serializer for time series job market data"""
    date = serializers.DateField()
    total_jobs = serializers.IntegerField()
    unique_job_titles = serializers.IntegerField()
    unique_companies = serializers.IntegerField()
    countries_active = serializers.IntegerField()
    avg_salary = serializers.DecimalField(max_digits=12, decimal_places=2, allow_null=True)
    jobs_with_salary = serializers.IntegerField()
    quality_score_avg = serializers.FloatField()