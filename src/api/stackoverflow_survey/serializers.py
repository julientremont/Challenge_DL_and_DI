from rest_framework import serializers
from .models import StackOverflowSurvey


class StackOverflowSurveySerializer(serializers.ModelSerializer):
    """Complete survey response serializer"""
    technologies_list = serializers.SerializerMethodField()
    
    class Meta:
        model = StackOverflowSurvey
        fields = '__all__'
        read_only_fields = ['id', 'processed_at', 'data_quality_score']
    
    def get_technologies_list(self, obj):
        """Convert semicolon-delimited technologies to list"""
        if obj.technologies_used:
            return [tech.strip() for tech in obj.technologies_used.split(';') if tech.strip()]
        return []


class StackOverflowSurveyListSerializer(serializers.ModelSerializer):
    """Lightweight serializer for list views"""
    technologies_count = serializers.SerializerMethodField()
    
    class Meta:
        model = StackOverflowSurvey
        fields = [
            'id', 'survey_year', 'country_normalized', 'primary_role', 
            'education_normalized', 'salary_usd_cleaned', 'salary_range',
            'primary_language', 'technologies_count', 'data_quality_score'
        ]
    
    def get_technologies_count(self, obj):
        """Count number of technologies"""
        if obj.technologies_used:
            return len([tech.strip() for tech in obj.technologies_used.split(';') if tech.strip()])
        return 0


class StackOverflowSurveySummarySerializer(serializers.Serializer):
    """Serializer for aggregated survey data"""
    total_responses = serializers.IntegerField()
    survey_years = serializers.ListField(child=serializers.IntegerField())
    avg_salary = serializers.DecimalField(max_digits=12, decimal_places=2, allow_null=True)
    median_salary = serializers.DecimalField(max_digits=12, decimal_places=2, allow_null=True)
    unique_countries = serializers.IntegerField()
    unique_roles = serializers.IntegerField()
    unique_technologies = serializers.IntegerField()
    avg_quality_score = serializers.FloatField()


class DeveloperRoleStatsSerializer(serializers.Serializer):
    """Serializer for developer role statistics"""
    primary_role = serializers.CharField()
    count = serializers.IntegerField()
    avg_salary = serializers.DecimalField(max_digits=12, decimal_places=2, allow_null=True)
    median_salary = serializers.DecimalField(max_digits=12, decimal_places=2, allow_null=True)
    top_technologies = serializers.ListField(child=serializers.CharField())
    countries = serializers.ListField(child=serializers.CharField())


class TechnologyStatsSerializer(serializers.Serializer):
    """Serializer for technology usage statistics"""
    technology = serializers.CharField()
    usage_count = serializers.IntegerField()
    percentage = serializers.FloatField()
    avg_salary = serializers.DecimalField(max_digits=12, decimal_places=2, allow_null=True)
    top_roles = serializers.ListField(child=serializers.CharField())
    top_countries = serializers.ListField(child=serializers.CharField())


class SalaryAnalysisSerializer(serializers.Serializer):
    """Serializer for salary analysis"""
    salary_range = serializers.CharField()
    count = serializers.IntegerField()
    percentage = serializers.FloatField()
    avg_technologies_count = serializers.FloatField()
    top_technologies = serializers.ListField(child=serializers.CharField())
    top_roles = serializers.ListField(child=serializers.CharField())


class CountryStatsSerializer(serializers.Serializer):
    """Serializer for country-based statistics"""
    country_normalized = serializers.CharField()
    count = serializers.IntegerField()
    avg_salary = serializers.DecimalField(max_digits=12, decimal_places=2, allow_null=True)
    top_roles = serializers.ListField(child=serializers.CharField())
    top_technologies = serializers.ListField(child=serializers.CharField())
    top_languages = serializers.ListField(child=serializers.CharField())