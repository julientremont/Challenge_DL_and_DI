from rest_framework import serializers
from .models import StackOverflowSurvey


class StackOverflowSurveySerializer(serializers.ModelSerializer):
    class Meta:
        model = StackOverflowSurvey
        fields = '__all__'
        read_only_fields = ['id', 'created_at', 'updated_at']


class StackOverflowSurveySummarySerializer(serializers.Serializer):
    """Serializer for aggregated survey data"""
    survey_year = serializers.IntegerField()
    total_respondents = serializers.IntegerField()
    avg_salary = serializers.DecimalField(max_digits=12, decimal_places=2)
    top_technologies = serializers.ListField(child=serializers.CharField())
    countries_count = serializers.IntegerField()


class DeveloperTypeStatsSerializer(serializers.Serializer):
    """Serializer for developer type statistics"""
    developer_type = serializers.CharField()
    count = serializers.IntegerField()
    avg_salary = serializers.DecimalField(max_digits=12, decimal_places=2)
    avg_experience = serializers.FloatField()


class TechnologyStatsSerializer(serializers.Serializer):
    """Serializer for technology usage statistics"""
    technology = serializers.CharField()
    usage_count = serializers.IntegerField()
    percentage = serializers.FloatField()
    avg_salary = serializers.DecimalField(max_digits=12, decimal_places=2)