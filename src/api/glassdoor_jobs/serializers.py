from rest_framework import serializers
from .models import GlassdoorJob, GlassdoorCompanyReview


class GlassdoorJobSerializer(serializers.ModelSerializer):
    class Meta:
        model = GlassdoorJob
        fields = '__all__'
        read_only_fields = ['id', 'created_at', 'updated_at']


class GlassdoorJobSummarySerializer(serializers.Serializer):
    """Serializer for aggregated job data"""
    total_jobs = serializers.IntegerField()
    total_companies = serializers.IntegerField()
    avg_rating = serializers.FloatField()
    avg_salary_min = serializers.DecimalField(max_digits=12, decimal_places=2)
    avg_salary_max = serializers.DecimalField(max_digits=12, decimal_places=2)
    top_industries = serializers.ListField(child=serializers.CharField())
    top_locations = serializers.ListField(child=serializers.CharField())


class CompanyStatsSerializer(serializers.Serializer):
    """Serializer for company statistics"""
    company = serializers.CharField()
    job_count = serializers.IntegerField()
    avg_rating = serializers.FloatField()
    avg_salary_min = serializers.DecimalField(max_digits=12, decimal_places=2)
    industry = serializers.CharField()
    company_size = serializers.CharField()
    headquarters = serializers.CharField()


class IndustryStatsSerializer(serializers.Serializer):
    """Serializer for industry statistics"""
    industry = serializers.CharField()
    job_count = serializers.IntegerField()
    avg_rating = serializers.FloatField()
    avg_salary_min = serializers.DecimalField(max_digits=12, decimal_places=2)
    top_companies = serializers.ListField(child=serializers.CharField())


class LocationStatsSerializer(serializers.Serializer):
    """Serializer for location-based statistics"""
    location = serializers.CharField()
    country = serializers.CharField()
    job_count = serializers.IntegerField()
    avg_rating = serializers.FloatField()
    avg_salary_min = serializers.DecimalField(max_digits=12, decimal_places=2)
    top_industries = serializers.ListField(child=serializers.CharField())


class SalaryRatingCorrelationSerializer(serializers.Serializer):
    """Serializer for salary vs rating analysis"""
    rating_range = serializers.CharField()
    avg_salary_min = serializers.DecimalField(max_digits=12, decimal_places=2)
    avg_salary_max = serializers.DecimalField(max_digits=12, decimal_places=2)
    job_count = serializers.IntegerField()


class GlassdoorCompanyReviewSerializer(serializers.ModelSerializer):
    class Meta:
        model = GlassdoorCompanyReview
        fields = '__all__'