from rest_framework import serializers
from .models import TrendsSilver


class TrendsSilverSerializer(serializers.ModelSerializer):
    """Serializer for Google Trends Silver data"""
    popularity_level = serializers.ReadOnlyField()
    quality_level = serializers.ReadOnlyField()
    
    class Meta:
        model = TrendsSilver
        fields = [
            'id', 'date', 'keyword', 'country_code', 'search_frequency',
            'processed_at', 'data_quality_score', 'popularity_level', 'quality_level'
        ]
        read_only_fields = ['id', 'processed_at']


class TrendsSummarySerializer(serializers.Serializer):
    """Serializer for aggregated trends data"""
    total_records = serializers.IntegerField()
    unique_keywords = serializers.IntegerField()
    countries_covered = serializers.IntegerField()
    avg_search_frequency = serializers.FloatField()
    max_search_frequency = serializers.IntegerField()
    min_search_frequency = serializers.IntegerField()
    data_quality_avg = serializers.FloatField()
    date_range = serializers.DictField()


class KeywordAnalysisSerializer(serializers.Serializer):
    """Serializer for keyword performance analysis"""
    keyword = serializers.CharField()
    avg_search_frequency = serializers.FloatField()
    total_data_points = serializers.IntegerField()
    countries_tracked = serializers.IntegerField()
    peak_frequency = serializers.IntegerField()
    peak_date = serializers.DateField()
    trend_direction = serializers.CharField()
    data_quality_avg = serializers.FloatField()


class CountryTrendsSerializer(serializers.Serializer):
    """Serializer for country-specific trends"""
    country_code = serializers.CharField()
    unique_keywords = serializers.IntegerField()
    avg_search_frequency = serializers.FloatField()
    most_searched_keyword = serializers.CharField()
    trending_keywords = serializers.ListField(child=serializers.CharField())
    data_quality_avg = serializers.FloatField()


class TrendsTimeSeriesSerializer(serializers.Serializer):
    """Serializer for time series trends data"""
    date = serializers.DateField()
    search_frequency = serializers.FloatField()
    keyword = serializers.CharField()
    country_code = serializers.CharField()


class TechTrendsAnalysisSerializer(serializers.Serializer):
    """Serializer for technology trends analysis"""
    keyword = serializers.CharField()
    category = serializers.CharField()
    avg_popularity = serializers.FloatField()
    growth_rate = serializers.FloatField()
    countries_interested = serializers.ListField(child=serializers.CharField())
    peak_periods = serializers.ListField(child=serializers.DateField())
