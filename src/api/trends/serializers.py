from rest_framework import serializers
from .models import TrendsFR


class TrendsFRSerializer(serializers.ModelSerializer):
    class Meta:
        model = TrendsFR
        fields = ['id', 'country_code', 'date', 'keyword', 'search_frequency', 'country']
        read_only_fields = ['id']


class TrendsAggregatedSerializer(serializers.Serializer):
    keyword = serializers.CharField()
    avg_search_frequency = serializers.FloatField()
    total_searches = serializers.IntegerField()
    date_range = serializers.CharField()


class TrendsTimeSeriesSerializer(serializers.Serializer):
    date = serializers.DateField()
    search_frequency = serializers.IntegerField()
    keyword = serializers.CharField()