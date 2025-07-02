from rest_framework import serializers
from .models import GitHubRepos


class GitHubReposSerializer(serializers.ModelSerializer):
    class Meta:
        model = GitHubRepos
        fields = [
            'id', 'name_cleaned', 'technology_normalized', 'search_type',
            'stars_count', 'forks_count', 'watchers_count', 'open_issues_count',
            'created_at_cleaned', 'collected_at_cleaned', 'popularity_score',
            'days_since_creation', 'activity_score', 'activity_level',
            'processed_at', 'data_quality_score'
        ]
        read_only_fields = ['id']


class GitHubReposListSerializer(serializers.ModelSerializer):
    """Simplified serializer for list views"""
    class Meta:
        model = GitHubRepos
        fields = [
            'id', 'name_cleaned', 'technology_normalized', 'stars_count',
            'forks_count', 'activity_level', 'popularity_score', 'created_at_cleaned'
        ]
        read_only_fields = ['id']


class GitHubTechnologyStatsSerializer(serializers.Serializer):
    technology_normalized = serializers.CharField()
    repository_count = serializers.IntegerField()
    avg_stars = serializers.FloatField()
    avg_forks = serializers.FloatField()
    avg_popularity_score = serializers.FloatField()
    total_stars = serializers.IntegerField()
    activity_distribution = serializers.DictField()


class GitHubActivityStatsSerializer(serializers.Serializer):
    activity_level = serializers.CharField()
    repository_count = serializers.IntegerField()
    avg_stars = serializers.FloatField()
    avg_popularity_score = serializers.FloatField()
    avg_days_since_creation = serializers.FloatField()


class GitHubTimeSeriesSerializer(serializers.Serializer):
    period = serializers.CharField()
    repository_count = serializers.IntegerField()
    avg_stars = serializers.FloatField()
    avg_popularity_score = serializers.FloatField()
    technology = serializers.CharField(required=False)


class GitHubPopularReposSerializer(serializers.Serializer):
    id = serializers.IntegerField()
    name_cleaned = serializers.CharField()
    technology_normalized = serializers.CharField()
    stars_count = serializers.IntegerField()
    forks_count = serializers.IntegerField()
    popularity_score = serializers.FloatField()
    activity_level = serializers.CharField()
    created_at_cleaned = serializers.DateTimeField()