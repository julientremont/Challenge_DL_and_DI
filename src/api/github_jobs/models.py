from django.db import models


class GitHubRepos(models.Model):
    id = models.BigIntegerField(primary_key=True)
    name_cleaned = models.CharField(max_length=255)
    technology_normalized = models.CharField(max_length=100)
    search_type = models.CharField(max_length=50)
    stars_count = models.IntegerField(default=0)
    forks_count = models.IntegerField(default=0)
    watchers_count = models.IntegerField(default=0)
    open_issues_count = models.IntegerField(default=0)
    created_at_cleaned = models.DateTimeField()
    collected_at_cleaned = models.DateTimeField()
    popularity_score = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    days_since_creation = models.IntegerField(default=0)
    activity_score = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    activity_level = models.CharField(
        max_length=20, 
        choices=[('low', 'Low'), ('medium', 'Medium'), ('high', 'High')],
        default='low'
    )
    processed_at = models.DateTimeField()
    data_quality_score = models.SmallIntegerField(default=0)
    
    class Meta:
        db_table = 'github_repos_silver'
        managed = False
        indexes = [
            models.Index(fields=['technology_normalized'], name='idx_github_technology'),
            models.Index(fields=['activity_level'], name='idx_github_activity_level'),
            models.Index(fields=['created_at_cleaned'], name='idx_github_created_at'),
            models.Index(fields=['popularity_score'], name='idx_github_popularity'),
            models.Index(fields=['data_quality_score'], name='idx_github_quality'),
        ]
    
    def __str__(self):
        return f"{self.name_cleaned} - {self.technology_normalized} - {self.activity_level}"