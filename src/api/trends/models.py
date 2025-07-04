from django.db import models


class TrendsSilver(models.Model):
    """
    Google Trends Silver Layer Data Model
    Matches the trends_silver table schema
    """
    id = models.AutoField(primary_key=True)
    date = models.DateField()
    keyword = models.CharField(max_length=100)
    country_code = models.CharField(max_length=5)
    search_frequency = models.IntegerField(default=0)
    processed_at = models.DateTimeField()
    data_quality_score = models.SmallIntegerField(default=0)
    
    class Meta:
        db_table = 'trends_silver'
        managed = False  # Managed by data pipeline
        indexes = [
            models.Index(fields=['date'], name='idx_trends_date'),
            models.Index(fields=['keyword'], name='idx_trends_keyword'),
            models.Index(fields=['country_code'], name='idx_trends_country_code'),
            models.Index(fields=['search_frequency'], name='idx_trends_search_frequency'),
            models.Index(fields=['date', 'keyword'], name='idx_trends_date_keyword'),
            models.Index(fields=['data_quality_score'], name='idx_trends_quality_score'),
        ]
    
    def __str__(self):
        return f"{self.keyword} - {self.country_code} - {self.date}"
    
    @property
    def popularity_level(self):
        """Get popularity level based on search frequency"""
        if self.search_frequency >= 80:
            return 'viral'
        elif self.search_frequency >= 50:
            return 'high'
        elif self.search_frequency >= 20:
            return 'medium'
        elif self.search_frequency > 0:
            return 'low'
        else:
            return 'minimal'
    
    @property
    def quality_level(self):
        """Get quality level based on score"""
        if self.data_quality_score >= 80:
            return 'high'
        elif self.data_quality_score >= 60:
            return 'medium'
        else:
            return 'low'


# Legacy model for backward compatibility (if needed)
class TrendsFR(models.Model):
    id = models.AutoField(primary_key=True)
    country_code = models.CharField(max_length=2)
    date = models.DateField()
    keyword = models.CharField(max_length=100)
    search_frequency = models.IntegerField()
    country = models.CharField(max_length=50)
    
    class Meta:
        db_table = 'Trends_FR'
        managed = False
        indexes = [
            models.Index(fields=['country_code'], name='idx_country_code'),
            models.Index(fields=['date'], name='idx_date'),
            models.Index(fields=['keyword'], name='idx_keyword'),
        ]
    
    def __str__(self):
        return f"{self.keyword} - {self.country_code} - {self.date}"