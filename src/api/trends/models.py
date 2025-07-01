from django.db import models


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