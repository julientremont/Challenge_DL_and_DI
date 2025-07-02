from django.db import models


class StackOverflowSurvey(models.Model):
    """
    StackOverflow Developer Survey Data Model
    Based on the actual silver layer schema with tech market focus
    """
    id = models.AutoField(primary_key=True)
    survey_year = models.IntegerField()
    country_normalized = models.CharField(max_length=100, null=True, blank=True)
    primary_role = models.CharField(max_length=255, null=True, blank=True)
    education_normalized = models.CharField(max_length=100, null=True, blank=True)
    
    # Salary data
    salary_usd_cleaned = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    salary_range = models.CharField(max_length=50, null=True, blank=True)
    
    # Technology stack
    technologies_used = models.TextField(null=True, blank=True)
    primary_language = models.CharField(max_length=100, null=True, blank=True)
    
    # Metadata
    processed_at = models.DateTimeField()
    data_quality_score = models.SmallIntegerField(default=0)
    
    class Meta:
        db_table = 'stackoverflow_survey_silver'
        managed = False  # Will be managed by data pipeline
        indexes = [
            models.Index(fields=['survey_year'], name='idx_survey_year'),
            models.Index(fields=['country_normalized'], name='idx_country'),
            models.Index(fields=['primary_role'], name='idx_role'),
            models.Index(fields=['salary_range'], name='idx_salary_range'),
            models.Index(fields=['primary_language'], name='idx_primary_language'),
            models.Index(fields=['data_quality_score'], name='idx_quality_score'),
        ]
    
    def __str__(self):
        return f"Survey {self.survey_year} - {self.primary_role} - {self.country_normalized}"