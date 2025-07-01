from django.db import models


class StackOverflowSurvey(models.Model):
    """
    StackOverflow Developer Survey Data Model
    TODO: Define actual schema when available
    """
    id = models.AutoField(primary_key=True)
    survey_year = models.IntegerField()
    
    # Placeholder fields - to be updated when schema is available
    respondent_id = models.CharField(max_length=100, null=True, blank=True)
    developer_type = models.CharField(max_length=255, null=True, blank=True)
    experience_years = models.CharField(max_length=100, null=True, blank=True)
    education_level = models.CharField(max_length=255, null=True, blank=True)
    salary = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    country = models.CharField(max_length=100, null=True, blank=True)
    gender = models.CharField(max_length=100, null=True, blank=True)
    industry = models.CharField(max_length=255, null=True, blank=True)
    technologies_worked_with = models.TextField(null=True, blank=True)
    
    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'stackoverflow_survey'
        managed = False  # Will be managed by data pipeline
        indexes = [
            models.Index(fields=['survey_year'], name='idx_survey_year'),
            models.Index(fields=['country'], name='idx_survey_country'),
            models.Index(fields=['developer_type'], name='idx_developer_type'),
        ]
    
    def __str__(self):
        return f"Survey {self.survey_year} - Respondent {self.respondent_id}"