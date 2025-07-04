from django.db import models


class AdzunaJobSilver(models.Model):
    """
    Adzuna Jobs Silver Layer Data Model
    Matches the adzuna_jobs_silver table schema
    """
    id = models.AutoField(primary_key=True)
    date = models.DateField()
    job_title = models.CharField(max_length=255)
    country_code = models.CharField(max_length=5)
    country_name = models.CharField(max_length=100)
    average_salary = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    salary_range = models.CharField(max_length=50, null=True, blank=True)
    job_count = models.IntegerField(default=0)
    processed_at = models.DateTimeField()
    data_quality_score = models.SmallIntegerField(default=0)
    
    class Meta:
        db_table = 'adzuna_jobs_silver'
        managed = False  # Managed by data pipeline
        indexes = [
            models.Index(fields=['date'], name='idx_adzuna_date'),
            models.Index(fields=['job_title'], name='idx_adzuna_job_title'),
            models.Index(fields=['country_code'], name='idx_adzuna_country_code'),
            models.Index(fields=['salary_range'], name='idx_adzuna_salary_range'),
            models.Index(fields=['date', 'job_title'], name='idx_adzuna_date_job'),
            models.Index(fields=['data_quality_score'], name='idx_adzuna_quality_score'),
        ]
    
    def __str__(self):
        return f"{self.job_title} - {self.country_name} ({self.date})"
    
    @property
    def has_salary_data(self):
        """Check if job has salary information"""
        return self.average_salary is not None or self.salary_range is not None
    
    @property
    def quality_level(self):
        """Get quality level based on score"""
        if self.data_quality_score >= 80:
            return 'high'
        elif self.data_quality_score >= 60:
            return 'medium'
        else:
            return 'low'