from django.db import models


class AdzunaJobSilver(models.Model):
    """
    Adzuna Jobs Silver Layer Data Model
    Matches the updated adzuna_jobs_silver table schema
    """
    id = models.AutoField(primary_key=True)
    job_title = models.CharField(max_length=255)
    country_code = models.CharField(max_length=10)
    date = models.DateField()
    average_salary = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)
    processed_at = models.DateTimeField(auto_now_add=True)
    data_quality_score = models.IntegerField(default=0)
    
    class Meta:
        db_table = 'adzuna_jobs_silver'
        managed = False
        indexes = [
            models.Index(fields=['country_code', 'date'], name='idx_adzuna_country_date'),
            models.Index(fields=['job_title'], name='idx_adzuna_job_title'),
            models.Index(fields=['date'], name='idx_adzuna_date'),
        ]
    
    def __str__(self):
        return f"{self.job_title} - {self.country_code} ({self.date})"
    
    @property
    def has_salary_data(self):
        """Check if job has salary information"""
        return self.average_salary is not None
    
    @property
    def quality_level(self):
        """Get quality level based on score"""
        if self.data_quality_score >= 80:
            return 'high'
        elif self.data_quality_score >= 60:
            return 'medium'
        else:
            return 'low'