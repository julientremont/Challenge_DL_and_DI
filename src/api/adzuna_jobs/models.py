from django.db import models


class AdzunaJob(models.Model):
    """
    Adzuna Jobs Data Model
    TODO: Define actual schema when available
    """
    id = models.AutoField(primary_key=True)
    
    # Placeholder fields - to be updated when schema is available
    job_id = models.CharField(max_length=100, unique=True)
    title = models.CharField(max_length=500)
    company = models.CharField(max_length=255, null=True, blank=True)
    location = models.CharField(max_length=255, null=True, blank=True)
    country = models.CharField(max_length=100, null=True, blank=True)
    salary_min = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    salary_max = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    salary_currency = models.CharField(max_length=10, null=True, blank=True)
    job_type = models.CharField(max_length=100, null=True, blank=True)  # full-time, part-time, contract
    experience_level = models.CharField(max_length=100, null=True, blank=True)
    skills_required = models.TextField(null=True, blank=True)
    job_description = models.TextField(null=True, blank=True)
    posted_date = models.DateTimeField(null=True, blank=True)
    deadline = models.DateTimeField(null=True, blank=True)
    remote_allowed = models.BooleanField(default=False)
    
    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'adzuna_jobs'
        managed = False  # Will be managed by data pipeline
        indexes = [
            models.Index(fields=['country'], name='idx_adzuna_country'),
            models.Index(fields=['location'], name='idx_adzuna_location'),
            models.Index(fields=['job_type'], name='idx_adzuna_job_type'),
            models.Index(fields=['posted_date'], name='idx_adzuna_posted_date'),
            models.Index(fields=['experience_level'], name='idx_adzuna_experience'),
        ]
    
    def __str__(self):
        return f"{self.title} at {self.company} ({self.location})"


class AdzunaJobCategory(models.Model):
    """
    Job categories/tags for Adzuna jobs
    TODO: Define relationship when schema is available
    """
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=100, unique=True)
    description = models.TextField(null=True, blank=True)
    
    class Meta:
        db_table = 'adzuna_job_categories'
        managed = False
    
    def __str__(self):
        return self.name