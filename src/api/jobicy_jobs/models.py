from django.db import models


class JobicyJobSilver(models.Model):
    """
    Jobicy Jobs Silver Layer Data Model
    Matches the jobicy_silver table schema
    """
    id = models.AutoField(primary_key=True)
    job_id = models.CharField(max_length=255, unique=True)
    job_title = models.CharField(max_length=500, null=True, blank=True)
    company_name = models.CharField(max_length=255, null=True, blank=True)
    company_logo = models.URLField(max_length=1000, null=True, blank=True)
    job_location = models.CharField(max_length=255, null=True, blank=True)
    country_code = models.CharField(max_length=5, null=True, blank=True)
    job_level = models.CharField(max_length=100, null=True, blank=True)
    job_level_standardized = models.CharField(max_length=50, null=True, blank=True)
    job_type = models.CharField(max_length=100, null=True, blank=True)
    job_type_standardized = models.CharField(max_length=50, null=True, blank=True)
    publication_date = models.CharField(max_length=100, null=True, blank=True)
    job_description = models.TextField(null=True, blank=True)
    job_url = models.URLField(max_length=1000, null=True, blank=True)
    job_tags = models.TextField(null=True, blank=True)
    job_industry = models.CharField(max_length=255, null=True, blank=True)
    salary_min = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    salary_max = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    salary_avg = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    salary_currency = models.CharField(max_length=10, null=True, blank=True)
    has_salary_info = models.BooleanField(default=False)
    tags_count = models.IntegerField(default=0)
    processed_date = models.DateTimeField()
    data_quality_score = models.IntegerField(default=0)
    
    class Meta:
        db_table = 'jobicy_silver'
        managed = False
        indexes = [
            models.Index(fields=['country_code'], name='idx_jobicy_country'),
            models.Index(fields=['job_level_standardized'], name='idx_jobicy_level'),
            models.Index(fields=['job_type_standardized'], name='idx_jobicy_type'),
            models.Index(fields=['has_salary_info'], name='idx_jobicy_salary'),
            models.Index(fields=['processed_date'], name='idx_jobicy_processed'),
        ]
    
    def __str__(self):
        return f"{self.job_title} - {self.company_name} ({self.country_code})"
    
    @property
    def has_salary_data(self):
        """Check if job has salary information"""
        return self.has_salary_info
    
    @property
    def quality_level(self):
        """Get quality level based on score"""
        if self.data_quality_score >= 80:
            return 'high'
        elif self.data_quality_score >= 60:
            return 'medium'
        else:
            return 'low'
    
    @property
    def salary_range_text(self):
        """Get formatted salary range"""
        if self.salary_min and self.salary_max:
            currency = self.salary_currency or 'USD'
            return f"{currency} {self.salary_min:,.0f} - {self.salary_max:,.0f}"
        elif self.salary_avg:
            currency = self.salary_currency or 'USD'
            return f"{currency} {self.salary_avg:,.0f} (avg)"
        return "Not specified"
    
    @property
    def experience_level(self):
        """Get standardized experience level"""
        level = self.job_level_standardized or "Not specified"
        return level
    
    @property
    def employment_type(self):
        """Get standardized employment type"""
        emp_type = self.job_type_standardized or "Not specified"
        return emp_type