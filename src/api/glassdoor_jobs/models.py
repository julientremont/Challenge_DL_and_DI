from django.db import models


class GlassdoorJob(models.Model):
    """
    Glassdoor Jobs Data Model
    TODO: Define actual schema when available
    """
    id = models.AutoField(primary_key=True)
    
    # Placeholder fields - to be updated when schema is available
    job_id = models.CharField(max_length=100, unique=True)
    title = models.CharField(max_length=500)
    company = models.CharField(max_length=255, null=True, blank=True)
    location = models.CharField(max_length=255, null=True, blank=True)
    country = models.CharField(max_length=100, null=True, blank=True)
    salary_estimate = models.CharField(max_length=255, null=True, blank=True)
    salary_min = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    salary_max = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    salary_currency = models.CharField(max_length=10, null=True, blank=True)
    job_description = models.TextField(null=True, blank=True)
    rating = models.FloatField(null=True, blank=True)  # Company rating
    company_size = models.CharField(max_length=100, null=True, blank=True)
    company_type = models.CharField(max_length=100, null=True, blank=True)  # Public, Private, etc.
    industry = models.CharField(max_length=255, null=True, blank=True)
    sector = models.CharField(max_length=255, null=True, blank=True)
    revenue = models.CharField(max_length=100, null=True, blank=True)
    headquarters = models.CharField(max_length=255, null=True, blank=True)
    founded = models.IntegerField(null=True, blank=True)
    easy_apply = models.BooleanField(default=False)
    posted_date = models.DateTimeField(null=True, blank=True)
    
    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'glassdoor_jobs'
        managed = False  # Will be managed by data pipeline
        indexes = [
            models.Index(fields=['country'], name='idx_glassdoor_country'),
            models.Index(fields=['location'], name='idx_glassdoor_location'),
            models.Index(fields=['industry'], name='idx_glassdoor_industry'),
            models.Index(fields=['company'], name='idx_glassdoor_company'),
            models.Index(fields=['posted_date'], name='idx_glassdoor_posted_date'),
            models.Index(fields=['rating'], name='idx_glassdoor_rating'),
        ]
    
    def __str__(self):
        return f"{self.title} at {self.company} ({self.location})"


class GlassdoorCompanyReview(models.Model):
    """
    Company reviews from Glassdoor
    TODO: Define relationship when schema is available
    """
    id = models.AutoField(primary_key=True)
    company = models.CharField(max_length=255)
    overall_rating = models.FloatField(null=True, blank=True)
    work_life_balance = models.FloatField(null=True, blank=True)
    culture_values = models.FloatField(null=True, blank=True)
    career_opportunities = models.FloatField(null=True, blank=True)
    compensation_benefits = models.FloatField(null=True, blank=True)
    senior_management = models.FloatField(null=True, blank=True)
    recommend_to_friend = models.FloatField(null=True, blank=True)
    approve_of_ceo = models.FloatField(null=True, blank=True)
    
    class Meta:
        db_table = 'glassdoor_company_reviews'
        managed = False
    
    def __str__(self):
        return f"{self.company} - Rating: {self.overall_rating}"