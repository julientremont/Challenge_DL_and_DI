from django.db import models


class EuroTechJobs(models.Model):
    id = models.AutoField(primary_key=True)
    job_title = models.CharField(max_length=500)
    job_title_category = models.CharField(max_length=255)
    company = models.CharField(max_length=255)
    location = models.CharField(max_length=255)
    country_code = models.CharField(max_length=3)
    technologies = models.TextField(null=True, blank=True)
    primary_technology = models.CharField(max_length=100)
    tech_count = models.IntegerField(default=0)
    job_type = models.CharField(max_length=50)
    category = models.CharField(max_length=100)
    url = models.CharField(max_length=1000, unique=True)
    processed_at = models.DateTimeField()
    data_quality_score = models.SmallIntegerField(default=0)
    
    class Meta:
        db_table = 'eurotechjobs_silver'
        managed = False
        indexes = [
            models.Index(fields=['country_code'], name='idx_etj_country'),
            models.Index(fields=['job_title_category'], name='idx_etj_job_title_category'),
            models.Index(fields=['company'], name='idx_etj_company'),
            models.Index(fields=['primary_technology'], name='idx_etj_primary_tech'),
            models.Index(fields=['job_type'], name='idx_etj_job_type'),
            models.Index(fields=['category'], name='idx_etj_category'),
            models.Index(fields=['data_quality_score'], name='idx_etj_quality'),
            models.Index(fields=['processed_at'], name='idx_etj_processed_at'),
        ]
    
    def __str__(self):
        return f"{self.job_title} - {self.company} - {self.country_code}"