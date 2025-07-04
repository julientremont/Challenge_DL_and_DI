"""
Analysis models for Gold layer data warehouse.
These models map to the gold database tables created by the table_unification script.
"""

from django.db import models


class DimCalendar(models.Model):
    """Dimension table for dates"""
    date_key = models.DateField(primary_key=True)
    day = models.SmallIntegerField()
    month = models.SmallIntegerField()
    quarter = models.SmallIntegerField()
    year = models.SmallIntegerField()
    day_of_week = models.SmallIntegerField()
    week_of_year = models.SmallIntegerField()
    is_weekend = models.BooleanField(default=False)

    class Meta:
        managed = False
        db_table = 'dim_calendar'


class DimCountry(models.Model):
    """Dimension table for countries"""
    id_country = models.AutoField(primary_key=True)
    country_code = models.CharField(max_length=3, unique=True)
    country_name = models.CharField(max_length=100, null=True, blank=True)
    region = models.CharField(max_length=50, null=True, blank=True)
    continent = models.CharField(max_length=30, null=True, blank=True)

    class Meta:
        managed = False
        db_table = 'dim_country'


class DimCompany(models.Model):
    """Dimension table for companies"""
    id_company = models.AutoField(primary_key=True)
    company_name = models.CharField(max_length=255)
    company_normalized = models.CharField(max_length=255, null=True, blank=True)
    sector = models.CharField(max_length=100, null=True, blank=True)
    size_category = models.CharField(max_length=50, null=True, blank=True)

    class Meta:
        managed = False
        db_table = 'dim_company'


class DimTechnology(models.Model):
    """Dimension table for technologies"""
    TECH_TYPE_CHOICES = [
        ('language', 'Programming Language'),
        ('framework', 'Framework'),
        ('tool', 'Tool'),
        ('database', 'Database'),
        ('cloud', 'Cloud/DevOps'),
        ('other', 'Other'),
    ]

    id_technology = models.AutoField(primary_key=True)
    technology_name = models.CharField(max_length=100, unique=True)
    technology_category = models.CharField(max_length=50, null=True, blank=True)
    technology_type = models.CharField(max_length=20, choices=TECH_TYPE_CHOICES, default='other')
    popularity_rank = models.IntegerField(null=True, blank=True)

    class Meta:
        managed = False
        db_table = 'dim_technology'


class DimDataSource(models.Model):
    """Dimension table for data sources"""
    SOURCE_TYPE_CHOICES = [
        ('job_board', 'Job Board'),
        ('survey', 'Survey'),
        ('repository', 'Repository'),
        ('trends', 'Trends'),
    ]

    id_source = models.SmallAutoField(primary_key=True)
    source_name = models.CharField(max_length=50, unique=True)
    source_type = models.CharField(max_length=20, choices=SOURCE_TYPE_CHOICES)
    source_description = models.TextField(null=True, blank=True)
    is_active = models.BooleanField(default=True)

    class Meta:
        managed = False
        db_table = 'dim_data_source'


class DimJobRole(models.Model):
    """Dimension table for job roles"""
    SENIORITY_CHOICES = [
        ('junior', 'Junior'),
        ('mid', 'Mid-level'),
        ('senior', 'Senior'),
        ('lead', 'Lead'),
        ('manager', 'Manager'),
        ('unknown', 'Unknown'),
    ]

    ROLE_TYPE_CHOICES = [
        ('developer', 'Developer'),
        ('data', 'Data'),
        ('devops', 'DevOps'),
        ('design', 'Design'),
        ('management', 'Management'),
        ('other', 'Other'),
    ]

    id_job_role = models.AutoField(primary_key=True)
    role_title = models.CharField(max_length=255)
    role_category = models.CharField(max_length=100, null=True, blank=True)
    seniority_level = models.CharField(max_length=20, choices=SENIORITY_CHOICES, default='unknown')
    role_type = models.CharField(max_length=20, choices=ROLE_TYPE_CHOICES, default='other')

    class Meta:
        managed = False
        db_table = 'dim_job_role'


class AnalysisTechActivity(models.Model):
    """Fact table for technology activity analysis"""
    id_activity = models.BigAutoField(primary_key=True)
    date_key = models.DateField()
    id_country = models.ForeignKey(DimCountry, on_delete=models.DO_NOTHING, null=True, blank=True, db_column='id_country')
    id_technology = models.ForeignKey(DimTechnology, on_delete=models.DO_NOTHING, null=True, blank=True, db_column='id_technology')
    id_source = models.ForeignKey(DimDataSource, on_delete=models.DO_NOTHING, null=True, blank=True, db_column='id_source')
    
    # Job metrics
    job_count = models.IntegerField(default=0)
    avg_salary_usd = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    
    # Developer metrics (StackOverflow)
    developer_count = models.IntegerField(default=0)
    avg_developer_salary = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    
    # Popularity metrics (GitHub + Trends)
    github_stars = models.IntegerField(default=0)
    github_forks = models.IntegerField(default=0)
    search_volume = models.IntegerField(default=0)
    popularity_score = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    
    # Metadata
    data_quality_score = models.SmallIntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = False
        db_table = 'analysis_tech_activity'


class AnalysisJobDetails(models.Model):
    """Fact table for job details analysis"""
    JOB_TYPE_CHOICES = [
        ('full_time', 'Full-time'),
        ('part_time', 'Part-time'),
        ('contract', 'Contract'),
        ('freelance', 'Freelance'),
        ('unknown', 'Unknown'),
    ]

    SENIORITY_CHOICES = [
        ('junior', 'Junior'),
        ('mid', 'Mid-level'),
        ('senior', 'Senior'),
        ('lead', 'Lead'),
        ('unknown', 'Unknown'),
    ]

    id_job = models.BigAutoField(primary_key=True)
    date_key = models.DateField()
    id_country = models.ForeignKey(DimCountry, on_delete=models.DO_NOTHING, null=True, blank=True, db_column='id_country')
    id_company = models.ForeignKey(DimCompany, on_delete=models.DO_NOTHING, null=True, blank=True, db_column='id_company')
    id_job_role = models.ForeignKey(DimJobRole, on_delete=models.DO_NOTHING, null=True, blank=True, db_column='id_job_role')
    id_source = models.ForeignKey(DimDataSource, on_delete=models.DO_NOTHING, null=True, blank=True, db_column='id_source')
    
    # Job details
    job_title = models.CharField(max_length=500, null=True, blank=True)
    salary_usd = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    salary_range = models.CharField(max_length=50, null=True, blank=True)
    job_type = models.CharField(max_length=20, choices=JOB_TYPE_CHOICES, default='unknown')
    seniority = models.CharField(max_length=20, choices=SENIORITY_CHOICES, default='unknown')
    
    # Metadata
    data_quality_score = models.SmallIntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = False
        db_table = 'analysis_job_details'


class BridgeJobTechnologies(models.Model):
    """Bridge table for job-technology relationships"""
    id_job = models.ForeignKey(AnalysisJobDetails, on_delete=models.CASCADE, db_column='id_job')
    id_technology = models.ForeignKey(DimTechnology, on_delete=models.CASCADE, db_column='id_technology')
    is_primary = models.BooleanField(default=False)

    class Meta:
        managed = False
        db_table = 'bridge_job_technologies'
        unique_together = ['id_job', 'id_technology']