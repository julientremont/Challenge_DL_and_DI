from pyspark.sql import SparkSession
import requests
from src.utils.sparkmanager import spark_manager
from pyspark.sql.functions import col, when, regexp_replace, trim, lower, split, explode, size, lit, udf
from pyspark.sql.types import StringType, IntegerType, DoubleType


def extract_country_code(location):
    """Extract country code from location string"""
    if not location or location.lower() == 'unknown':
        return 'unknown'

    location = str(location).strip().lower()

    # Country list from import_jobicy.py with lowercase codes
    country_list = ["austria", "belgium", "bulgaria", "croatia", "cyprus", "czechia", "denmark", "estonia", "finland",
                    "france", "germany", "greece", "hungary", "ireland", "italy", "latvia", "lithuania", "netherlands",
                    "norway", "poland", "portugal", "romania", "serbia", "slovakia", "slovenia", "spain", "sweden",
                    "switzerland", "uk"]

    # Country mappings to lowercase codes
    country_mappings = {
        'austria': 'at',
        'belgium': 'be',
        'bulgaria': 'bg',
        'croatia': 'hr',
        'cyprus': 'cy',
        'czechia': 'cz',
        'czech republic': 'cz',
        'denmark': 'dk',
        'estonia': 'ee',
        'finland': 'fi',
        'france': 'fr',
        'germany': 'de',
        'greece': 'gr',
        'hungary': 'hu',
        'ireland': 'ie',
        'italy': 'it',
        'latvia': 'lv',
        'lithuania': 'lt',
        'netherlands': 'nl',
        'norway': 'no',
        'poland': 'pl',
        'portugal': 'pt',
        'romania': 'ro',
        'serbia': 'rs',
        'slovakia': 'sk',
        'slovenia': 'si',
        'spain': 'es',
        'sweden': 'se',
        'switzerland': 'ch',
        'uk': 'gb',
        'united kingdom': 'gb',
        'great britain': 'gb',
        'england': 'gb',
        'scotland': 'gb',
        'wales': 'gb',
        'northern ireland': 'gb'
    }

    # Direct mapping check
    if location in country_mappings:
        return country_mappings[location]

    # Check if any country name is contained in the location
    for country_name, code in country_mappings.items():
        if country_name in location:
            return code

    # Check if it's a city with country info (e.g., "paris, france")
    if ',' in location:
        parts = location.split(',')
        country_part = parts[-1].strip()
        if country_part in country_mappings:
            return country_mappings[country_part]

    # Check each word in the location
    words = location.split()
    for word in words:
        word = word.strip('.,()[]')
        if word in country_mappings:
            return country_mappings[word]

    return 'unknown'


def clean_jobicy():
    """
    Clean and process Jobicy data from bronze layer to silver layer.
    Similar to AzunaSilver process but adapted for Jobicy data structure.
    """

    # Initialize Spark session
    spark = SparkSession.builder.appName("JobicySilver").getOrCreate()

    # Register UDF for country code extraction
    extract_country_code_udf = udf(extract_country_code, StringType())

    try:
        # Read the bronze data from parquet files
        df = spark_manager.read_parquet("data/bronze/jobicy/")

        if df.count() == 0:
            print("No data found in bronze/jobicy/")
            return

        # Show initial data structure for debugging
        print("Initial schema:")
        df.printSchema()

        # Clean and standardize the data
        cleaned_df = df.select(
            col("id").cast(StringType()).alias("job_id"),
            col("jobTitle").alias("job_title"),
            col("companyName").alias("company_name"),
            col("companyLogo").alias("company_logo"),
            col("jobGeo").alias("job_location"),
            col("jobLevel").alias("job_level"),
            col("jobType").alias("job_type"),
            col("pubDate").alias("publication_date"),
            col("jobExcerpt").alias("job_description"),
            col("url").alias("job_url"),
            col("tags").alias("job_tags"),
            col("jobIndustry").alias("job_industry"),
            col("annualSalaryMin").cast(DoubleType()).alias("salary_min"),
            col("annualSalaryMax").cast(DoubleType()).alias("salary_max"),
            col("salaryCurrency").alias("salary_currency")
        )

        # Extract country code from job location
        cleaned_df = cleaned_df.withColumn("country_code", extract_country_code_udf(col("job_location")))

        # Clean text fields - remove extra whitespace and normalize
        cleaned_df = cleaned_df.withColumn("job_title", trim(col("job_title"))) \
            .withColumn("company_name", trim(col("company_name"))) \
            .withColumn("job_location", trim(col("job_location"))) \
            .withColumn("job_description", trim(col("job_description")))

        # Standardize job levels
        cleaned_df = cleaned_df.withColumn("job_level_standardized",
                                           when(lower(col("job_level")).contains("senior"), "Senior")
                                           .when(lower(col("job_level")).contains("junior"), "Junior")
                                           .when(lower(col("job_level")).contains("mid"), "Mid-level")
                                           .when(lower(col("job_level")).contains("entry"), "Entry-level")
                                           .otherwise("Not specified"))

        # Standardize job types
        cleaned_df = cleaned_df.withColumn("job_type_standardized",
                                           when(lower(col("job_type")).contains("full"), "Full-time")
                                           .when(lower(col("job_type")).contains("part"), "Part-time")
                                           .when(lower(col("job_type")).contains("contract"), "Contract")
                                           .when(lower(col("job_type")).contains("freelance"), "Freelance")
                                           .otherwise("Not specified"))

        # Create salary range indicator
        cleaned_df = cleaned_df.withColumn("has_salary_info",
                                           when((col("salary_min").isNotNull()) | (col("salary_max").isNotNull()), True)
                                           .otherwise(False))

        # Calculate average salary where both min and max are available
        cleaned_df = cleaned_df.withColumn("salary_avg",
                                           when((col("salary_min").isNotNull()) & (col("salary_max").isNotNull()),
                                                (col("salary_min") + col("salary_max")) / 2)
                                           .otherwise(None))

        # Process tags if they exist (assuming they might be in array format or comma-separated)
        if "job_tags" in cleaned_df.columns:
            cleaned_df = cleaned_df.withColumn("tags_count",
                                               when(col("job_tags").isNotNull(),
                                                    size(col("job_tags")))
                                               .otherwise(0))

        # Add processing metadata
        cleaned_df = cleaned_df.withColumn("processed_date",
                                           lit(spark.sql("SELECT current_timestamp()").collect()[0][0]))

        # Remove duplicates based on job_id
        cleaned_df = cleaned_df.dropDuplicates(["job_id"])

        # Show statistics
        print(f"Total records after cleaning: {cleaned_df.count()}")
        print(f"Records with salary info: {cleaned_df.filter(col('has_salary_info') == True).count()}")

        # Show country code distribution
        print("\nCountry code distribution:")
        cleaned_df.groupBy("country_code").count().orderBy(col("count").desc()).show(20)

        # Show sample of cleaned data
        print("\nSample of cleaned data:")
        cleaned_df.select("job_id", "job_title", "company_name", "job_location", "country_code",
                          "job_level_standardized", "job_type_standardized", "has_salary_info").show(10, False)

        # Write cleaned data to silver layer
        spark_manager.write_parquet(cleaned_df, "data/silver/jobicy/", mode="overwrite")

        print("Jobicy data successfully cleaned and saved to silver layer!")

    except Exception as e:
        print(f"Error in clean_jobicy: {str(e)}")
        raise e

    finally:
        spark.stop()


if __name__ == "__main__":
    clean_jobicy()