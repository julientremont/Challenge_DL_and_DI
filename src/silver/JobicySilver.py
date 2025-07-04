from pyspark.sql import SparkSession
import requests
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from src.utils.sparkmanager import spark_manager
from src.utils.sqlmanager import sql_manager
from src.utils.mysql_schemas import create_table, save_spark_df_to_mysql
from pyspark.sql.functions import col, when, trim, lower, size, lit, udf, current_timestamp
from pyspark.sql.types import StringType, DoubleType, IntegerType, BooleanType
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_bronze_parquet(spark,output_path):
    return spark.read.parquet(output_path)

def extract_country_code(location):
    """Extract country code from location string"""
    if not location or location.lower() == 'unknown':
        return 'unknown'

    location = str(location).strip().lower()

    country_list = ["austria", "belgium", "bulgaria", "croatia", "cyprus", "czechia", "denmark", "estonia", "finland",
                    "france", "germany", "greece", "hungary", "ireland", "italy", "latvia", "lithuania", "netherlands",
                    "norway", "poland", "portugal", "romania", "serbia", "slovakia", "slovenia", "spain", "sweden",
                    "switzerland", "uk"]

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

    if location in country_mappings:
        return country_mappings[location]

    for country_name, code in country_mappings.items():
        if country_name in location:
            return code

    if ',' in location:
        parts = location.split(',')
        country_part = parts[-1].strip()
        if country_part in country_mappings:
            return country_mappings[country_part]

    words = location.split()
    for word in words:
        word = word.strip('.,()[]')
        if word in country_mappings:
            return country_mappings[word]

    return 'unknown'

def clean_jobicy():
    """
    Clean and process Jobicy data from bronze layer to silver layer.
    Uses SQLManager for database operations like other silver processors.
    """
    table_name = "jobicy_silver"
    
    # Déplacer tout le traitement à l'intérieur du context manager
    with spark_manager as sm:
        spark = sm.get_session()
        
        extract_country_code_udf = udf(extract_country_code, StringType())

        try:
            output_path = "./data/bronze/jobicy"

            df = get_bronze_parquet(spark, output_path)

            if df.count() == 0:
                print("No data found in bronze/jobicy")
                return

            print("Initial schema:")
            df.printSchema()

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
                col("jobIndustry").alias("job_industry"),
                col("SalaryMin").cast(DoubleType()).alias("salary_min"),
                col("SalaryMax").cast(DoubleType()).alias("salary_max"),
                col("salaryCurrency").alias("salary_currency")
            )

            cleaned_df = cleaned_df.withColumn("country_code", extract_country_code_udf(col("job_location")))

            cleaned_df = cleaned_df.withColumn("job_title", trim(col("job_title"))) \
                .withColumn("company_name", trim(col("company_name"))) \
                .withColumn("job_location", trim(col("job_location"))) \
                .withColumn("job_description", trim(col("job_description")))

            cleaned_df = cleaned_df.withColumn("job_level_standardized",
                                               when(lower(col("job_level")).contains("senior"), "Senior")
                                               .when(lower(col("job_level")).contains("junior"), "Junior")
                                               .when(lower(col("job_level")).contains("mid"), "Mid-level")
                                               .when(lower(col("job_level")).contains("entry"), "Entry-level")
                                               .otherwise("Not specified"))

            cleaned_df = cleaned_df.withColumn("has_salary_info",
                                               when((col("salary_min").isNotNull()) | (col("salary_max").isNotNull()), True)
                                               .otherwise(False))

            cleaned_df = cleaned_df.withColumn("salary_avg",
                                               when((col("salary_min").isNotNull()) & (col("salary_max").isNotNull()),
                                                    (col("salary_min") + col("salary_max")) / 2)
                                               .otherwise(None))

            cleaned_df = cleaned_df.withColumn("processed_date", current_timestamp())
            
            cleaned_df = cleaned_df.withColumn("data_quality_score",
                                               lit(100))

            cleaned_df = cleaned_df.dropDuplicates(["job_id"])

            print(f"Total records after cleaning: {cleaned_df.count()}")
            print(f"Records with salary info: {cleaned_df.filter(col('has_salary_info') == True).count()}")

            print("\nCountry code distribution:")
            cleaned_df.groupBy("country_code").count().orderBy(col("count").desc()).show(20)

            print("\nSample of cleaned data:")
            cleaned_df.select("job_id", "job_title", "company_name", "job_location", "country_code",
                              "job_level_standardized", "has_salary_info").show(10, False)

            logger.info(f"Creating/verifying MySQL table: {table_name}")
            success = create_table(table_name)
            if not success:
                logger.error(f"Failed to create table: {table_name}")
                raise Exception(f"Failed to create table: {table_name}")
            
            logger.info("Saving cleaned data to MySQL...")
            affected_rows = save_spark_df_to_mysql(cleaned_df, table_name)
            

            logger.info(f"Successfully saved {affected_rows} rows to {table_name}")
            
            print(f"Jobicy data successfully processed: {affected_rows} rows saved to MySQL and parquet!")

        except Exception as e:
            logger.error(f"Error in clean_jobicy: {str(e)}")
            raise e



def main():
    """Main execution function"""
    logger.info("Starting Jobicy silver layer processing...")
    clean_jobicy()
    logger.info("Jobicy silver layer processing completed!")

if __name__ == "__main__":
    main()