import sys
import os
import logging
from pyspark.sql.functions import col, when, isnan, upper, lit
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from dotenv import load_dotenv
from functools import reduce
import operator

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from src.utils.sparkmanager import spark_manager
from src.utils.sqlmanager import sql_manager
from src.utils.mysql_schemas import create_table, save_spark_df_to_mysql

# Charger les variables d'environnement
load_dotenv()

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class AdzunaSilverProcessor:
    """Adzuna jobs silver layer processor using centralized schema and SQLManager"""
    
    def __init__(self):
        self.table_name = "adzuna_jobs_silver"
        self.pays_europeens = {
            "at": "Autriche",
            "be": "Belgique", 
            "ch": "Suisse",
            "de": "Allemagne",
            "es": "Espagne",
            "fr": "France",
            "gb": "Royaume-Uni",
            "it": "Italie",
            "nl": "Pays-Bas",
            "pl": "Pologne"
        }

    def supprimer_lignes_vides(self, df, seuil_pct=0.9):
        """Remove rows with too many null values"""
        nb_colonnes = len(df.columns)
        null_conditions = []
        
        for c in df.columns:
            col_type = dict(df.dtypes)[c]
            if col_type.lower() in ['double', 'float']:
                null_condition = when(col(c).isNull() | isnan(col(c)), 1).otherwise(0)
            else:
                null_condition = when(col(c).isNull(), 1).otherwise(0)
            
            null_conditions.append(null_condition)
        sum_nulls = reduce(operator.add, null_conditions)
        df_with_null_count = df.withColumn("nb_nulls", sum_nulls)
        df_with_pct = df_with_null_count.withColumn(
            "pct_nulls", 
            col("nb_nulls") / nb_colonnes
        )
        df_filtre = df_with_pct.filter(col("pct_nulls") < seuil_pct)
        df_final = df_filtre.drop("nb_nulls", "pct_nulls")
        
        return df_final

    def get_bronze_parquet(self, spark, output_path):
        """Read bronze parquet data"""
        return spark.read.parquet(output_path)

    def merge_multiple_dataframes_reduce(self, df1, df2):
        """Merge dataframes using union"""
        return df1.union(df2)

    def create_mysql_table(self):
        """Create MySQL table using centralized schema"""
        success = create_table(self.table_name)
        if success:
            logger.info(f"Created/verified table: {self.table_name}")
        else:
            logger.error(f"Failed to create table: {self.table_name}")
            raise Exception(f"Failed to create table: {self.table_name}")

    def save_to_mysql(self, df):
        """Save dataframe to MySQL using SQLManager via helper function"""
        try:
            affected_rows = save_spark_df_to_mysql(df, self.table_name, batch_size=500)
            logger.info(f"Successfully saved {affected_rows} rows to {self.table_name}")
        except Exception as e:
            logger.error(f"Error saving to MySQL: {e}")
            raise
    def process_salary_data(self):
        """Process salary data from bronze layer"""
        logger.info("Processing Adzuna salary data...")
        
        with spark_manager as sm:
            spark = sm.get_session()
            dataframes = []
            
            for key in self.pays_europeens.keys():
                output_path = f"./data/bronze/azuna/Salary/{key}"
                try:
                    df = self.get_bronze_parquet(spark, output_path)
                    dataframes.append(df)
                    logger.info(f"Loaded salary data for {key}")
                except Exception as e:
                    logger.warning(f"Could not load salary data for {key}: {e}")
                    continue
            
            if not dataframes:
                logger.warning("No salary data found to process")
                return
            
            # Merge all dataframes
            merged_df = reduce(self.merge_multiple_dataframes_reduce, dataframes)
            
            # Data transformations
            drop_df = merged_df.drop('annee_insertion', 'mois_insertion').dropDuplicates()
            rename_df = drop_df.withColumnRenamed('date', 'date') \
                              .withColumnRenamed('average_salary', 'average_salary') \
                              .withColumnRenamed('keyword', 'job_title')
            
            # Process each country
            final_dataframes = []
            for key in self.pays_europeens.keys():
                country_df = rename_df.withColumn('country_code', upper(lit(key))) \
                                    .withColumn('country_name', lit(self.pays_europeens[key]))
                final_dataframes.append(country_df)
            
            # Merge country dataframes
            if final_dataframes:
                final_df = reduce(self.merge_multiple_dataframes_reduce, final_dataframes)
                clean_df = self.supprimer_lignes_vides(final_df)
                
                # Add processing metadata
                clean_df = clean_df.withColumn('processed_at', current_timestamp()) \
                                 .withColumn('data_quality_score', lit(85))
                
                return clean_df
            
        return None
    
    def process_dispersion_data(self):
        """Process job dispersion data from bronze layer"""
        logger.info("Processing Adzuna dispersion data...")
        
        with spark_manager as sm:
            spark = sm.get_session()
            dataframes = []
            
            for key in self.pays_europeens.keys():
                output_path = f"./data/bronze/azuna/Dispertion/{key}"
                try:
                    df = self.get_bronze_parquet(spark, output_path)
                    dataframes.append(df)
                    logger.info(f"Loaded dispersion data for {key}")
                except Exception as e:
                    logger.warning(f"Could not load dispersion data for {key}: {e}")
                    continue
            
            if not dataframes:
                logger.warning("No dispersion data found to process")
                return
            
            # Merge all dataframes
            merged_df = reduce(self.merge_multiple_dataframes_reduce, dataframes)
            
            # Data transformations
            drop_df = merged_df.drop('annee_insertion', 'mois_insertion', 'average_salary').dropDuplicates()
            rename_df = drop_df.withColumnRenamed('date', 'date') \
                              .withColumnRenamed('country', 'country_name') \
                              .withColumnRenamed('keyword', 'job_title') \
                              .withColumnRenamed('salary_range', 'salary_range') \
                              .withColumnRenamed('job_count', 'job_count')
            
            # Process each country
            final_dataframes = []
            for key in self.pays_europeens.keys():
                country_df = rename_df.withColumn('country_code', upper(lit(key)))
                final_dataframes.append(country_df)
            
            # Merge country dataframes
            if final_dataframes:
                final_df = reduce(self.merge_multiple_dataframes_reduce, final_dataframes)
                clean_df = self.supprimer_lignes_vides(final_df)
                clean_df = clean_df.dropna(subset=["salary_range", "job_count"])
                
                # Add processing metadata
                clean_df = clean_df.withColumn('processed_at', current_timestamp()) \
                                 .withColumn('data_quality_score', lit(80))
                
                return clean_df
            
        return None
    
    def process(self):
        """Main processing function"""
        logger.info("Starting Adzuna silver layer processing...")
        
        # Create table
        self.create_mysql_table()
        
        # Process both salary and dispersion data
        salary_df = self.process_salary_data()
        dispersion_df = self.process_dispersion_data()
        
        # Combine datasets if both exist
        if salary_df and dispersion_df:
            # Union both datasets (they should have similar schemas)
            combined_df = salary_df.union(dispersion_df)
            self.save_to_mysql(combined_df)
            logger.info("Successfully processed and saved combined Adzuna data")
        elif salary_df:
            self.save_to_mysql(salary_df)
            logger.info("Successfully processed and saved Adzuna salary data")
        elif dispersion_df:
            self.save_to_mysql(dispersion_df)
            logger.info("Successfully processed and saved Adzuna dispersion data")
        else:
            logger.warning("No data to process")

def main():
    """Main execution function"""
    processor = AdzunaSilverProcessor()
    processor.process()

if __name__ == "__main__":
    main()