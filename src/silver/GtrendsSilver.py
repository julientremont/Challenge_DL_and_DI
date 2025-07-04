import sys
import os
import logging
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

class GTrendsSilverProcessor:
    """Google Trends silver layer processor using centralized schema and SQLManager"""
    
    def __init__(self):
        self.table_name = "trends_silver"
        self.keywords_techs = [
            # Langages de programmation populaires
            'python', 'javascript', 'java', 'c++', 'c#', 'php', 'ruby', 'go', 'rust', 'kotlin',
            'swift', 'typescript', 'scala', 'perl', 'r programming', 'matlab', 'dart', 'elixir',
            
            # Frameworks et bibliothèques web
            'react', 'angular', 'django',
        ]
        
        self.country_codes = ["FR", "AT", "BE", "CH", "DE", "ES", "GB", "IT", "NL", "PL"]

    def supprimer_lignes_vides(self, df, seuil_pct=0.7):
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
    def process(self):
        """Main processing function for Google Trends data"""
        logger.info("Starting Google Trends silver layer processing...")
        
        # Create table
        self.create_mysql_table()
        
        with spark_manager as sm:
            spark = sm.get_session()
            dataframes = []
            
            # Process each country and keyword combination
            for country_code in self.country_codes:
                for keyword in self.keywords_techs:
                    output_path = f"./data/bronze/gtrends/{country_code}/{keyword}"
                    try:
                        df = self.get_bronze_parquet(spark, output_path)
                        # Add country information to the dataframe
                        df_with_country = df.withColumn('country_code', lit(country_code))
                        dataframes.append(df_with_country)
                        logger.info(f"Loaded data for {country_code} - {keyword}")
                    except Exception as e:
                        logger.warning(f"Could not load data for {country_code} - {keyword}: {e}")
                        continue
            
            if not dataframes:
                logger.warning("No data found to process")
                return
            
            # Merge all dataframes
            merged_df = reduce(self.merge_multiple_dataframes_reduce, dataframes)
            
            # Data transformations
            drop_df = merged_df.drop('isPartial', 'annee_insert', 'mois_insert', 'jour_insert') \
                              .dropDuplicates()
            
            rename_df = drop_df.withColumnRenamed('date', 'date') \
                              .withColumnRenamed('search_frequency', 'search_frequency') \
                              .withColumnRenamed('keyword', 'keyword')
            
            # Filter out null search frequencies
            dropnull_df = rename_df.dropna(subset=["search_frequency"])
            
            # Remove rows with too many nulls
            clean_df = self.supprimer_lignes_vides(dropnull_df)
            
            # Add processing metadata
            final_df = clean_df.withColumn('processed_at', current_timestamp()) \
                              .withColumn('data_quality_score', lit(90))
            
            # Save to MySQL
            self.save_to_mysql(final_df)
            
            logger.info("Google Trends silver layer processing completed!")

def main():
    """Main execution function"""
    processor = GTrendsSilverProcessor()
    processor.process()

if __name__ == "__main__":
    main()

