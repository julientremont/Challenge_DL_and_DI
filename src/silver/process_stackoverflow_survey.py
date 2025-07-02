import pandas as pd
import os
import glob
import re
from datetime import datetime
from typing import List, Dict, Any, Optional
import logging

from src.utils.sqlmanager import sql_manager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StackOverflowSurveySilverProcessor:
    def __init__(self, bronze_path: str = "../../data/bronze/stackoverflow_survey"):
        self.bronze_path = bronze_path
        self.table_name = "stackoverflow_survey_silver"
        
    def load_bronze_data(self) -> pd.DataFrame:
        """Load all parquet files from bronze layer"""
        parquet_files = glob.glob(os.path.join(self.bronze_path, "*.parquet"))
        
        if not parquet_files:
            logger.warning(f"No parquet files found in {self.bronze_path}")
            return pd.DataFrame()
        
        logger.info(f"Found {len(parquet_files)} parquet files to process")
        
        dfs = []
        for file in parquet_files:
            try:
                df = pd.read_parquet(file)
                dfs.append(df)
                logger.info(f"Loaded {len(df)} records from {os.path.basename(file)}")
            except Exception as e:
                logger.error(f"Error loading {file}: {e}")
                
        if not dfs:
            return pd.DataFrame()
            
        combined_df = pd.concat(dfs, ignore_index=True)
        logger.info(f"Combined {len(combined_df)} total records from bronze layer")
        
        return combined_df
    
    def clean_and_normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and normalize the StackOverflow survey data"""
        if df.empty:
            return df
            
        logger.info("Starting data cleaning and normalization...")
        
        # Remove duplicates based on survey_year and similar responses
        initial_count = len(df)
        df = df.drop_duplicates().copy()
        logger.info(f"Removed {initial_count - len(df)} duplicate records")
        
        # Clean and normalize categorical fields
        df = self._clean_categorical_fields(df)
        
        # Process salary data
        df = self._process_salary_data(df)
        
        # Process technologies
        df = self._process_technologies(df)
        
        # Clean country data
        df = self._clean_country_data(df)
        
        # Add processing metadata
        df['processed_at'] = pd.Timestamp.now()
        df['data_quality_score'] = self._calculate_quality_score(df)
        
        logger.info(f"Cleaned dataset contains {len(df)} records")
        return df
    
    def _clean_categorical_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean categorical fields focused on tech market analysis"""
        # Developer type normalization (core field)
        if 'developer_type' in df.columns:
            df['primary_role'] = df['developer_type'].str.split(';').str[0]  # Take first role
            df['primary_role'] = df['primary_role'].str.strip()
            df['primary_role'] = df['primary_role'].fillna('unknown')
            
            # Standardize common role names
            role_mapping = {
                'Full-stack developer': 'fullstack',
                'Backend developer': 'backend', 
                'Frontend developer': 'frontend',
                'Data scientist or machine learning specialist': 'data_scientist',
                'DevOps specialist': 'devops',
                'Mobile developer': 'mobile',
                'Data engineer': 'data_engineer',
                'Software Engineer': 'software_engineer',
                'Engineering manager': 'engineering_manager'
            }
            df['primary_role'] = df['primary_role'].replace(role_mapping)
        
        # Education normalization
        if 'education_level' in df.columns:
            df['education_normalized'] = df['education_level'].str.strip()
            
            # Categorize education levels
            education_mapping = {
                r'.*Bachelor.*': 'bachelor',
                r'.*Master.*': 'master', 
                r'.*PhD.*|.*doctorate.*': 'phd',
                r'.*Associate.*': 'associate',
                r'.*high school.*|.*secondary.*': 'high_school',
                r'.*bootcamp.*': 'bootcamp',
                r'.*self.*taught.*|.*autodidact.*': 'self_taught'
            }
            
            df['education_normalized'] = 'other'  # default
            for pattern, level in education_mapping.items():
                mask = df['education_level'].str.contains(pattern, case=False, na=False)
                df.loc[mask, 'education_normalized'] = level
            
            df['education_normalized'] = df['education_normalized'].fillna('unknown')
        
        return df
    
    def _process_salary_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process and validate salary data"""
        if 'salary_usd' not in df.columns:
            df['salary_usd_cleaned'] = None
            df['salary_range'] = 'unknown'
            return df
        
        # Convert to numeric and handle outliers
        df['salary_usd_cleaned'] = pd.to_numeric(df['salary_usd'], errors='coerce')
        
        # Remove extreme outliers (less than $1000 or more than $1M)
        df.loc[(df['salary_usd_cleaned'] < 1000) | (df['salary_usd_cleaned'] > 1000000), 'salary_usd_cleaned'] = None
        
        # Create salary ranges for tech market analysis
        df['salary_range'] = 'unknown'
        df.loc[df['salary_usd_cleaned'] < 50000, 'salary_range'] = 'under_50k'
        df.loc[(df['salary_usd_cleaned'] >= 50000) & (df['salary_usd_cleaned'] < 80000), 'salary_range'] = '50k_80k'
        df.loc[(df['salary_usd_cleaned'] >= 80000) & (df['salary_usd_cleaned'] < 120000), 'salary_range'] = '80k_120k'
        df.loc[(df['salary_usd_cleaned'] >= 120000) & (df['salary_usd_cleaned'] < 180000), 'salary_range'] = '120k_180k'
        df.loc[df['salary_usd_cleaned'] >= 180000, 'salary_range'] = 'over_180k'
        
        return df

    def _process_technologies(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process technology stack data into a single consolidated column"""
        technology_parts = []
        
        # Collect all technology fields
        tech_fields = ['languages_worked', 'databases_worked', 'platforms_worked', 'webframes_worked', 'tools_tech_worked']
        
        for _, row in df.iterrows():
            techs = []
            
            for field in tech_fields:
                if field in df.columns and pd.notna(row[field]) and str(row[field]).lower() != 'nan':
                    # Split by semicolon and clean each technology
                    field_techs = [tech.strip() for tech in str(row[field]).split(';') if tech.strip()]
                    techs.extend(field_techs)
            
            # Join all technologies with semicolon delimiter
            technology_used = ';'.join(techs) if techs else None
            technology_parts.append(technology_used)
        
        # Add the consolidated technology column
        df['technologies_used'] = technology_parts
        
        # Extract primary language (first language mentioned)
        if 'languages_worked' in df.columns:
            df['primary_language'] = df['languages_worked'].str.split(';').str[0]
            df['primary_language'] = df['primary_language'].str.strip()
            df['primary_language'] = df['primary_language'].fillna('unknown')
        else:
            df['primary_language'] = 'unknown'
        
        return df
    
    def _clean_country_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and normalize country data"""
        if 'country' not in df.columns:
            df['country_normalized'] = 'unknown'
            return df
        
        df['country_normalized'] = df['country'].str.strip()
        df['country_normalized'] = df['country_normalized'].fillna('unknown')
        
        # Normalize common country name variations
        country_mapping = {
            'United States': 'United States',
            'United States of America': 'United States',
            'USA': 'United States',
            'UK': 'United Kingdom',
            'United Kingdom of Great Britain and Northern Ireland': 'United Kingdom'
        }
        
        df['country_normalized'] = df['country_normalized'].replace(country_mapping)
        
        return df
    
    
    def _calculate_quality_score(self, df: pd.DataFrame) -> pd.Series:
        """Calculate a data quality score for each response focused on tech market data"""
        score = 100  # Start with perfect score
        
        quality_score = pd.Series([score] * len(df), index=df.index)
        
        # Deduct for missing key tech market fields
        quality_score -= df.get('salary_usd_cleaned', pd.Series([None]*len(df))).isna() * 10
        quality_score -= df.get('technologies_used', pd.Series([None]*len(df))).isna() * 20
        quality_score -= df.get('country_normalized', pd.Series(['unknown']*len(df))).eq('unknown') * 10
        quality_score -= df.get('primary_role', pd.Series(['unknown']*len(df))).eq('unknown') * 15
        
        # Bonus for complete tech stack information
        has_complete_stack = df.get('technologies_used', pd.Series([None]*len(df))).notna()
        quality_score += has_complete_stack * 10
        
        return quality_score.clip(lower=0, upper=100)
    
    def create_mysql_table(self):
        """Create MySQL table for tech market focused silver layer data"""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            survey_year INT NOT NULL,
            country_normalized VARCHAR(100),
            primary_role VARCHAR(255),
            education_normalized VARCHAR(100),
            salary_usd_cleaned DECIMAL(10,2),
            salary_range VARCHAR(50),
            
            -- Technology stack
            technologies_used TEXT,
            primary_language VARCHAR(100),
            
            -- Metadata
            processed_at DATETIME NOT NULL,
            data_quality_score TINYINT DEFAULT 0,
            
            -- Indexes for tech market analysis
            INDEX idx_survey_year (survey_year),
            INDEX idx_country (country_normalized),
            INDEX idx_role (primary_role),
            INDEX idx_salary_range (salary_range),
            INDEX idx_primary_language (primary_language),
            INDEX idx_quality_score (data_quality_score DESC),
            FULLTEXT INDEX ft_technologies (technologies_used)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        
        with sql_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(create_table_sql)
            conn.commit()
            logger.info(f"Created/verified table: {self.table_name}")
    
    def save_to_mysql(self, df: pd.DataFrame):
        """Save cleaned data to MySQL"""
        if df.empty:
            logger.warning("No data to save to MySQL")
            return
            
        # Select columns for database
        db_columns = [
            'survey_year', 'country_normalized', 'primary_role', 'education_normalized',
            'salary_usd_cleaned', 'salary_range', 'technologies_used', 'primary_language',
            'processed_at', 'data_quality_score'
        ]
        
        # Prepare data for insertion
        df_db = df[db_columns].copy()
        
        # Handle NaN values - replace with None for MySQL NULL
        df_db = df_db.where(pd.notnull(df_db), None)
        
        # Handle string columns - ensure no 'nan' strings
        string_columns = ['country_normalized', 'primary_role', 'education_normalized', 
                         'salary_range', 'primary_language', 'technologies_used']
        for col in string_columns:
            if col in df_db.columns:
                df_db[col] = df_db[col].apply(lambda x: None if pd.isna(x) or str(x).lower() == 'nan' else str(x))
        
        # Handle numeric columns
        numeric_columns = ['salary_usd_cleaned', 'data_quality_score']
        for col in numeric_columns:
            if col in df_db.columns:
                df_db[col] = df_db[col].apply(lambda x: None if pd.isna(x) else x)
        
        # Convert to list of tuples
        data_tuples = []
        for _, row in df_db.iterrows():
            tuple_row = []
            for val in row:
                if pd.isna(val) or str(val).lower() == 'nan':
                    tuple_row.append(None)
                else:
                    tuple_row.append(val)
            data_tuples.append(tuple(tuple_row))
        
        # Insert query
        placeholders = ', '.join(['%s'] * len(db_columns))
        insert_sql = f"""
        INSERT INTO {self.table_name} (
            {', '.join(db_columns)}
        ) VALUES ({placeholders})
        """
        
        # Use enhanced bulk insert with batching
        affected_rows = sql_manager.execute_bulk_insert(insert_sql, data_tuples, batch_size=500)
        logger.info(f"Successfully inserted {affected_rows} records into {self.table_name}")
    
    def process(self):
        """Main processing function"""
        logger.info("Starting StackOverflow survey silver layer processing...")
        
        # Load bronze data
        bronze_df = self.load_bronze_data()
        if bronze_df.empty:
            logger.warning("No data found in bronze layer")
            return
        
        # Clean and normalize
        silver_df = self.clean_and_normalize(bronze_df)
        
        # Create table and save to MySQL
        self.create_mysql_table()
        self.save_to_mysql(silver_df)
        
        # Print summary
        self._print_summary(silver_df)
        
        logger.info("StackOverflow survey silver layer processing completed!")
    
def main():
    """Main execution function"""
    processor = StackOverflowSurveySilverProcessor()
    processor.process()

if __name__ == "__main__":
    main()