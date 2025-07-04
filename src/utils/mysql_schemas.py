"""
Centralized MySQL schema management for silver layer tables.
This module provides a unified interface for creating and managing MySQL tables
across all silver layer processors.
"""

import logging
from typing import Dict, List, Optional, Any, Tuple
from src.utils.sqlmanager import sql_manager

logger = logging.getLogger(__name__)

class MySQLSchemaManager:
    """Centralized MySQL schema manager for all silver layer tables"""
    
    def __init__(self):
        self.schema_definitions = {
            'github_repos_silver': self._get_github_repos_schema(),
            'stackoverflow_survey_silver': self._get_stackoverflow_survey_schema(),
            'trends_silver': self._get_trends_schema(),
            'adzuna_jobs_silver': self._get_adzuna_jobs_schema(),
            'eurotechjobs_silver': self._get_eurotechjobs_schema(),
            'jobicy_silver': self._get_jobicy_schema()
        }
    
    def _get_github_repos_schema(self) -> Dict:
        """GitHub repositories silver layer schema"""
        return {
            'table_name': 'github_repos_silver',
            'columns': [
                'id BIGINT PRIMARY KEY',
                'name VARCHAR(255) NOT NULL',
                'name_cleaned VARCHAR(255) NOT NULL',
                'technology_normalized VARCHAR(100) NOT NULL',
                'search_type VARCHAR(50) NOT NULL',
                'stars_count INT NOT NULL DEFAULT 0',
                'forks_count INT NOT NULL DEFAULT 0',
                'watchers_count INT NOT NULL DEFAULT 0',
                'open_issues_count INT NOT NULL DEFAULT 0',
                'created_at_cleaned DATETIME NOT NULL',
                'collected_at_cleaned DATETIME NOT NULL',
                'popularity_score DECIMAL(12,2) NOT NULL DEFAULT 0',
                'days_since_creation INT NOT NULL DEFAULT 0',
                'activity_score DECIMAL(12,2) NOT NULL DEFAULT 0',
                "activity_level ENUM('low', 'medium', 'high') NOT NULL DEFAULT 'low'",
                'processed_at DATETIME NOT NULL',
                'data_quality_score TINYINT NOT NULL DEFAULT 0'
            ],
            'indexes': [
                'INDEX idx_technology (technology_normalized)',
                'INDEX idx_activity_level (activity_level)',
                'INDEX idx_created_at (created_at_cleaned)',
                'INDEX idx_popularity (popularity_score DESC)',
                'INDEX idx_quality (data_quality_score DESC)'
            ],
            'engine': 'InnoDB',
            'charset': 'utf8mb4',
            'collate': 'utf8mb4_unicode_ci'
        }
    
    def _get_stackoverflow_survey_schema(self) -> Dict:
        """StackOverflow survey silver layer schema"""
        return {
            'table_name': 'stackoverflow_survey_silver',
            'columns': [
                'id INT AUTO_INCREMENT PRIMARY KEY',
                'survey_year INT NOT NULL',
                'country_normalized VARCHAR(100)',
                'primary_role VARCHAR(255)',
                'education_normalized VARCHAR(100)',
                'salary_usd_cleaned DECIMAL(10,2)',
                'salary_range VARCHAR(50)',
                'technologies_used TEXT',
                'primary_language VARCHAR(100)',
                'processed_at DATETIME NOT NULL',
                'data_quality_score TINYINT DEFAULT 0'
            ],
            'indexes': [
                'INDEX idx_survey_year (survey_year)',
                'INDEX idx_country (country_normalized)',
                'INDEX idx_role (primary_role)',
                'INDEX idx_salary_range (salary_range)',
                'INDEX idx_primary_language (primary_language)',
                'INDEX idx_quality_score (data_quality_score DESC)',
                'FULLTEXT INDEX ft_technologies (technologies_used)'
            ],
            'engine': 'InnoDB',
            'charset': 'utf8mb4',
            'collate': 'utf8mb4_unicode_ci'
        }
    
    def _get_trends_schema(self) -> Dict:
        """Google Trends silver layer schema"""
        return {
            'table_name': 'trends_silver',
            'columns': [
                'id INT AUTO_INCREMENT PRIMARY KEY',
                'date DATE NOT NULL',
                'keyword VARCHAR(100) NOT NULL',
                'country_code VARCHAR(5) NOT NULL',
                'search_frequency INT NOT NULL DEFAULT 0',
                'processed_at DATETIME NOT NULL',
                'data_quality_score TINYINT DEFAULT 0'
            ],
            'indexes': [
                'INDEX idx_date (date)',
                'INDEX idx_keyword (keyword)',
                'INDEX idx_country_code (country_code)',
                'INDEX idx_search_frequency (search_frequency DESC)',
                'INDEX idx_date_keyword (date, keyword)',
                'INDEX idx_quality_score (data_quality_score DESC)'
            ],
            'engine': 'InnoDB',
            'charset': 'utf8mb4',
            'collate': 'utf8mb4_unicode_ci'
        }
    
    def _get_adzuna_jobs_schema(self) -> Dict:
        """Adzuna jobs silver layer schema"""
        return {
            'table_name': 'adzuna_jobs_silver',
            'columns': [
                'id INT AUTO_INCREMENT PRIMARY KEY',
                'date DATE NOT NULL',
                'job_title VARCHAR(255) NOT NULL',
                'country_code VARCHAR(5) NOT NULL',
                'average_salary DECIMAL(10,2)',
                'salary_range INT NOT NULL DEFAULT 0',
                'processed_at DATETIME NOT NULL',
                'data_quality_score TINYINT DEFAULT 0'
            ],
            'indexes': [
                'INDEX idx_date (date)',
                'INDEX idx_job_title (job_title)',
                'INDEX idx_country_code (country_code)',
                'INDEX idx_salary_range (salary_range)',
                'INDEX idx_date_job (date, job_title)',
                'INDEX idx_quality_score (data_quality_score DESC)'
            ],
            'engine': 'InnoDB',
            'charset': 'utf8mb4',
            'collate': 'utf8mb4_unicode_ci'
        }
    
    def _get_eurotechjobs_schema(self) -> Dict:
        """EuroTechJobs silver layer schema"""
        return {
            'table_name': 'eurotechjobs_silver',
            'columns': [
                'id INT AUTO_INCREMENT PRIMARY KEY',
                'job_title VARCHAR(500)',
                'job_title_category VARCHAR(255)',
                'company VARCHAR(255)',
                'location VARCHAR(255)',
                'country_code VARCHAR(3)',
                'technologies TEXT',
                'primary_technology VARCHAR(100)',
                'tech_count INT DEFAULT 0',
                'job_type VARCHAR(50)',
                'category VARCHAR(100)',
                'url VARCHAR(1000)',
                'processed_at DATETIME NOT NULL',
                'data_quality_score TINYINT DEFAULT 0'
            ],
            'indexes': [
                'INDEX idx_country (country_code)',
                'INDEX idx_job_title (job_title(255))',
                'INDEX idx_job_title_category (job_title_category)',
                'INDEX idx_company (company)',
                'INDEX idx_primary_tech (primary_technology)',
                'INDEX idx_job_type (job_type)',
                'INDEX idx_category (category)',
                'INDEX idx_quality_score (data_quality_score DESC)',
                'INDEX idx_processed_at (processed_at DESC)',
                'FULLTEXT INDEX ft_technologies (technologies)',
                'UNIQUE KEY unique_job_url (url(767))'
            ],
            'engine': 'InnoDB',
            'charset': 'utf8mb4',
            'collate': 'utf8mb4_unicode_ci'
        }
    
    def _get_jobicy_schema(self) -> Dict:
        """Jobicy jobs silver layer schema"""
        return {
            'table_name': 'jobicy_silver',
            'columns': [
                'id INT AUTO_INCREMENT PRIMARY KEY',
                'job_id VARCHAR(255) UNIQUE NOT NULL',
                'job_title VARCHAR(500)',
                'company_name VARCHAR(255)',
                'company_logo VARCHAR(1000)',
                'job_location VARCHAR(255)',
                'country_code VARCHAR(5)',
                'job_level VARCHAR(100)',
                'job_level_standardized VARCHAR(50)',
                'job_type VARCHAR(100)',
                'job_type_standardized VARCHAR(50)',
                'publication_date VARCHAR(100)',
                'job_description TEXT',
                'job_url VARCHAR(1000)',
                'job_tags TEXT',
                'job_industry VARCHAR(255)',
                'salary_min DECIMAL(10,2)',
                'salary_max DECIMAL(10,2)',
                'salary_avg DECIMAL(10,2)',
                'salary_currency VARCHAR(10)',
                'has_salary_info BOOLEAN DEFAULT FALSE',
                'tags_count INT DEFAULT 0',
                'processed_date DATETIME NOT NULL',
                'data_quality_score TINYINT DEFAULT 0'
            ],
            'indexes': [
                'UNIQUE KEY unique_job_id (job_id)',
                'INDEX idx_country_code (country_code)',
                'INDEX idx_job_title (job_title(255))',
                'INDEX idx_company (company_name)',
                'INDEX idx_job_level (job_level_standardized)',
                'INDEX idx_job_type (job_type_standardized)',
                'INDEX idx_salary_range (salary_min, salary_max)',
                'INDEX idx_has_salary (has_salary_info)',
                'INDEX idx_processed_date (processed_date DESC)',
                'INDEX idx_quality_score (data_quality_score DESC)',
                'FULLTEXT INDEX ft_job_tags (job_tags)',
                'FULLTEXT INDEX ft_job_description (job_description)'
            ],
            'engine': 'InnoDB',
            'charset': 'utf8mb4',
            'collate': 'utf8mb4_unicode_ci'
        }
    
    def _build_create_table_sql(self, schema: Dict) -> str:
        """Build CREATE TABLE SQL statement from schema definition"""
        table_name = schema['table_name']
        columns = schema['columns']
        indexes = schema.get('indexes', [])
        engine = schema.get('engine', 'InnoDB')
        charset = schema.get('charset', 'utf8mb4')
        collate = schema.get('collate', 'utf8mb4_unicode_ci')
        
        # Build column definitions
        column_definitions = ',\n            '.join(columns)
        
        # Build index definitions
        index_definitions = ''
        if indexes:
            index_definitions = ',\n            ' + ',\n            '.join(indexes)
        
        # Build complete SQL
        sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {column_definitions}{index_definitions}
        ) ENGINE={engine} DEFAULT CHARSET={charset} COLLATE={collate}
        """
        
        return sql
    
    def create_table(self, table_name: str) -> bool:
        """Create a specific table"""
        if table_name not in self.schema_definitions:
            logger.error(f"Schema definition not found for table: {table_name}")
            return False
        
        try:
            schema = self.schema_definitions[table_name]
            create_sql = self._build_create_table_sql(schema)
            
            with sql_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(create_sql)
                conn.commit()
                logger.info(f"Created/verified table: {table_name}")
                return True
                
        except Exception as e:
            logger.error(f"Error creating table {table_name}: {e}")
            return False
    
    def create_all_tables(self) -> bool:
        """Create all defined tables"""
        success = True
        for table_name in self.schema_definitions.keys():
            if not self.create_table(table_name):
                success = False
        return success
    
    def drop_table(self, table_name: str) -> bool:
        """Drop a specific table"""
        try:
            drop_sql = f"DROP TABLE IF EXISTS {table_name}"
            with sql_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(drop_sql)
                conn.commit()
                logger.info(f"Dropped table: {table_name}")
                return True
                
        except Exception as e:
            logger.error(f"Error dropping table {table_name}: {e}")
            return False
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists"""
        try:
            check_sql = """
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = DATABASE() AND table_name = %s
            """
            with sql_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(check_sql, (table_name,))
                result = cursor.fetchone()
                return result[0] > 0
                
        except Exception as e:
            logger.error(f"Error checking table existence {table_name}: {e}")
            return False
    
    def get_table_schema(self, table_name: str) -> Optional[Dict]:
        """Get schema definition for a table"""
        return self.schema_definitions.get(table_name)
    
    def get_table_columns(self, table_name: str) -> List[str]:
        """Get column names for a table"""
        schema = self.get_table_schema(table_name)
        if not schema:
            return []
        
        columns = []
        for column_def in schema['columns']:
            # Extract column name from definition (before first space)
            column_name = column_def.split()[0]
            columns.append(column_name)
        
        return columns
    
    def get_insert_columns(self, table_name: str) -> List[str]:
        """Get column names for INSERT statements (excluding AUTO_INCREMENT)"""
        columns = self.get_table_columns(table_name)
        schema = self.get_table_schema(table_name)
        
        if not schema:
            return columns
        
        # Filter out AUTO_INCREMENT columns
        insert_columns = []
        for i, column_def in enumerate(schema['columns']):
            if 'AUTO_INCREMENT' not in column_def.upper():
                insert_columns.append(columns[i])
        
        return insert_columns

# Global instance
schema_manager = MySQLSchemaManager()

# Convenience functions
def create_table(table_name: str) -> bool:
    """Create a specific table"""
    return schema_manager.create_table(table_name)

def create_all_tables() -> bool:
    """Create all defined tables"""
    return schema_manager.create_all_tables()

def table_exists(table_name: str) -> bool:
    """Check if a table exists"""
    return schema_manager.table_exists(table_name)

def get_table_schema(table_name: str) -> Optional[Dict]:
    """Get schema definition for a table"""
    return schema_manager.get_table_schema(table_name)

def get_insert_columns(table_name: str) -> List[str]:
    """Get column names for INSERT statements"""
    return schema_manager.get_insert_columns(table_name)

def spark_df_to_sql_tuples(spark_df, table_name: str) -> Tuple[List[str], List[tuple]]:
    """
    Convert Spark DataFrame to format compatible with SQLManager.execute_bulk_insert.
    
    Args:
        spark_df: Spark DataFrame to convert
        table_name: Name of the target table (for column mapping)
        
    Returns:
        Tuple of (column_names, data_tuples) for use with execute_bulk_insert
    """
    try:
        # Get the target table columns (excluding AUTO_INCREMENT)
        insert_columns = get_insert_columns(table_name)
        
        # Select only the columns that exist in both the DataFrame and target table
        available_columns = spark_df.columns
        selected_columns = [col for col in insert_columns if col in available_columns]
        
        if not selected_columns:
            logger.warning(f"No matching columns found between DataFrame and table {table_name}")
            return [], []
        
        # Select the columns we need and collect the data
        df_selected = spark_df.select(*selected_columns)
        
        # Convert to list of tuples
        data_rows = df_selected.collect()
        data_tuples = [tuple(row) for row in data_rows]
        
        logger.info(f"Converted Spark DataFrame to {len(data_tuples)} tuples with {len(selected_columns)} columns")
        return selected_columns, data_tuples
        
    except Exception as e:
        logger.error(f"Error converting Spark DataFrame to SQL tuples: {e}")
        raise

def save_spark_df_to_mysql(spark_df, table_name: str, batch_size: int = 1000) -> int:
    try:
        # Convert DataFrame to SQL format
        columns, data_tuples = spark_df_to_sql_tuples(spark_df, table_name)
        
        if not data_tuples:
            logger.warning("No data to save to MySQL")
            return 0
        
        # Build INSERT SQL
        placeholders = ', '.join(['%s'] * len(columns))
        insert_sql = f"""
        INSERT INTO {table_name} (
            {', '.join(columns)}
        ) VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE
            {', '.join([f'{col} = VALUES({col})' for col in columns if col != 'id'])}
        """
        
        # Execute bulk insert
        affected_rows = sql_manager.execute_bulk_insert(insert_sql, data_tuples, batch_size=batch_size)
        logger.info(f"Successfully saved {affected_rows} rows to {table_name}")
        
        return affected_rows
        
    except Exception as e:
        logger.error(f"Error saving Spark DataFrame to MySQL: {e}")
        raise