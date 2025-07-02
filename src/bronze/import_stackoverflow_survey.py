import requests
import pandas as pd
import os
import zipfile
import time
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StackOverflowSurveyCollector:
    """Collector for StackOverflow Developer Survey data from their official site"""
    
    def __init__(self, output_dir: str = "../../data/bronze/stackoverflow_survey"):
        self.output_dir = output_dir
        self.base_url = "https://survey.stackoverflow.co/datasets/"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'DataPipeline-StackOverflow-Collector/1.0'
        })
        
        # Create output directory
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Standard CSV filename found in all survey zips
        self.csv_filename = 'survey_results_public.csv'
    
    def get_survey_url(self, year: int) -> str:
        """Generate the survey URL for a given year using the standard pattern"""
        return f"{self.base_url}stack-overflow-developer-survey-{year}.zip"
    
    def discover_available_surveys(self, start_year: int = 2020, end_year: int = None) -> List[int]:
        """Discover which survey years are available by testing URLs"""
        if end_year is None:
            end_year = datetime.now().year
        
        available_years = []
        
        for year in range(start_year, end_year + 1):
            url = self.get_survey_url(year)
            try:
                response = self.session.head(url, timeout=10)
                if response.status_code == 200:
                    available_years.append(year)
                    logger.info(f"Survey {year} is available")
                else:
                    logger.debug(f"Survey {year} not available (status: {response.status_code})")
            except Exception as e:
                logger.debug(f"Survey {year} not available (error: {e})")
        
        logger.info(f"Found {len(available_years)} available surveys: {available_years}")
        return available_years
    
    def download_survey_data(self, year: int) -> Optional[str]:
        """Download survey data for a specific year"""
        zip_url = self.get_survey_url(year)
        
        try:
            logger.info(f"Downloading StackOverflow {year} survey data from {zip_url}")
            
            # Test if URL exists first
            head_response = self.session.head(zip_url, timeout=10)
            if head_response.status_code != 200:
                logger.error(f"Survey data for year {year} not available (status: {head_response.status_code})")
                return None
            
            # Download the zip file
            response = self.session.get(zip_url, stream=True)
            response.raise_for_status()
            
            zip_path = os.path.join(self.output_dir, f"stackoverflow_survey_{year}.zip")
            
            # Save zip file
            with open(zip_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logger.info(f"Downloaded zip file: {zip_path}")
            
            # Extract and process zip contents
            csv_output_path = self._extract_csv_from_zip(zip_path, year)
            
            # Clean up zip file
            os.remove(zip_path)
            
            return csv_output_path
            
        except Exception as e:
            logger.error(f"Error downloading survey data for {year}: {e}")
            return None
    
    def _extract_csv_from_zip(self, zip_path: str, year: int) -> Optional[str]:
        """Extract CSV file from downloaded zip"""
        csv_output_path = os.path.join(self.output_dir, f"survey_results_public_{year}.csv")
        
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # List all files in zip for debugging
                all_files = zip_ref.namelist()
                logger.info(f"Files in zip: {all_files}")
                
                # Find the CSV file - it should be named 'survey_results_public.csv'
                csv_files = [f for f in all_files if f.endswith('.csv') and 'survey_results_public' in f.lower()]
                
                if not csv_files:
                    # Fallback: look for any CSV file
                    csv_files = [f for f in all_files if f.endswith('.csv')]
                
                if csv_files:
                    csv_file = csv_files[0]  # Take the first matching CSV
                    logger.info(f"Extracting CSV file: {csv_file}")
                    
                    # Extract the CSV file
                    zip_ref.extract(csv_file, self.output_dir)
                    
                    # Rename to standardized name if needed
                    extracted_path = os.path.join(self.output_dir, csv_file)
                    if extracted_path != csv_output_path:
                        os.rename(extracted_path, csv_output_path)
                    
                    logger.info(f"Extracted and renamed CSV to: {csv_output_path}")
                    
                    # Also extract schema file if available for future reference
                    schema_files = [f for f in all_files if 'schema' in f.lower() and f.endswith('.csv')]
                    if schema_files:
                        schema_file = schema_files[0]
                        zip_ref.extract(schema_file, self.output_dir)
                        schema_output = os.path.join(self.output_dir, f"survey_schema_{year}.csv")
                        os.rename(os.path.join(self.output_dir, schema_file), schema_output)
                        logger.info(f"Also extracted schema file: {schema_output}")
                    
                    return csv_output_path
                else:
                    logger.error(f"No CSV file found in {zip_path}")
                    return None
                    
        except Exception as e:
            logger.error(f"Error extracting CSV from {zip_path}: {e}")
            return None
    
    def load_and_process_csv(self, year: int, csv_path: str) -> Optional[pd.DataFrame]:
        """Load and process CSV data for a specific year"""
        try:
            logger.info(f"Loading survey data for {year} from {csv_path}")
            
            # Read CSV with pandas
            df = pd.read_csv(csv_path, low_memory=False)
            
            logger.info(f"Loaded {len(df)} records with {len(df.columns)} columns for year {year}")
            
            # Filter and standardize columns
            df_processed = self.filter_columns(df, year)
            
            return df_processed
            
        except Exception as e:
            logger.error(f"Error processing CSV for {year}: {e}")
            return None
    
    def _get_common_schema_mapping(self) -> Dict[str, Dict[str, Any]]:
        """
        Tech market focused schema mapping for StackOverflow survey data.
        Focuses on technology adoption, market trends, and career progression in tech.
        """
        return {
            # Core Demographics (relevant to tech market analysis)
            'country': {
                'patterns': ['Country'],
                'description': 'Country of residence - for geographic tech market analysis',
                'type': 'string',
                'required': True
            },
            'age': {
                'patterns': ['Age'],
                'description': 'Age group - for generational tech adoption patterns',
                'type': 'string',
                'required': False
            },
            
            # Professional Profile (tech market focus)
            'developer_type': {
                'patterns': ['DevType', 'DeveloperType', 'JobRoleInterest'],
                'description': 'Developer role/type - core to tech market segmentation',
                'type': 'string',
                'required': True
            },

            'education_level': {
                'patterns': ['EdLevel', 'FormalEducation', 'EducationLevel'],
                'description': 'Education level - background influence on tech adoption',
                'type': 'string',
                'required': False
            },
            
            # Compensation (market value indicators)
            'salary_usd': {
                'patterns': ['ConvertedCompYearly', 'ConvertedComp', 'ConvertedSalary', 'CompTotal'],
                'description': 'Annual salary in USD - market value of skills',
                'type': 'numeric',
                'required': False
            },
            
            # Technology Stack - Core to market analysis
            'languages_worked': {
                'patterns': ['LanguageHaveWorkedWith', 'LanguageWorkedWith', 'LanguagesWorkedWith'],
                'description': 'Programming languages used - current market adoption',
                'type': 'string',
                'required': True
            },
            'languages_want': {
                'patterns': ['LanguageWantToWorkWith', 'LanguageDesireNextYear'],
                'description': 'Languages developers want to use - future market trends',
                'type': 'string',
                'required': False
            },
            'databases_worked': {
                'patterns': ['DatabaseHaveWorkedWith', 'DatabaseWorkedWith'],
                'description': 'Databases used - data technology adoption',
                'type': 'string',
                'required': False
            },
            'platforms_worked': {
                'patterns': ['PlatformHaveWorkedWith', 'PlatformWorkedWith'],
                'description': 'Platforms used - infrastructure and deployment trends',
                'type': 'string',
                'required': False
            },
            'webframes_worked': {
                'patterns': ['WebframeHaveWorkedWith', 'WebFrameWorkedWith', 'FrameworkWorkedWith'],
                'description': 'Web frameworks used - frontend/backend technology trends',
                'type': 'string',
                'required': False
            },
            'tools_tech_worked': {
                'patterns': ['ToolsTechHaveWorkedWith', 'ToolsTechWorkedWith'],
                'description': 'Development tools and technologies - tooling adoption',
                'type': 'string',
                'required': False
            },
            
            # Work Environment (tech market evolution)
            'main_branch': {
                'patterns': ['MainBranch', 'Hobbyist'],
                'description': 'Professional vs hobby coding - market participation type',
                'type': 'string',
                'required': False
            },
        }
    
    def _find_column_by_patterns(self, df: pd.DataFrame, patterns: List[str]) -> Optional[str]:
        """Find a column by trying multiple pattern matches with intelligent fallbacks"""
        for pattern in patterns:
            # Try exact match first
            if pattern in df.columns:
                return pattern
            
            # Try case-insensitive exact match
            for col in df.columns:
                if col.lower() == pattern.lower():
                    return col
            
            # Try partial match (pattern in column name)
            for col in df.columns:
                if pattern.lower() in col.lower():
                    return col
        
        return None
    
    def filter_columns(self, df: pd.DataFrame, year: int) -> pd.DataFrame:
        """
        Universal column mapping that works across all StackOverflow survey years.
        Uses comprehensive schema-based pattern matching to extract the most important data.
        """
        logger.info(f"Processing {len(df.columns)} columns for year {year}")
        logger.debug(f"Available columns: {[col for col in df.columns if not col.startswith('Q')][:20]}...")
        
        schema_mapping = self._get_common_schema_mapping()
        processed_data = {}
        found_columns = {}
        missing_required = []
        
        # Apply schema-based column mapping
        for field_name, field_config in schema_mapping.items():
            patterns = field_config['patterns']
            is_required = field_config.get('required', False)
            
            found_col = self._find_column_by_patterns(df, patterns)
            
            if found_col:
                processed_data[field_name] = df[found_col]
                found_columns[field_name] = found_col
                logger.debug(f"✓ Mapped '{field_name}' to '{found_col}'")
            else:
                processed_data[field_name] = None
                if is_required:
                    missing_required.append(field_name)
                logger.debug(f"✗ No column found for '{field_name}' using patterns: {patterns}")
        
        # Add metadata
        processed_data['survey_year'] = year
        processed_data['collected_at'] = datetime.now().isoformat()
        
        # Create new DataFrame
        df_processed = pd.DataFrame(processed_data)
        
        # Log mapping results
        total_fields = len(schema_mapping)
        found_fields = len(found_columns)
        mapping_percentage = (found_fields / total_fields) * 100
        
        logger.info(f"Schema mapping results for {year}:")
        logger.info(f"  ✓ Found: {found_fields}/{total_fields} fields ({mapping_percentage:.1f}%)")
        logger.info(f"  ✓ Core fields mapped: {list(found_columns.keys())[:10]}...")
        
        if missing_required:
            logger.warning(f"  ✗ Missing required fields: {missing_required}")
        
        logger.info(f"  → Processed {len(df_processed):,} records with {len(df_processed.columns)} standardized columns")
        
        return df_processed
    
    def save_to_parquet(self, df: pd.DataFrame, year: int):
        """Save processed DataFrame to parquet format"""
        if df.empty:
            logger.warning(f"No data to save for year {year}")
            return
        
        # Create timestamped filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"stackoverflow_survey_{year}_{timestamp}.parquet"
        output_path = os.path.join(self.output_dir, filename)
        
        # Optimize data types for parquet
        df_optimized = df.copy()
        
        # Convert numeric columns
        numeric_columns = ['salary', 'survey_year']
        for col in numeric_columns:
            if col in df_optimized.columns:
                df_optimized[col] = pd.to_numeric(df_optimized[col], errors='coerce')
        
        # Convert datetime columns
        df_optimized['collected_at'] = pd.to_datetime(df_optimized['collected_at'])
        
        # Save to parquet
        df_optimized.to_parquet(
            output_path,
            engine='pyarrow',
            compression='snappy',
            index=False
        )
        
        logger.info(f"Saved {len(df_optimized)} records to {output_path}")
    
    def collect_survey_data(self, years: List[int] = None) -> Dict[int, pd.DataFrame]:
        """Main method to collect and process survey data"""
        if years is None:
            years = list(self.survey_years.keys())
        
        all_data = {}
        
        for year in years:
            logger.info(f"\n=== Processing StackOverflow {year} Survey Data ===")
            
            # Check if CSV already exists
            csv_path = os.path.join(self.output_dir, f"survey_results_public_{year}.csv")
            
            if not os.path.exists(csv_path):
                # Download the data
                csv_path = self.download_survey_data(year)
                if not csv_path:
                    logger.error(f"Failed to download data for {year}")
                    continue
            else:
                logger.info(f"Using existing CSV: {csv_path}")
            
            # Process the data
            df = self.load_and_process_csv(year, csv_path)
            
            if df is not None:
                all_data[year] = df
                
                # Save to parquet
                self.save_to_parquet(df, year)
                
                # Print summary
                logger.info(f"Summary for {year}:")
                logger.info(f"  Records: {len(df)}")
                logger.info(f"  Columns: {list(df.columns)}")

            # Rate limiting
            time.sleep(2)
        
        return all_data

def main():
    """Main execution function"""
    logger.info("Starting StackOverflow Survey data collection...")
    
    # Initialize collector
    collector = StackOverflowSurveyCollector()
    
    # Collect data for recent years (adjust as needed)
    years_to_collect = [2024, 2023, 2022, 2021]  # Most recent years
    
    # Collect and process data
    survey_data = collector.collect_survey_data(years_to_collect)
    
    # Print final summary
    logger.info(f"\nData collection completed!")
    logger.info(f"Successfully processed {len(survey_data)} survey years")
    
    for year, df in survey_data.items():
        logger.info(f"  {year}: {len(df)} records")

if __name__ == "__main__":
    main()