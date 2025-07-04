#!/usr/bin/env python3
"""
Main pipeline runner for Challenge DL & DI project
Orchestrates the complete data pipeline from bronze to gold layer
"""

import os
import sys
import logging
import argparse
from datetime import datetime
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).resolve().parent
sys.path.insert(0, str(project_root))

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'pipeline_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_bronze_layer(sources_config):
    """Execute bronze layer data collection"""
    logger.info("🔹 Starting Bronze Layer Data Collection")
    
    # Google Trends
    if sources_config.get('gtrends', True):
        try:
            logger.info("📊 Collecting Google Trends data...")
            from src.bronze.ImportGtrends import main as gtrends_main
            gtrends_main()
            logger.info("✅ Google Trends collection completed")
        except Exception as e:
            logger.error(f"❌ Google Trends collection failed: {e}")
    
    # Adzuna Jobs
    if sources_config.get('adzuna', True):
        try:
            logger.info("💼 Collecting Adzuna Jobs data...")
            from src.bronze.ImportAzuna import main as adzuna_main
            adzuna_main()
            logger.info("✅ Adzuna Jobs collection completed")
        except Exception as e:
            logger.error(f"❌ Adzuna Jobs collection failed: {e}")
    
    # GitHub Repositories
    if sources_config.get('github', True):
        try:
            logger.info("🐙 Collecting GitHub Repositories data...")
            from src.bronze.import_github_repos import main as github_main
            github_main()
            logger.info("✅ GitHub Repositories collection completed")
        except Exception as e:
            logger.error(f"❌ GitHub Repositories collection failed: {e}")
    
    # StackOverflow Survey
    if sources_config.get('stackoverflow', True):
        try:
            logger.info("📋 Collecting StackOverflow Survey data...")
            from src.bronze.import_stackoverflow_survey import main as stackoverflow_main
            stackoverflow_main()
            logger.info("✅ StackOverflow Survey collection completed")
        except Exception as e:
            logger.error(f"❌ StackOverflow Survey collection failed: {e}")
    
    # EuroTechJobs
    if sources_config.get('eurotechjobs', True):
        try:
            logger.info("🇪🇺 Collecting EuroTechJobs data...")
            # from src.bronze.import_eurotechjobs import main as eurotechjobs_main
            # eurotechjobs_main()
            logger.info("⚠️ EuroTechJobs collection not implemented yet")
        except Exception as e:
            logger.error(f"❌ EuroTechJobs collection failed: {e}")
    
    logger.info("🔹 Bronze Layer Collection Completed")

def run_silver_layer(sources_config):
    """Execute silver layer data processing"""
    logger.info("🔸 Starting Silver Layer Data Processing")
    
    # Google Trends Silver Processing
    if sources_config.get('gtrends', True):
        try:
            logger.info("📊 Processing Google Trends to Silver...")
            from src.silver.GtrendsSilver import main as gtrends_silver_main
            gtrends_silver_main()
            logger.info("✅ Google Trends Silver processing completed")
        except Exception as e:
            logger.error(f"❌ Google Trends Silver processing failed: {e}")
    
    # Adzuna Jobs Silver Processing
    if sources_config.get('adzuna', True):
        try:
            logger.info("💼 Processing Adzuna Jobs to Silver...")
            # Import and run Adzuna Silver processing
            # Note: Assuming AzunaSilver.py exists or needs to be created
            logger.info("⚠️ Adzuna Silver processing not implemented yet")
        except Exception as e:
            logger.error(f"❌ Adzuna Jobs Silver processing failed: {e}")
    
    # GitHub Repositories Silver Processing
    if sources_config.get('github', True):
        try:
            logger.info("🐙 Processing GitHub Repositories to Silver...")
            from src.silver.process_github_repos import main as github_silver_main
            github_silver_main()
            logger.info("✅ GitHub Repositories Silver processing completed")
        except Exception as e:
            logger.error(f"❌ GitHub Repositories Silver processing failed: {e}")
    
    # StackOverflow Survey Silver Processing
    if sources_config.get('stackoverflow', True):
        try:
            logger.info("📋 Processing StackOverflow Survey to Silver...")
            from src.silver.process_stackoverflow_survey import main as stackoverflow_silver_main
            stackoverflow_silver_main()
            logger.info("✅ StackOverflow Survey Silver processing completed")
        except Exception as e:
            logger.error(f"❌ StackOverflow Survey Silver processing failed: {e}")
    
    # EuroTechJobs Silver Processing
    if sources_config.get('eurotechjobs', True):
        try:
            logger.info("🇪🇺 Processing EuroTechJobs to Silver...")
            # from src.silver.process_eurotechjobs import main as eurotechjobs_silver_main
            # eurotechjobs_silver_main()
            logger.info("⚠️ EuroTechJobs Silver processing not implemented yet")
        except Exception as e:
            logger.error(f"❌ EuroTechJobs Silver processing failed: {e}")
    
    logger.info("🔸 Silver Layer Processing Completed")

def run_gold_layer(sources_config):
    """Execute gold layer table unification"""
    logger.info("🔶 Starting Gold Layer Table Unification")
    
    try:
        logger.info("🏗️ Running table unification...")
        # Import and run table unification script
        from src.gold.table_unification import main as gold_main
        gold_main()
        logger.info("✅ Gold Layer table unification completed")
    except Exception as e:
        logger.error(f"❌ Gold Layer table unification failed: {e}")
    
    logger.info("🔶 Gold Layer Processing Completed")

def check_environment():
    """Check if environment is properly configured"""
    logger.info("🔧 Checking environment configuration...")
    
    # Check if .env file exists
    env_path = project_root / '.env'
    if not env_path.exists():
        logger.warning("⚠️ .env file not found, using default configuration")
    
    # Check MySQL configuration
    required_env_vars = ['MYSQL_HOST', 'MYSQL_USER', 'MYSQL_PASSWORD']
    missing_vars = []
    
    for var in required_env_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        logger.warning(f"⚠️ Missing environment variables: {', '.join(missing_vars)}")
        logger.info("Using default values where possible")
    
    logger.info("✅ Environment check completed")

def main():
    """Main pipeline execution"""
    parser = argparse.ArgumentParser(description='Run Challenge DL & DI Data Pipeline')
    parser.add_argument('--skip-bronze', action='store_true', help='Skip bronze layer collection')
    parser.add_argument('--skip-silver', action='store_true', help='Skip silver layer processing')
    parser.add_argument('--skip-gold', action='store_true', help='Skip gold layer unification')
    parser.add_argument('--only-layer', choices=['bronze', 'silver', 'gold'], help='Run only specific layer')
    
    # Source-specific flags
    parser.add_argument('--skip-gtrends', action='store_true', help='Skip Google Trends processing')
    parser.add_argument('--skip-adzuna', action='store_true', help='Skip Adzuna Jobs processing')
    parser.add_argument('--skip-github', action='store_true', help='Skip GitHub Repositories processing')
    parser.add_argument('--skip-stackoverflow', action='store_true', help='Skip StackOverflow Survey processing')
    parser.add_argument('--skip-eurotechjobs', action='store_true', help='Skip EuroTechJobs processing')
    
    args = parser.parse_args()
    
    # Configure which sources to run
    sources_config = {
        'gtrends': not args.skip_gtrends,
        'adzuna': not args.skip_adzuna,
        'github': not args.skip_github,
        'stackoverflow': not args.skip_stackoverflow,
        'eurotechjobs': not args.skip_eurotechjobs
    }
    
    logger.info("🚀 Starting Challenge DL & DI Pipeline")
    logger.info(f"📝 Active sources: {[k for k, v in sources_config.items() if v]}")
    
    start_time = datetime.now()
    
    try:
        # Environment check
        check_environment()
        
        # Layer execution based on arguments
        if args.only_layer:
            if args.only_layer == 'bronze':
                run_bronze_layer(sources_config)
            elif args.only_layer == 'silver':
                run_silver_layer(sources_config)
            elif args.only_layer == 'gold':
                run_gold_layer(sources_config)
        else:
            # Run all layers unless specifically skipped
            if not args.skip_bronze:
                run_bronze_layer(sources_config)
            
            if not args.skip_silver:
                run_silver_layer(sources_config)
            
            if not args.skip_gold:
                run_gold_layer(sources_config)
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info("🎉 Pipeline execution completed successfully!")
        logger.info(f"⏱️ Total execution time: {duration}")
        
    except KeyboardInterrupt:
        logger.info("⏹️ Pipeline execution interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"💥 Pipeline execution failed: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()