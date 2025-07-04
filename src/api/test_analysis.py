#!/usr/bin/env python3
"""
Test script for Analysis API - Gold layer endpoints.
Run this to verify the analysis app is working correctly.
"""

import os
import sys
import django
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'src.api.settings')
django.setup()

def test_analysis_models():
    """Test if analysis models can connect to gold database"""
    try:
        from src.api.analysis.models import (
            DimCountry, DimTechnology, AnalysisTechActivity, AnalysisJobDetails
        )
        
        print("🧪 Testing Analysis Models...")
        
        # Test dimension tables
        country_count = DimCountry.objects.using('gold').count()
        print(f"✅ Countries in gold DB: {country_count}")
        
        tech_count = DimTechnology.objects.using('gold').count()
        print(f"✅ Technologies in gold DB: {tech_count}")
        
        # Test fact tables
        activity_count = AnalysisTechActivity.objects.using('gold').count()
        print(f"✅ Tech activities in gold DB: {activity_count}")
        
        job_count = AnalysisJobDetails.objects.using('gold').count()
        print(f"✅ Job details in gold DB: {job_count}")
        
        print("🎉 All models work correctly!")
        return True
        
    except Exception as e:
        print(f"❌ Error testing models: {e}")
        return False

def test_database_connections():
    """Test both database connections"""
    try:
        from django.db import connections
        
        print("🔗 Testing Database Connections...")
        
        # Test default (silver) connection
        silver_conn = connections['default']
        with silver_conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'silver'")
            silver_tables = cursor.fetchone()[0]
            print(f"✅ Silver DB tables: {silver_tables}")
        
        # Test gold connection
        gold_conn = connections['gold']
        with gold_conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'gold'")
            gold_tables = cursor.fetchone()[0]
            print(f"✅ Gold DB tables: {gold_tables}")
        
        print("🎉 Both database connections work!")
        return True
        
    except Exception as e:
        print(f"❌ Error testing connections: {e}")
        return False

def main():
    """Run all tests"""
    print("🚀 Testing Analysis API - Gold Layer")
    print("=" * 50)
    
    success = True
    
    # Test database connections
    if not test_database_connections():
        success = False
    
    print()
    
    # Test models
    if not test_analysis_models():
        success = False
    
    print()
    print("=" * 50)
    if success:
        print("✅ All tests passed! Analysis API is ready to use.")
        print("\n📋 Available endpoints:")
        print("- /api/analysis/tech-activity/")
        print("- /api/analysis/job-market/")
        print("- /api/analysis/tech-activity/technology_stats/")
        print("- /api/analysis/job-market/market_summary/")
    else:
        print("❌ Some tests failed. Check the errors above.")
    
    return success

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)