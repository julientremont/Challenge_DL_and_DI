"""
Unified path management for the Challenge DL & DI project.
Provides consistent, absolute paths that work across different IDEs and environments.
"""

from pathlib import Path
import os


def get_project_root() -> Path:
    """
    Determine the project root directory dynamically.
    Looks for the directory containing 'src/' folder.
    
    Returns:
        Path: Absolute path to the project root
    """
    current_path = Path(__file__).resolve()
    
    # Walk up the directory tree to find the project root (containing 'src')
    for parent in current_path.parents:
        if (parent / 'src').exists() and (parent / 'src').is_dir():
            return parent
    
    # Fallback: assume current structure (src/utils/paths.py -> project root)
    return current_path.parent.parent.parent


# Project structure constants
PROJECT_ROOT = get_project_root()

# Data directories
DATA_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze" 
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

# Specific data source directories
BRONZE_DIRS = {
    'google_trends': BRONZE_DIR / "google_trends",
    'github_repos': BRONZE_DIR / "github_repos", 
    'stackoverflow_survey': BRONZE_DIR / "stackoverflow_survey",
    'adzuna_jobs': BRONZE_DIR / "adzuna_jobs",
    'eurotechjobs': BRONZE_DIR / "eurotechjobs",
    'jobicy_jobs': BRONZE_DIR / "jobicy"
}

SILVER_DIRS = {
    'google_trends': SILVER_DIR / "google_trends",
    'github_repos': SILVER_DIR / "github_repos",
    'stackoverflow_survey': SILVER_DIR / "stackoverflow_survey", 
    'adzuna_jobs': SILVER_DIR / "adzuna_jobs",
    'eurotechjobs': SILVER_DIR / "eurotechjobs",
    'jobicy_jobs': SILVER_DIR / "jobicy"
}

# Configuration and logs directories
CONFIG_DIR = PROJECT_ROOT / "config"
LOGS_DIR = PROJECT_ROOT / "logs"
TEMP_DIR = PROJECT_ROOT / "temp"


def ensure_directories():
    """Create all necessary directories if they don't exist."""
    directories = [
        DATA_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR,
        CONFIG_DIR, LOGS_DIR, TEMP_DIR
    ]
    
    # Add all bronze and silver subdirectories
    directories.extend(BRONZE_DIRS.values())
    directories.extend(SILVER_DIRS.values())
    
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)


def get_bronze_path(source: str) -> Path:
    """
    Get bronze layer path for a specific data source.
    
    Args:
        source: Data source name (e.g., 'github_repos', 'jobicy_jobs')
        
    Returns:
        Path: Bronze directory path for the source
    """
    if source in BRONZE_DIRS:
        return BRONZE_DIRS[source]
    else:
        # Fallback for unknown sources
        return BRONZE_DIR / source


def get_silver_path(source: str) -> Path:
    """
    Get silver layer path for a specific data source.
    
    Args:
        source: Data source name (e.g., 'github_repos', 'jobicy_jobs')
        
    Returns:
        Path: Silver directory path for the source
    """
    if source in SILVER_DIRS:
        return SILVER_DIRS[source]
    else:
        # Fallback for unknown sources
        return SILVER_DIR / source


def get_timestamped_filename(base_name: str, extension: str = "parquet") -> str:
    """
    Generate a timestamped filename.
    
    Args:
        base_name: Base name for the file
        extension: File extension (default: parquet)
        
    Returns:
        str: Timestamped filename
    """
    from datetime import datetime
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    return f"{base_name}_{timestamp}.{extension}"


# Utility functions for common path operations
def list_parquet_files(directory: Path) -> list:
    """List all parquet files in a directory."""
    if not directory.exists():
        return []
    return list(directory.glob("*.parquet"))


def get_latest_parquet_file(directory: Path) -> Path:
    """Get the most recently modified parquet file in a directory."""
    parquet_files = list_parquet_files(directory)
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {directory}")
    
    return max(parquet_files, key=lambda f: f.stat().st_mtime)


# Initialize directories on import
ensure_directories()


# For debugging and verification
if __name__ == "__main__":
    print(f"Project Root: {PROJECT_ROOT}")
    print(f"Data Directory: {DATA_DIR}")
    print(f"Bronze Directory: {BRONZE_DIR}")
    print(f"Silver Directory: {SILVER_DIR}")
    print(f"Gold Directory: {GOLD_DIR}")
    
    print("\nBronze directories:")
    for source, path in BRONZE_DIRS.items():
        print(f"  {source}: {path}")
    
    print("\nSilver directories:")
    for source, path in SILVER_DIRS.items():
        print(f"  {source}: {path}")
    
    print(f"\nAll directories exist: {all(d.exists() for d in [DATA_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR])}")