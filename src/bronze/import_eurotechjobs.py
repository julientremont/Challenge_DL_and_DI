import requests
import time
import random
import pandas as pd
import os
from datetime import datetime
from typing import List, Dict, Any
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EuroTechJobsScraper:
    def __init__(self):
        self.base_url = "https://www.eurotechjobs.com"
        self.job_search_url = f"{self.base_url}/job_search"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        self.retry_count = 3
        self.retry_delay = 5
        self.categories = []
        
    def make_request(self, url: str, max_retries: int = 3) -> requests.Response:
        """Make HTTP request with retry logic and error handling"""
        for attempt in range(max_retries):
            try:
                logger.info(f"Making request to: {url} (attempt {attempt + 1})")
                response = self.session.get(url, timeout=30)
                
                if response.status_code == 200:
                    return response
                elif response.status_code == 429:
                    wait_time = (2 ** attempt) * 5
                    logger.warning(f"Rate limited. Waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                elif response.status_code in [403, 404]:
                    logger.error(f"HTTP {response.status_code} for {url}")
                    return None
                else:
                    logger.warning(f"HTTP {response.status_code} for {url}")
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed for {url}: {e}")
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) * self.retry_delay
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Max retries reached for {url}")
                    return None
        
        return None
    
    def get_available_categories(self) -> List[tuple]:
        """Fetch available job categories from the job search page"""
        logger.info("Fetching available job categories...")
        
        response = self.make_request(self.job_search_url)
        if not response:
            return []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        category_inputs = soup.find_all('input', {'name': 'category[]'})
        categories = []
        
        for inp in category_inputs:
            value = inp.get('value', '')
            label = None
            
            if inp.parent:
                parent_text = inp.parent.get_text(strip=True)
                if parent_text and len(parent_text) < 100:
                    label = re.sub(r'\s*\(\d+\)\s*$', '', parent_text).strip()
            
            if value and label:
                categories.append((value, label))
        
        logger.info(f"Found {len(categories)} categories")
        return categories
    
    def extract_technologies(self, text: str) -> List[str]:
        """Extract technology keywords from job text"""
        technologies = []
        
        tech_keywords = [
            'python', 'javascript', 'java', 'typescript', 'c++', 'c#', 'php', 'ruby', 'go', 'rust',
            'kotlin', 'swift', 'scala', 'dart', 'matlab', 'perl', 'lua', 'haskell', 'clojure',
            
            'react', 'angular', 'vue', 'django', 'flask', 'spring', 'laravel', 'rails', 'express',
            'nextjs', 'nuxt', 'svelte', 'jquery', 'bootstrap', 'tailwind', 'node.js', 'nodejs',
            
            'mysql', 'postgresql', 'mongodb', 'redis', 'elasticsearch', 'cassandra', 'oracle',
            'sqlite', 'mariadb', 'dynamodb', 'firebase',
            
            'aws', 'azure', 'gcp', 'docker', 'kubernetes', 'jenkins', 'gitlab', 'github',
            'terraform', 'ansible', 'vagrant', 'heroku', 'vercel', 'netlify',
            
            'spark', 'hadoop', 'kafka', 'rabbitmq', 'nginx', 'apache', 'linux', 'ubuntu',
            'tensorflow', 'pytorch', 'scikit-learn', 'pandas', 'numpy', 'matplotlib'
        ]
        
        text_lower = text.lower()
        for tech in tech_keywords:
            if tech.lower() in text_lower:
                technologies.append(tech)
        
        return list(set(technologies))
    
    def parse_job_page(self, job_href: str, category: str = '') -> Dict[str, Any]:
        """Parse job data by fetching the individual job page"""
        try:
            job_data = {
                'job_title': '',
                'company': '',
                'location': '',
                'technologies': [],
                'job_type': '',
                'url': '',
                'category': category,
                'scraped_at': datetime.now()
            }
            
            job_url = urljoin(self.base_url, job_href)
            job_data['url'] = job_url
            
            response = self.make_request(job_url)
            if not response:
                return None
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            job_display = soup.find('div', class_='jobDisplay')
            if not job_display:
                logger.warning(f"No jobDisplay div found for {job_url}")
                return None
            
            job_text = job_display.get_text()
            lines = [line.strip() for line in job_text.split('\n') if line.strip()]
            
            if len(lines) >= 1:
                job_data['job_title'] = lines[0]
            
            if len(lines) >= 2:
                job_data['company'] = lines[1]
            
            if len(lines) >= 3:
                job_data['location'] = lines[2]
            
            job_data['technologies'] = self.extract_technologies(job_text)
            
            if job_data['job_title']:
                title_lower = job_data['job_title'].lower()
                if any(word in title_lower for word in ['senior', 'lead', 'principal']):
                    job_data['job_type'] = 'Senior'
                elif any(word in title_lower for word in ['junior', 'entry', 'graduate']):
                    job_data['job_type'] = 'Junior'
                elif any(word in title_lower for word in ['intern', 'trainee']):
                    job_data['job_type'] = 'Internship'
                else:
                    job_data['job_type'] = 'Mid-level'
            
            return job_data
            
        except Exception as e:
            logger.error(f"Error parsing job page {job_href}: {e}")
            return None
    
    def scrape_jobs_by_category(self, category_value: str, category_name: str) -> List[Dict[str, Any]]:
        """Scrape job listings for a specific category using category-specific URL"""
        logger.info(f"Scraping jobs for category: {category_name}")
        jobs = []
        
        category_url = f"{self.base_url}/job_search/category/{category_value}"
        
        response = self.make_request(category_url)
        if not response:
            return jobs
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        job_links = soup.find_all('a', href=re.compile(r'/job_display/'))
        logger.info(f"Found {len(job_links)} job links for category: {category_name}")
        
        seen_urls = set()
        for link in job_links:
            href = link.get('href')
            if href and href not in seen_urls:
                seen_urls.add(href)
                job_data = self.parse_job_page(href, category_name)
                if job_data and job_data['job_title']:
                    jobs.append(job_data)
                    
                time.sleep(random.uniform(0.5, 1.0))
        
        return jobs
    
    def scrape_all_jobs(self) -> List[Dict[str, Any]]:
        """Scrape jobs from all available categories"""
        all_jobs = []
        
        logger.info("Starting EuroTechJobs scraping...")
        
        if not self.categories:
            self.categories = self.get_available_categories()
        
        if not self.categories:
            logger.error("No categories found. Cannot proceed with scraping.")
            return all_jobs
        

        limited_categories = self.categories[:2]
        logger.info(f"Processing {len(limited_categories)} categories out of {len(self.categories)} available")
        
        for category_value, category_name in limited_categories:
            logger.info(f"Processing category: {category_name} ({category_value})")
            
            category_jobs = self.scrape_jobs_by_category(category_value, category_name)
            all_jobs.extend(category_jobs)
            logger.info(f"Scraped {len(category_jobs)} jobs from category: {category_name}")
            
            time.sleep(random.uniform(2, 3))
        
        unique_jobs = []
        seen = set()
        for job in all_jobs:
            key = (job['job_title'], job['company'], job['url'])
            if key not in seen:
                seen.add(key)
                unique_jobs.append(job)
        
        logger.info(f"Total unique jobs scraped: {len(unique_jobs)}")
        return unique_jobs

def save_to_parquet(jobs_data: List[Dict[str, Any]], output_path: str):
    """Save jobs data to parquet format using pandas"""
    if not jobs_data:
        logger.warning("No data to save")
        return
    
    df = pd.DataFrame(jobs_data)
    
    df['technologies'] = df['technologies'].apply(lambda x: ','.join(x) if x else '')
    
    df['scraped_at'] = df['scraped_at'].astype(str)
    
    df['year'] = pd.to_datetime(df['scraped_at']).dt.year
    df['month'] = pd.to_datetime(df['scraped_at']).dt.month
    df['day'] = pd.to_datetime(df['scraped_at']).dt.day
    
    os.makedirs(output_path, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"eurotechjobs_{timestamp}.parquet"
    full_path = os.path.join(output_path, filename)
    
    df.to_parquet(
        full_path,
        engine='pyarrow',
        compression='snappy',
        index=False
    )
    
    logger.info(f"Data saved to {full_path}")
    logger.info(f"Saved {len(df)} records with {len(df.columns)} columns")

def main():
    """Main execution function"""
    logger.info("Starting EuroTechJobs data collection...")
    
    scraper = EuroTechJobsScraper()
    
    jobs_data = scraper.scrape_all_jobs()
    
    output_path = "./data/bronze/eurotechjobs"
    save_to_parquet(jobs_data, output_path)
    
    logger.info(f"Data collection completed! Collected {len(jobs_data)} jobs.")
    logger.info(f"Data saved to {output_path}")
    
    if jobs_data:
        companies = set(job['company'] for job in jobs_data if job['company'])
        locations = set(job['location'] for job in jobs_data if job['location'])
        categories = set(job['category'] for job in jobs_data if job['category'])
        all_techs = []
        for job in jobs_data:
            all_techs.extend(job.get('technologies', []))
        
        logger.info(f"\nSummary statistics:")
        logger.info(f"  Total jobs: {len(jobs_data)}")
        logger.info(f"  Unique companies: {len(companies)}")
        logger.info(f"  Unique locations: {len(locations)}")
        logger.info(f"  Categories processed: {len(categories)}")
        logger.info(f"  Most common technologies: {', '.join(list(set(all_techs))[:10])}")

if __name__ == "__main__":
    main()