import requests
import time
import random
import pandas as pd
import os
from datetime import datetime
from typing import List, Dict, Any

TECHNOLOGIES = [
    'python', 'javascript', 'java', 'typescript', 'c++', 'c#', 'php', 'ruby', 'go', 'rust', 'kotlin',
    'swift', 'scala', 'dart', 'elixir', 'haskell', 'clojure', 'lua', 'perl', 'r', 'matlab',
    
    'react', 'angular', 'vue', 'django', 'flask', 'spring', 'laravel', 'rails', 'express',
    'nextjs', 'nuxt', 'svelte', 'jquery', 'bootstrap', 'tailwind',
    
    'docker', 'kubernetes', 'tensorflow', 'pytorch', 'spark', 'elasticsearch',
    'mongodb', 'postgresql', 'mysql', 'redis', 'nginx', 'apache'
]

GITHUB_API_BASE = "https://api.github.com"
SEARCH_ENDPOINT = f"{GITHUB_API_BASE}/search/repositories"
REPOS_ENDPOINT = f"{GITHUB_API_BASE}/repos"

class GitHubRepoCollector:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/vnd.github.v3+json',
            'User-Agent': 'DataPipeline-GitHub-Collector/1.0'
        })
        
    def search_repositories_by_language(self, language, sort='stars', order='desc', per_page=100, max_pages=5):
        """Search repositories by programming language"""
        all_repos = []
        
        for page in range(1, max_pages + 1):
            params = {
                'q': f'language:{language}',
                'sort': sort,
                'order': order,
                'per_page': per_page,
                'page': page
            }
            
            try:
                print(f"Fetching page {page} for language: {language}")
                response = self.session.get(SEARCH_ENDPOINT, params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    repos = data.get('items', [])
                    
                    for repo in repos:
                        repo_data = self.extract_repo_data(repo, language)
                        all_repos.append(repo_data)
                    
                    if len(repos) < per_page:
                        break
                        
                elif response.status_code == 403:
                    print(f"Rate limit exceeded. Waiting...")
                    time.sleep(3600)
                    continue
                    
                else:
                    print(f"Error fetching data: {response.status_code}")
                    break
                    
                time.sleep(random.uniform(1, 3))
                
            except Exception as e:
                print(f"Error processing page {page} for {language}: {e}")
                time.sleep(random.uniform(5, 10))
                
        return all_repos
    
    def search_repositories_by_topic(self, topic, sort='stars', order='desc', per_page=100, max_pages=3):
        """Search repositories by topic/technology"""
        all_repos = []
        
        for page in range(1, max_pages + 1):
            params = {
                'q': f'topic:{topic}',
                'sort': sort,
                'order': order,
                'per_page': per_page,
                'page': page
            }
            
            try:
                print(f"Fetching page {page} for topic: {topic}")
                response = self.session.get(SEARCH_ENDPOINT, params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    repos = data.get('items', [])
                    
                    for repo in repos:
                        repo_data = self.extract_repo_data(repo, topic, search_type='topic')
                        all_repos.append(repo_data)
                    
                    if len(repos) < per_page:
                        break
                        
                elif response.status_code == 403:
                    print(f"Rate limit exceeded. Waiting...")
                    time.sleep(360)
                    continue
                    
                else:
                    print(f"Error fetching data: {response.status_code}")
                    break
                    
                time.sleep(random.uniform(1, 3))
                
            except Exception as e:
                print(f"Error processing page {page} for {topic}: {e}")
                time.sleep(random.uniform(5, 10))
                
        return all_repos
    
    def get_repository_details(self, owner, repo_name):
        """Get detailed repository information"""
        try:
            repo_url = f"{REPOS_ENDPOINT}/{owner}/{repo_name}"
            repo_response = self.session.get(repo_url)
            
            if repo_response.status_code != 200:
                return None
                
            repo_data = repo_response.json()
            
            commits_url = f"{repo_url}/stats/commit_activity"
            commits_response = self.session.get(commits_url)
            
            commit_activity = 0
            if commits_response.status_code == 200:
                commit_data = commits_response.json()
                if commit_data:
                    commit_activity = sum(week.get('total', 0) for week in commit_data)
            
            contributors_url = f"{repo_url}/contributors"
            contributors_response = self.session.get(contributors_url)
            
            contributors_count = 0
            if contributors_response.status_code == 200:
                contributors_data = contributors_response.json()
                contributors_count = len(contributors_data) if contributors_data else 0
            
            time.sleep(random.uniform(0.5, 1.5))
            
            return {
                'commit_activity_last_year': commit_activity,
                'contributors_count': contributors_count,
                'has_wiki': repo_data.get('has_wiki', False),
                'has_pages': repo_data.get('has_pages', False),
                'archived': repo_data.get('archived', False),
                'disabled': repo_data.get('disabled', False)
            }
            
        except Exception as e:
            print(f"Error getting details for {owner}/{repo_name}: {e}")
            return None
    
    def extract_repo_data(self, repo, technology, search_type='language'):
        """Extract and structure repository data"""
        repo_data = {
            'id': repo.get('id'),
            'name': repo.get('name'),
            'technology': technology,
            'search_type': search_type,
            'stars_count': repo.get('stargazers_count', 0),
            'forks_count': repo.get('forks_count', 0),
            'watchers_count': repo.get('watchers_count', 0),
            'open_issues_count': repo.get('open_issues_count', 0),
            'created_at': repo.get('created_at'),
            'collected_at': datetime.now().isoformat(),
        }
        
        return repo_data

def calculate_activity_score(repo_data):
    """Calculate a composite activity score for repositories"""
    stars = repo_data.get('stars_count', 0)
    forks = repo_data.get('forks_count', 0)
    watchers = repo_data.get('watchers_count', 0)
    issues = repo_data.get('open_issues_count', 0)
    
    try:
        created_at = datetime.fromisoformat(repo_data.get('created_at', '').replace('Z', '+00:00'))
        days_since_creation = (datetime.now() - created_at.replace(tzinfo=None)).days
    except:
        days_since_creation = 365
    
    popularity_score = (stars * 1.0) + (forks * 2.0) + (watchers * 0.5)
    
    age_penalty = max(0, days_since_creation - 365) * 0.05
    
    activity_score = max(0, popularity_score - age_penalty)
    
    return {
        'popularity_score': popularity_score,
        'days_since_creation': days_since_creation,
        'activity_score': activity_score,
        'activity_level': 'high' if activity_score > 1000 else 'medium' if activity_score > 100 else 'low'
    }

def collect_github_data(technologies=None, max_repos_per_tech=500):
    """Main function to collect GitHub repository data"""
    if technologies is None:
        technologies = TECHNOLOGIES
    
    collector = GitHubRepoCollector()
    all_repos = []
    
    for tech in technologies:
        print(f"\n=== Collecting data for {tech} ===")
        
        repos_by_lang = collector.search_repositories_by_language(
            tech, max_pages=1
        )
        
        repos_by_topic = collector.search_repositories_by_topic(
            tech, max_pages=1
        )
        
        tech_repos = repos_by_lang + repos_by_topic
        
        seen_ids = set()
        unique_repos = []
        for repo in tech_repos:
            if repo['id'] not in seen_ids:
                seen_ids.add(repo['id'])
                unique_repos.append(repo)
        
        unique_repos = unique_repos[:max_repos_per_tech]
        
        for repo in unique_repos:
            activity_data = calculate_activity_score(repo)
            repo.update(activity_data)
        
        all_repos.extend(unique_repos)
        print(f"Collected {len(unique_repos)} repositories for {tech}")
        
        time.sleep(random.uniform(5, 15))
    
    return all_repos

def save_to_parquet(repos_data: List[Dict[str, Any]], output_path: str):
    """Save repository data to parquet format using pandas"""
    if not repos_data:
        print("No data to save")
        return
    
    df = pd.DataFrame(repos_data)
    
    df['id'] = df['id'].astype('Int64')
    df['stars_count'] = df['stars_count'].astype('Int64')
    df['forks_count'] = df['forks_count'].astype('Int64')
    df['watchers_count'] = df['watchers_count'].astype('Int64')
    df['open_issues_count'] = df['open_issues_count'].astype('Int64')
    df['days_since_creation'] = df['days_since_creation'].astype('Int64')
    df['popularity_score'] = df['popularity_score'].astype('float64')
    df['activity_score'] = df['activity_score'].astype('float64')
    
    df['created_at'] = pd.to_datetime(df['created_at'])
    df['year'] = df['created_at'].dt.year
    df['month'] = df['created_at'].dt.month
    df['day'] = df['created_at'].dt.day
    
    os.makedirs(output_path, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"github_repos_{timestamp}.parquet"
    full_path = os.path.join(output_path, filename)
    
    df.to_parquet(
        full_path,
        engine='pyarrow',
        compression='snappy',
        index=False
    )
    
    print(f"Data saved to {full_path}")
    print(f"Saved {len(df)} records with {len(df.columns)} columns")

def main():
    """Main execution function"""
    print("Starting GitHub repositories data collection...")
    
    repos_data = collect_github_data(
        technologies=TECHNOLOGIES,
        max_repos_per_tech=5
    )
    
    output_path = "../../data/bronze/github_repos"
    save_to_parquet(repos_data, output_path)
    
    print(f"\nData collection completed! Collected {len(repos_data)} repositories.")
    print(f"Data saved to {output_path}")
    
    if repos_data:
        technologies_count = {}
        for repo in repos_data:
            tech = repo['technology']
            technologies_count[tech] = technologies_count.get(tech, 0) + 1
        
        print("\nSummary by technology:")
        for tech, count in sorted(technologies_count.items()):
            print(f"  {tech}: {count} repositories")

if __name__ == "__main__":
    main()