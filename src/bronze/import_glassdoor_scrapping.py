from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException
from webdriver_manager.chrome import ChromeDriverManager
import concurrent.futures
import urllib.parse
import time
import csv
import random
import os
import pandas as pd
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def scrape_glassdoor(keyword):
    encoded_kw = urllib.parse.quote_plus(keyword)
    GLASSDOOR_URL = f"https://www.glassdoor.com/Job/jobs.htm?sc.keyword={encoded_kw}"

    # Enhanced Chrome options for Cloudflare bypass
    options = Options()
    options.add_argument("--start-maximized")
    options.add_argument("--disable-gpu")
    options.add_argument("--headless=new")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-web-security")
    options.add_argument("--disable-features=VizDisplayCompositor")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    
    # Randomize user agent slightly
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebDriver/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ]
    options.add_argument(f"user-agent={random.choice(user_agents)}")
    
    # Additional anti-detection measures
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )
    
    # Remove webdriver property
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    logger.info(f"Starting scrape for keyword: {keyword}")
    
    try:
        driver.get(GLASSDOOR_URL)
        
        # Wait longer and check for Cloudflare
        initial_wait = 4
        logger.info(f"Waiting {initial_wait:.1f}s for page to load...")
        time.sleep(initial_wait)
        
        # Check if we hit Cloudflare protection
        page_title = driver.title.lower()
        page_source = driver.page_source.lower()
        
        if 'cloudflare' in page_title or 'checking your browser' in page_source or 'just a moment' in page_source:
            logger.warning(f"Cloudflare protection detected for {keyword}. Waiting longer...")
            time.sleep(random.uniform(15, 25))
            
        # Scroll to trigger any lazy loading
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
        time.sleep(2)
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(2)

    except Exception as e:
        logger.error(f"Error loading page for {keyword}: {e}")
        driver.quit()
        return []

    results = []
    load_more_clicks = 0
    max_load_more_attempts = 1

    try:
        # Wait for job list to load with multiple possible selectors
        job_list_selectors = [
            "JobsList_jobsList__lqjTr",
            "react-job-listing",
            "jobsListContainer",
            "[data-test='jobsList']"
        ]
        
        ul = None
        for selector in job_list_selectors:
            try:
                if selector.startswith('['):
                    ul = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                else:
                    ul = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.CLASS_NAME, selector))
                    )
                logger.info(f"Found job list using selector: {selector}")
                break
            except TimeoutException:
                continue
        
        if not ul:
            logger.error(f"Could not find job list for keyword: {keyword}")
            driver.quit()
            return results

        # Multiple attempts to load more jobs
        while load_more_clicks < max_load_more_attempts:
            # Get current job cards
            job_cards = ul.find_elements(By.TAG_NAME, "li")
            logger.info(f"[{keyword}] Found {len(job_cards)} job cards (after {load_more_clicks} load more clicks)")

            # Process current job cards - TEMP: only process first job card
            for idx, job_card in enumerate(job_cards[len(results):]):  # Only process first job card
                try:
                    # Scroll to job card with random delay
                    driver.execute_script("arguments[0].scrollIntoView(true);", job_card)
                    time.sleep(random.uniform(0.5, 1.5))
                    
                    # Click job card
                    job_card.click()

                    # Wait for job details to load
                    wait = WebDriverWait(driver, 2)
                    
                    # Try multiple selectors for job details header
                    header_selectors= [
                        "JobDetails_jo bDetailsHeaderWrapper__JlXWG",
                        "JobDetails_jobHeader__",
                        "[data-test='job-header']",
                        ".job-details-header"
                    ]
                    
                    header = None
                    for h_selector in header_selectors:
                        try:
                            if h_selector.startswith('[') or h_selector.startswith('.'):
                                header = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, h_selector)))
                            else:
                                header = wait.until(EC.presence_of_element_located((By.CLASS_NAME, h_selector)))
                            break
                        except TimeoutException:
                            continue
                    
                    if not header:
                        logger.warning(f"Could not find job details header for job {idx + 1}")
                        continue
                    
                    time.sleep(random.uniform(1, 2))  # Wait for details to fully load

                    # Extract job information with multiple selectors
                    job_title = extract_text_multiple_selectors(header, [
                        "h1.heading_Heading__BqX5J",
                        "h1[data-test='job-title']",
                        ".job-title h1",
                        "h1"
                    ])

                    company_name = extract_text_multiple_selectors(header, [
                        "h4.heading_Heading__BqX5J",
                        "[data-test='employer-name']",
                        ".employer-name",
                        "h4"
                    ])

                    location = extract_text_multiple_selectors(header, [
                        "div[data-test='location']",
                        ".location",
                        "[data-test='job-location']"
                    ])

                    # Extract salary with multiple attempts
                    salary = ""
                    salary_selectors = [
                        f"jd-salary-{header.get_attribute('data-brandviews').split('jlid=')[-1] if header.get_attribute('data-brandviews') else ''}",
                        "JobCard_salaryEstimate__QpbTW",
                        "[data-test='salary']",
                        ".salary",
                        ".salary-estimate"
                    ]
                    
                    for sal_selector in salary_selectors:
                        try:
                            if sal_selector.startswith('[') or sal_selector.startswith('.'):
                                salary_elem = header.find_element(By.CSS_SELECTOR, sal_selector)
                            else:
                                salary_elem = header.find_element(By.ID if not sal_selector.startswith('.') else By.CLASS_NAME, sal_selector)
                            salary = salary_elem.text
                            if salary:
                                break
                        except:
                            continue

                    # Store result
                    job_data = {
                        "keyword": keyword,
                        "title": job_title,
                        "company": company_name,
                        "location": location,
                        "salary": salary,
                        "scraped_at": datetime.now().isoformat()
                    }
                    results.append(job_data)

                    logger.info(f"[{keyword}] [{len(results)}] {job_title} | {company_name} | {location} | {salary}")

                except Exception as e:
                    logger.warning(f"[{keyword}] Error scraping job at index {idx}: {e}")
                    continue
                
                finally:
                    # Try to close job details modal
                    try:
                        close_selectors = [
                            ".CloseButton",
                            "[data-test='modal-close']",
                            ".modal-close",
                            ".close-button"
                        ]
                        for close_selector in close_selectors:
                            try:
                                close_btn = WebDriverWait(driver, 3).until(
                                    EC.element_to_be_clickable((By.CSS_SELECTOR, close_selector))
                                )
                                close_btn.click()
                                time.sleep(1)
                                break
                            except:
                                continue
                    except:
                        pass

            # Try to load more jobs
            load_more_success = False
            load_more_selectors = [
                "button[data-test='load-more']",
                "button[data-test='show-more-jobs']", 
                ".load-more-button",
                "button:contains('Show more jobs')",
                "button:contains('Voir plus')"
            ]
            
            for load_selector in load_more_selectors:
                try:
                    load_more_btn = WebDriverWait(driver, 8).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, load_selector))
                    )
                    
                    # Scroll to button first
                    driver.execute_script("arguments[0].scrollIntoView(true);", load_more_btn)
                    time.sleep(random.uniform(2, 4))
                    
                    # Click with retry
                    try:
                        load_more_btn.click()
                    except ElementClickInterceptedException:
                        driver.execute_script("arguments[0].click();", load_more_btn)
                    
                    logger.info(f"✅ Clicked 'Load More' button ({load_more_clicks + 1}/{max_load_more_attempts})")
                    load_more_success = True
                    
                    # Wait longer for new content to load
                    time.sleep(random.uniform(1, 4))
                    break
                    
                except (TimeoutException, NoSuchElementException):
                    continue
                except Exception as e:
                    logger.warning(f"Error clicking load more button: {e}")
                    continue
            
            if load_more_success:
                load_more_clicks += 1
            else:
                logger.info(f"ℹ️ No more 'Load More' buttons found for {keyword}")
                break

    except Exception as e:
        logger.error(f"[{keyword}] Error during scraping: {e}")

    finally:
        driver.quit()
    
    logger.info(f"Completed scraping for {keyword}: {len(results)} jobs found")
    return results

def extract_text_multiple_selectors(parent_element, selectors):
    """Try multiple CSS selectors to extract text"""
    for selector in selectors:
        try:
            element = parent_element.find_element(By.CSS_SELECTOR, selector)
            text = element.text.strip()
            if text:
                return text
        except:
            continue
    return ""


# -----------------------
# Data saving utilities
# -----------------------

def save_to_parquet(all_results, output_dir="../../data/bronze/glassdoor_jobs"):
    """Save results to parquet format for silver layer processing"""
    if not all_results:
        logger.warning("No data to save")
        return
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Convert to DataFrame
    df = pd.DataFrame(all_results)
    
    # Add processing metadata
    df['collected_at'] = datetime.now().isoformat()
    
    # Create timestamped filename
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"glassdoor_jobs_{timestamp}.parquet"
    output_path = os.path.join(output_dir, filename)
    
    # Save to parquet
    df.to_parquet(
        output_path,
        engine='pyarrow',
        compression='snappy',
        index=False
    )
    
    logger.info(f"✅ Saved {len(df)} jobs to {output_path}")
    
    # Also save as CSV for backup
    csv_path = output_path.replace('.parquet', '.csv')
    df.to_csv(csv_path, index=False, encoding='utf-8')
    logger.info(f"✅ Also saved as CSV: {csv_path}")

# -----------------------
# Main execution
# -----------------------

if __name__ == "__main__":
    # Tech market focused keywords - TEMP: only test with one keyword
    keywords = [
        "data science",
        "data analysis",
        "data engineering",
        "machine learning",
        "deep learning",
        "artificial intelligence",
        "software engineer",
        "software developer",
        "python developer",
        "javascript developer",
        "java developer",
        "node.js developer",
        "cloud engineer",
        "cloud architect",
        "AI engineer",
        "devops engineer",
        "backend engineer",
        "full stack developer",
        "data architect",
        "business intelligence",
        "ETL developer",
        "big data engineer",
        "computer vision engineer",
        "MLOps engineer"
        "data science"
        ]

    logger.info(f"Starting Glassdoor scraping for {len(keywords)} keywords")
    all_results = []

    max_workers = 6  # Reduced from 20 to 3
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:

        future_to_keyword = {executor.submit(scrape_glassdoor, kw): kw for kw in keywords}
        
        for future in concurrent.futures.as_completed(future_to_keyword):
            keyword = future_to_keyword[future]
            try:
                jobs = future.result()
                all_results.extend(jobs)
                logger.info(f"✅ Completed {keyword}: {len(jobs)} jobs scraped")
                
                time.sleep(random.uniform(1, 4))
                
            except Exception as e:
                logger.error(f"❌ Failed to scrape {keyword}: {e}")

    # Save results
    logger.info(f"🎯 Total jobs scraped: {len(all_results)}")
    
    if all_results:
        save_to_parquet(all_results)
        
        # Print summary
        df = pd.DataFrame(all_results)
        print(f"\n📊 Scraping Summary:")
        print(f"Total jobs: {len(df)}")
        print(f"Keywords processed: {df['keyword'].nunique()}")
        print(f"Unique companies: {df['company'].nunique()}")
        print(f"Jobs with salary info: {df[df['salary'] != '']['salary'].count()}")
        
        print(f"\nTop 10 companies by job count:")
        company_counts = df['company'].value_counts().head(10)
        for company, count in company_counts.items():
            print(f"  {company}: {count}")
            
        print(f"\nTop 10 locations:")
        location_counts = df['location'].value_counts().head(10)
        for location, count in location_counts.items():
            print(f"  {location}: {count}")
    else:
        logger.warning("No jobs were scraped successfully")

    logger.info("✅ Glassdoor scraping completed!")
