import time
import uuid
import datetime
import re
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager


TECH_KEYWORDS = [
    "python", "java", "c++", "c#", "aws", "azure", "gcp", "sql",
    "mongodb", "docker", "kubernetes", "spark", "hadoop",
    "node", "react", "vue", "django", "flask", "tensorflow", "pytorch"
]

def extract_technologies(text):
    text = text.lower()
    return list({tech for tech in TECH_KEYWORDS if re.search(r"\b" + re.escape(tech) + r"\b", text)})

options = Options()
options.headless = True
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

url = "https://stackoverflowjobs.com"
driver.get(url)
wait = WebDriverWait(driver, 3)
time.sleep(3)

jobs_data = []
i = 0

while i < 1:

    job_cards = driver.find_element(By.ID, "job-list")
    job_cards = job_cards.find_elements(By.TAG_NAME, "li")

    for card in job_cards:
        try:
            card.click()
            time.sleep(1)


            panel = driver.find_element(By.ID, "right-pane-content")
            full_text = panel.text.lower()

            title = panel.find_element(By.CSS_SELECTOR, "h1").text.strip() if panel.find_elements(By.CSS_SELECTOR, "h1") else None

            location = None
            loc_elt = panel.find_elements(By.XPATH, ".//*[contains(text(),'Location') or contains(text(),'Remote')]")
            if loc_elt:
                location = loc_elt[0].text.strip()

            salary = None
            sal_elt = re.search(r"\$\d[\d,\s]*(?:k)?", panel.text)
            if sal_elt:
                salary = sal_elt.group()

            date_posted = None
            dt_elt = panel.find_elements(By.XPATH, ".//time")
            if dt_elt and dt_elt[0].get_attribute("datetime"):
                date_posted = dt_elt[0].get_attribute("datetime")

            technologies = extract_technologies(full_text)

            jobs_data.append({
                "id": str(uuid.uuid4()),
                "title": title,
                "location": location,
                "salary": salary,
                "date_posted": date_posted,
                "technologies": technologies,
                "scraped_at": datetime.datetime.utcnow().isoformat()
            })
        except Exception as e:
            print("Erreur sur une carte :", e)
            continue

    try:
        next_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '[aria-label="Next page"]')))
        driver.execute_script("arguments[0].click();", next_button)
        time.sleep(2)
        i += 1
    except:
        print("Fin de la pagination.")
        break

driver.quit()

# Export Parquet
df = pd.DataFrame(jobs_data)
df.to_parquet("../../../data/bronze/Jobs_feed/archived_web_scraped/stackoverflow_jobs.parquet", index=False)
print(f"{len(df)} jobs exportés.")
