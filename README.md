# Challenge_DL_and_DI
CREATE TABLE silver.Trends (
    id INT AUTO_INCREMENT PRIMARY KEY,
    country_code VARCHAR(2) NOT NULL,
    date DATE NOT NULL,
    keyword VARCHAR(100) NOT NULL,
    search_frequency INT NOT NULL,
    country VARCHAR(50) NOT NULL,
    INDEX idx_country_code (country_code),
    INDEX idx_date (date),
    INDEX idx_keyword (keyword)
);

CREATE TABLE average_salaries (
    id INT PRIMARY KEY AUTO_INCREMENT,
    average_salary DECIMAL(10,2) NOT NULL,
    country VARCHAR(100) NOT NULL,
    date DATE NOT NULL,
    job_title VARCHAR(100) NOT NULL,
    country_code VARCHAR(10) NOT NULL,
    processed_at TIMESTAMP NOT NULL
);

CREATE TABLE job_statistics (
    id INT PRIMARY KEY AUTO_INCREMENT,
    country VARCHAR(100) NOT NULL,
    date DATE NOT NULL,
    job_title VARCHAR(100) NOT NULL,
    job_count DECIMAL(10,1) NOT NULL,
    salary_range INT NOT NULL,
    country_code VARCHAR(10) NOT NULL,
    processed_at TIMESTAMP NOT NULL
);

CREATE INDEX idx_avg_salaries_country_date ON average_salaries(country, date);
CREATE INDEX idx_avg_salaries_job_title ON average_salaries(job_title);
CREATE INDEX idx_job_stats_country_date ON job_statistics(country, date);
CREATE INDEX idx_job_stats_salary_range ON job_statistics(salary_range);