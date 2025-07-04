# Challenge_DL_and_DI
-- Création de la table search_frequency
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

CREATE TABLE silver.it_jobs_salary (
    id INT PRIMARY KEY AUTO_INCREMENT,
    country VARCHAR(100) NOT NULL,
    country_code VARCHAR(10) NOT NULL,
    job_title VARCHAR(100) NOT NULL,
	date DATE NOT NULL
);

CREATE TABLE salary_it_jobs (
    id INT PRIMARY KEY AUTO_INCREMENT,
    country VARCHAR(100) NOT NULL,
    country_code VARCHAR(10) NOT NULL,
	job_title VARCHAR(100) NOT NULL,
    average_salary DECIMAL(10,2) NOT NULL,
    date DATE NOT NULL
);