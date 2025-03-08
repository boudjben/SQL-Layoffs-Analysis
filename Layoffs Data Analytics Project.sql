-- ========================================================
-- SQL LAYOFFS DATA ANALYSIS PROJECT
-- ========================================================
-- Project Goal: Clean and analyze the layoffs dataset
-- Tool: MySQL
-- ========================================================

-- ========================================================
-- I. DATA CLEANING
-- ========================================================

-- ========================================================
--  I.1- Removing Duplicates
-- ========================================================

-- Load the data
SELECT * FROM layoffs;

-- Rename the table for clarity
ALTER TABLE layoffs RENAME TO Layoffs_raw;

-- Create a cleaned table with the same structure
CREATE TABLE Layoffs_cleaned LIKE layoffs_raw;
INSERT INTO Layoffs_cleaned SELECT * FROM layoffs_raw;
SELECT * FROM Layoffs_cleaned;



-- Identify duplicates using  Windows function ROW_NUMBER
WITH CTE_layoffs AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
    ) AS Count_rows
    FROM Layoffs_cleaned
)
SELECT * FROM CTE_layoffs WHERE Count_rows > 1 ORDER BY company;

-- Create a staging table without duplicates
CREATE TABLE layoffs_cleaned2 (
    company TEXT,
    location TEXT,
    industry TEXT,
    total_laid_off INT,
    percentage_laid_off TEXT,
    `date` TEXT,
    stage TEXT,
    country TEXT,
    funds_raised_millions INT,
    Count_row INT
);

INSERT INTO layoffs_cleaned2
SELECT *, ROW_NUMBER() OVER (
    PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
) AS Count_rows
FROM Layoffs_cleaned;

-- Remove duplicates
DELETE FROM layoffs_cleaned2 WHERE Count_row > 1;

-- Drop Count_row column after removing duplicates
ALTER TABLE layoffs_cleaned2 DROP COLUMN Count_row;

-- ========================================================
-- I.2- Data Standardization
-- ========================================================

-- Trim whitespace from company names
UPDATE layoffs_cleaned2 SET company = TRIM(company);

-- Standardize industry names
UPDATE layoffs_cleaned2 SET industry = 'Crypto' WHERE industry LIKE 'Crypto%';

-- Standardize country names
UPDATE layoffs_cleaned2 SET country = 'United States' WHERE country LIKE 'United States%';

-- Rename the date column
ALTER TABLE layoffs_cleaned2 RENAME COLUMN `date` TO Layoff_Date;

-- Convert Layoff_Date from text to DATE format
UPDATE layoffs_cleaned2 SET Layoff_Date = STR_TO_DATE(Layoff_Date, '%m/%d/%Y');
ALTER TABLE layoffs_cleaned2 MODIFY COLUMN Layoff_Date DATE;

-- ========================================================
-- I.3- Handling NULL and Blank Values
-- ========================================================

-- Replace empty industry values with NULL
UPDATE layoffs_cleaned2 SET industry = NULL WHERE industry = '';

-- Fill missing industry values using data from the same company using JOIN function
UPDATE layoffs_cleaned2 T1
JOIN layoffs_cleaned2 T2 ON T1.company = T2.company AND T1.location = T2.location
SET T1.industry = T2.industry
WHERE T1.industry IS NULL AND T2.industry IS NOT NULL;

-- Remove rows where total_laid_off and percentage_laid_off are both NULL
DELETE FROM layoffs_cleaned2 WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

-- ========================================================
-- II. EXPLORATORY DATA ANALYSIS (EDA)
-- ========================================================

-- 1. Basic Summary
SELECT * FROM layoffs_cleaned2;
SELECT MAX(total_laid_off), MAX(percentage_laid_off) FROM layoffs_cleaned2;

-- 2. Top 5 Companies with the Most Layoffs
SELECT company, SUM(total_laid_off) AS Sum_layoff
FROM layoffs_cleaned2
GROUP BY company
ORDER BY Sum_layoff DESC
LIMIT 5;

-- 3. Top 5 Countries with the Most Layoffs
SELECT country, SUM(total_laid_off) AS Sum_layoff
FROM layoffs_cleaned2
GROUP BY country
ORDER BY Sum_layoff DESC
LIMIT 5;

-- 4. Year with the Most Layoffs
SELECT YEAR(Layoff_Date) AS Year_layoff, SUM(total_laid_off) AS Sum_layoff
FROM layoffs_cleaned2
WHERE YEAR(Layoff_Date) IS NOT NULL
GROUP BY Year_layoff
ORDER BY Sum_layoff DESC;

-- 5. Monthly Layoffs Trend
SELECT SUBSTRING(Layoff_Date, 1, 7) AS Layoff_month, SUM(total_laid_off) AS Total_laid
FROM layoffs_cleaned2
WHERE SUBSTRING(Layoff_Date, 1, 7) IS NOT NULL
GROUP BY Layoff_month
ORDER BY Layoff_month ASC;

-- 6. Cumulative Monthly Layoffs
WITH Rolling_CTE AS (
    SELECT SUBSTRING(Layoff_Date, 1, 7) AS Layoff_month, SUM(total_laid_off) AS Total_laid
    FROM layoffs_cleaned2
    WHERE SUBSTRING(Layoff_Date, 1, 7) IS NOT NULL
    GROUP BY Layoff_month
    ORDER BY Layoff_month ASC
)
SELECT Layoff_month, Total_laid, SUM(Total_laid) OVER (ORDER BY Layoff_month) AS Rolling_total
FROM Rolling_CTE;

-- 7. Top 5 Companies with the Most Layoffs Per Year
WITH Company_CTE AS (
    SELECT company, YEAR(Layoff_Date) AS Year_layoff, SUM(total_laid_off) AS Total
    FROM layoffs_cleaned2
    WHERE YEAR(Layoff_Date) IS NOT NULL
    GROUP BY company, Year_layoff
    ORDER BY Total DESC
), Company_CTE_RANKING AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY Year_layoff ORDER BY Total DESC) AS Ranking
    FROM Company_CTE
)
SELECT * FROM Company_CTE_RANKING WHERE Ranking <= 5;

-- ========================================================
-- End of Project
-- ========================================================
