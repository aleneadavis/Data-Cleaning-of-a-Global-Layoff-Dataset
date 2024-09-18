-- Data Cleaning

SELECT *
FROM layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or Blank Values
-- 4. Remove Any Columns or Rows

-- created a staging table to work off of instead of working on the raw data
CREATE TABLE layoffs_staging 
LIKE layoffs;

SELECT *
FROM layoffs_staging;  

INSERT layoffs_staging
SELECT *
FROM layoffs;

-- finding any duplicates
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num  
FROM layoffs_staging;

-- duplicates is identfied but double checking to verify they are actually duplicates
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num  
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- this shows there actually wasn't a dulipcate when it comes to Oda, they are very similar but not duplicates
SELECT *
FROM layoffs_staging
WHERE company = 'Oda';

-- adding more parimeters to the duplicate query to get a more accurate answer
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, `date`, stage,
country, funds_raised_millions) AS row_num  
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- noticed Oda is not in the results anymore
-- double check again

SELECT *
FROM layoffs_staging
WHERE company = 'Casper';

-- now we want to remove the duplicate but we need to remove the right one
-- creating another table with row identifer

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location,
industry, total_laid_off, percentage_laid_off, `date`, stage,
country, funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

-- Standardizing Data , finding issues in the data and fixing it
-- first going to start by taking the white space off by using TRIM
SELECT company, TRIM(company) -- first see how it looks by comparing
FROM layoffs_staging2;

-- update the data
UPDATE layoffs_staging2 
SET company = TRIM(company);

-- using distinct to only pull unique varibles
SELECT DISTINCT industry 
FROM layoffs_staging2
ORDER BY 1; 

-- the blank answers and the three crypto answers is concerning
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

-- the majority of the industry is just Crypto so updating all crypto industry to just Crypto
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- check to see if it went through
-- company and industry is done, looking at location now
SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1; -- looks good

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1; -- one of the united states has a period

SELECT *
FROM layoffs_staging2
WHERE country LIKE 'United States%'
ORDER BY 1;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%'; -- it is fixed now

-- looking at the data type and DATE is using text but it should be using date
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y')
;

-- the above query only changed teh date format, now chaning the data type of date

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE; -- check to data type to see if it changed

-- looking at the null and blank values
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

SELECT * 
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL; -- this did not work because if you run the one above it, it still shows blanks, could be because they are not null and is blank instead

UPDATE layoffs_staging2
SET industry = NULL 
WHERE industry = ''; -- now it shows null instead of blank space

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- looking at the rows that has a null for both total_laid_off and percentage_laid_off 
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

-- this is not needed anymore
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

