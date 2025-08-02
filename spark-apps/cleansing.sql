SELECT *
FROM input_table
WHERE title IS NOT NULL AND TRIM(title) != ''
  AND release_year BETWEEN 1900 AND 2025
  AND TRIM(country) != ''
  AND TRIM(genres) != ''
  AND TRIM(actors) != ''
  AND TRIM(directors) != ''
  AND TRIM(composers) != ''
  AND TRIM(screenwriters) != ''
  AND TRIM(cinematographer) != ''
  AND TRIM(production_companies) != ''

