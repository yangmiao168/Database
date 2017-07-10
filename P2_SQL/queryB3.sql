-- List the top 100 directors who have directed the most movies from 1990 to 2010,
-- in descending order of the number of movies they have directed. Output their first
-- name, last name, and number of movies directed.

SELECT d.fname, d.lname, COUNT(m.id) AS N 
FROM directors d, movie m, movie_directors md
WHERE d.id = md.did 
AND m.id = md.mid
AND year >= 1990
AND year <= 2010
GROUP BY d.id, d.fname, d.lname
ORDER BY N DESC 
LIMIT 100;
;