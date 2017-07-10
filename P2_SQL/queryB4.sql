-- For each year, count the number of movies in that year that had only female
-- actors. (A movie without any actors is a movie that has only female actors).

SELECT m.year,COUNT(*) AS numMovies
	FROM movie m
WHERE m.id NOT IN(
	SELECT m.id
	FROM actor a, movie m, casts c
     WHERE a.id = c.pid
     AND m.id = c.mid
	 AND a.gender = 'M'
	 )
GROUP BY m.year
;




