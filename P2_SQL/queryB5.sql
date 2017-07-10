--A decade is any sequence of 10 consecutive years (e.g., 1964, 1965, ..., 1973 is
--a decade). Find the decade with the largest number of films (output only the first
--year of the decade).

SELECT yearList.year, SUM(MoviePerYear.numMovies) as totalMovies

FROM (SELECT distinct year FROM movie) AS yearList,

(SELECT year,COUNT(movie.id) AS numMovies FROM movie
	GROUP BY year) AS MoviePerYear

WHERE MoviePerYear.year >= yearList.year AND MoviePerYear.year < (yearList.year+10)
GROUP BY yearList.year
HAVING SUM(MoviePerYear.numMovies)>= MAX(
(SELECT SUM(MoviePerYear.numMovies) FROM (SELECT distinct year FROM movie) AS yearList,
(SELECT year,COUNT(movie.id) AS numMovies FROM movie
	GROUP BY year) AS MoviePerYear
WHERE MoviePerYear.year >= yearList.year AND MoviePerYear.year < (yearList.year+10)
GROUP BY yearList.year))
ORDER BY totalMovies DESC
LIMIT 1
;
