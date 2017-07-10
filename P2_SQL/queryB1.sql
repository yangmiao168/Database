-- List all actors (their first and last name) who have played in at least 10 different
-- movies in 2010.
CREATE UNIQUE INDEX movieid ON movie(id);
CREATE UNIQUE INDEX actorid ON actor(id);
CREATE UNIQUE INDEX directorsid ON directors(id);
CREATE INDEX castsmid ON casts(mid);
CREATE INDEX castspid ON casts(pid);
CREATE INDEX movieyear ON movie(year);
CREATE INDEX moviedirectorsmid ON movie_directors(mid);
CREATE INDEX moviedirectordid ON movie_directors(did);


SELECT a.fname, a.lname, COUNT(m.id) AS N
FROM actor a, casts c, movie m
WHERE a.id = c.pid
AND   m.id = c.mid
AND  year = 2010
GROUP BY a.id, a.fname, a.lname
HAVING N >= 10
ORDER BY N DESC;

