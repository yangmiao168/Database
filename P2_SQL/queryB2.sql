-- List all people (their first and last name) who have directed and played in the
-- same movie in 2000.

SELECT a.fname, a.lname
FROM actor a, movie m, directors d, casts c, movie_directors md
WHERE a.id=c.pid
AND m.id = c.mid
AND d.id = md.did 
AND m.id = md.mid 
AND year = 2000
AND a.fname = d.fname
AND a.lname = d.lname
;
