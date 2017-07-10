--Find the number of actors with Bacon number 2.

SELECT COUNT(distinct c1.pid) 
FROM casts c1
WHERE c1.mid 
IN (SELECT c2.mid FROM casts c2 WHERE c2.pid 
	IN (SELECT c3.pid FROM casts c3 WHERE c3.mid 
		IN (SELECT c4.mid FROM casts c4, actor a 
		WHERE c4.pid = a.id AND a.fname = 'Kevin' AND a.lname = 'Bacon')))
AND c1.pid NOT IN (SELECT c2.pid FROM casts c2 WHERE c2.mid
        IN(SELECT c3.mid FROM casts c3, actor a 
        	WHERE c3.pid= a.id AND a.fname = 'Kevin' AND a.lname = 'Bacon'))
;