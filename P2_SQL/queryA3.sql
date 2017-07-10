-- Get the stores at locations where the unemployment rate exceeded 10% at least
-- once but the fuel price never exceeded 4.
SELECT DISTINCT s.store
FROM stores s
WHERE EXISTS (SELECT temp.store
	          FROM temporaldata temp
	          WHERE s.store = temp.store
	          AND temp.unemploymentrate > 10 
	          )
EXCEPT 
SELECT DISTINCT temp.store 
FROM temporaldata temp
WHERE temp.fuelprice>4
;

