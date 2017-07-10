--Which stores had the largest overall sales during holiday weeks?

SELECT store, SUM(weeklysales) AS OverAllSales
FROM salesnew s, holidays h
WHERE s.weekdate = h.weekdate 
AND isholiday = 'TRUE'
GROUP BY s.store
ORDER BY OverAllSales DESC
LIMIT 1;


--Which stores had the smallest overall sales during holiday weeks?

SELECT store, SUM(weeklysales) AS OverAllSales
FROM salesnew s, holidays h
WHERE s.weekdate = h.weekdate 
AND isholiday = 'TRUE'
GROUP BY s.store
ORDER BY OverAllSales ASC
LIMIT 1;




