-- wrong ouput

-- How many non-holiday weeks had larger sales than the overall average sales
-- during holiday weeks?

--create view table

CREATE VIEW totalWeeklySales AS 
SELECT s.weekdate, SUM(s.weeklysales) AS totalSales, h.isholiday
FROM salesnew s, holidays h
WHERE s.weekdate = h.weekdate
GROUP BY s.weekdate
;

SELECT COUNT (T.weekdate)
FROM totalWeeklySales T
WHERE T.isholiday = 'FALSE'
AND T.totalSales > (SELECT AVG(T.totalSales)
            FROM totalWeeklySales T
            WHERE T.isholiday = 'TRUE'
            )
;
