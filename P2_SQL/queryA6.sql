-- Which stores have had sales in every department in that store for every month
--of at least one calendar year among 2010, 2011, and 2012?

SELECT DISTINCT sa.Store
FROM Sales sa

EXCEPT 

SELECT DISTINCT H.BadStore

FROM (
--2010 badstores 
SELECT DISTINCT N.BadStore, N.BadDept

	FROM (SELECT DISTINCT sa.Store AS BadStore, sa.Dept AS BadDept, SUBSTR(sa.WeekDate,6,2) AS Month
      		FROM Sales sa
     		 GROUP BY sa.Store, sa.Dept, Month
      		EXCEPT
      		SELECT DISTINCT sa.Store, sa.Dept, SUBSTR(sa.WeekDate,6,2) AS Month
      		FROM Sales sa
      		WHERE SUBSTR(sa.WeekDate,1,4) = '2010'
      		GROUP BY sa.Store, sa.Dept, Month) AS N

	INTERSECT

--2011 badstores 
	SELECT DISTINCT M.BadStore, M.BadDept
	FROM (SELECT DISTINCT sa.Store AS BadStore, sa.Dept AS BadDept, SUBSTR(sa.WeekDate,6,2) AS Month
      		FROM Sales sa
      GROUP BY sa.Store, sa.Dept, Month
      EXCEPT
      SELECT DISTINCT sa.Store, sa.Dept, SUBSTR(sa.WeekDate,6,2) AS Month
      FROM Sales sa
      WHERE SUBSTR(sa.WeekDate,1,4) = '2011'
      GROUP BY sa.Store, sa.Dept, Month) AS M

INTERSECT

--2012 
SELECT DISTINCT K.BadStore, K.BadDept
FROM (SELECT DISTINCT sa.Store AS BadStore, sa.Dept AS BadDept, SUBSTR(sa.WeekDate,6,2) AS Month
      FROM Sales sa
      GROUP BY sa.Store, sa.Dept, Month
      EXCEPT
      SELECT DISTINCT sa.Store, sa.Dept, SUBSTR(sa.WeekDate,6,2) AS Month
      FROM Sales sa
      WHERE SUBSTR(sa.WeekDate,1,4) = '2012'
      GROUP BY sa.Store, sa.Dept, Month) AS K) AS H;
