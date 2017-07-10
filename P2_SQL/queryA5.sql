-- Get the total sales per month overall for each type of store. Since SQLite3 does
-- not support native operations on the DATE datatype, use the LIKE predicate and
-- the string concatenation operator (“||”) of SQLite3 to create a workaround.

SELECT  st.type, SUBSTR(sa.weekdate,6,2) AS Month, SUM(sa.weeklySales)
FROM salesnew sa, stores st 
WHERE sa.store = st.store
GROUP BY st.type, Month
;
