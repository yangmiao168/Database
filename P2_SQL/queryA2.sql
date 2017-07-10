-- Get the top 20 departments overall ranked by total sales normalized by the
-- size of the store where the sales were recorded. Output the department and the
-- normalized total sales.

SELECT sa.dept, SUM(sa.weeklysales/st.size) AS normalizedTotalSales
FROM salesnew sa, stores st
WHERE sa.store = st.store
GROUP BY sa.dept
ORDER BY normalizedTotalSales DESC
LIMIT 20;
