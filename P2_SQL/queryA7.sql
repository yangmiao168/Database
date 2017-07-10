-- For each of the 4 numeric attributes in TemporalData, are they positively or
-- negatively correlated with sales?

CREATE TABLE Output (AttributeName VARCHAR(20), CorrelationSign INTEGER,  PRIMARY KEY (AttributeName));
INSERT INTO Output (AttributeName, CorrelationSign)
VALUES ('Temperature',

(SELECT 1 AS TemperatureCorr

FROM (

SELECT SUM(product) AS tempCorr

FROM(

SELECT ((SUM(sa.WeeklySales)-AverageSales)*((td.Temperature)-AverageTemperature)) AS product

FROM Sales sa,
TemporalData td,
(SELECT AVG(WeeklySales) AS AverageSales FROM (SELECT SUM(sa.WeeklySales) AS WeeklySales  FROM Sales sa GROUP BY sa.Store, sa.WeekDate)),
(SELECT AVG(Temperature) AS AverageTemperature FROM TemporalData td) 

WHERE (sa.Store = td.Store AND sa.WeekDate = td.WeekDate) 

GROUP BY sa.Store, sa.WeekDate)

)
WHERE tempCorr>0

UNION

SELECT -1 AS TemperatureCorr

FROM (

SELECT SUM(product) AS tempCorr

FROM(

SELECT ((SUM(sa.WeeklySales)-AverageSales)*((td.Temperature)-AverageTemperature)) AS product

FROM Sales sa,
TemporalData td,
(SELECT AVG(WeeklySales) AS AverageSales FROM (SELECT SUM(sa.WeeklySales) AS WeeklySales  FROM Sales sa GROUP BY sa.Store, sa.WeekDate)),
(SELECT AVG(Temperature) AS AverageTemperature FROM TemporalData td) 

WHERE (sa.Store = td.Store AND sa.WeekDate = td.WeekDate) 

GROUP BY sa.Store, sa.WeekDate)

)
WHERE tempCorr<0));

INSERT INTO Output (AttributeName, CorrelationSign)
VALUES ('FuelPrice',

(SELECT 1 AS FuelPriceCorr

FROM (

SELECT SUM(product) AS fuelCorr

FROM(

SELECT ((SUM(sa.WeeklySales)-AverageSales)*((td.FuelPrice)-AverageFuelPrice)) AS product

FROM Sales sa,
TemporalData td,
(SELECT AVG(WeeklySales) AS AverageSales FROM (SELECT SUM(sa.WeeklySales) AS WeeklySales  FROM Sales sa GROUP BY sa.Store, sa.WeekDate)),
(SELECT AVG(FuelPrice) AS AverageFuelPrice FROM TemporalData td) 

WHERE (sa.Store = td.Store AND sa.WeekDate = td.WeekDate) 

GROUP BY sa.Store, sa.WeekDate)

)
WHERE fuelCorr>0

UNION

SELECT -1 AS TemperatureCorr

FROM (

SELECT SUM(product) AS fuelCorr

FROM(

SELECT ((SUM(sa.WeeklySales)-AverageSales)*((td.FuelPrice)-AverageFuelPrice)) AS product

FROM Sales sa,
TemporalData td,
(SELECT AVG(WeeklySales) AS AverageSales FROM (SELECT SUM(sa.WeeklySales) AS WeeklySales  FROM Sales sa GROUP BY sa.Store, sa.WeekDate)),
(SELECT AVG(FuelPrice) AS AverageFuelPrice FROM TemporalData td) 

WHERE (sa.Store = td.Store AND sa.WeekDate = td.WeekDate) 

GROUP BY sa.Store, sa.WeekDate)

)
WHERE fuelCorr<0));

INSERT INTO Output (AttributeName, CorrelationSign)
VALUES ('CPI',

(SELECT 1 AS CPICorr

FROM (

SELECT SUM(product) AS cpiCorr

FROM(

SELECT ((SUM(sa.WeeklySales)-AverageSales)*((td.CPI)-AverageCPI)) AS product

FROM Sales sa,
TemporalData td,
(SELECT AVG(WeeklySales) AS AverageSales FROM (SELECT SUM(sa.WeeklySales) AS WeeklySales  FROM Sales sa GROUP BY sa.Store, sa.WeekDate)),
(SELECT AVG(CPI) AS AverageCPI FROM TemporalData td) 

WHERE (sa.Store = td.Store AND sa.WeekDate = td.WeekDate) 

GROUP BY sa.Store, sa.WeekDate)

)
WHERE cpiCorr>0

UNION

SELECT -1 AS CPICorr

FROM (

SELECT SUM(product) AS cpiCorr

FROM(

SELECT ((SUM(sa.WeeklySales)-AverageSales)*((td.CPI)-AverageCPI)) AS product

FROM Sales sa,
TemporalData td,
(SELECT AVG(WeeklySales) AS AverageSales FROM (SELECT SUM(sa.WeeklySales) AS WeeklySales  FROM Sales sa GROUP BY sa.Store, sa.WeekDate)),
(SELECT AVG(CPI) AS AverageCPI FROM TemporalData td) 

WHERE (sa.Store = td.Store AND sa.WeekDate = td.WeekDate) 

GROUP BY sa.Store, sa.WeekDate)

)
WHERE cpiCorr<0));
INSERT INTO Output (AttributeName, CorrelationSign)
VALUES ('UnemploymentRate',

(SELECT 1 AS UnemploymentRateCorr

FROM (

SELECT SUM(product) AS unemploymentRateCorr

FROM(

SELECT ((SUM(sa.WeeklySales)-AverageSales)*((td.UnemploymentRate)-AverageUnemploymentRate)) AS product

FROM Sales sa,
TemporalData td,
(SELECT AVG(WeeklySales) AS AverageSales FROM (SELECT SUM(sa.WeeklySales) AS WeeklySales  FROM Sales sa GROUP BY sa.Store, sa.WeekDate)),
(SELECT AVG(UnemploymentRate) AS AverageUnemploymentRate FROM TemporalData td) 

WHERE (sa.Store = td.Store AND sa.WeekDate = td.WeekDate) 

GROUP BY sa.Store, sa.WeekDate)

)
WHERE unemploymentRateCorr>0

UNION

SELECT -1 AS UnemploymentRateCorr

FROM (

SELECT SUM(product) AS unemploymentRateCorr

FROM(

SELECT ((SUM(sa.WeeklySales)-AverageSales)*((td.UnemploymentRate)-AverageUnemploymentRate)) AS product

FROM Sales sa,
TemporalData td,
(SELECT AVG(WeeklySales) AS AverageSales FROM (SELECT SUM(sa.WeeklySales) AS WeeklySales  FROM Sales sa GROUP BY sa.Store, sa.WeekDate)),
(SELECT AVG(UnemploymentRate) AS AverageUnemploymentRate FROM TemporalData td) 

WHERE (sa.Store = td.Store AND sa.WeekDate = td.WeekDate) 

GROUP BY sa.Store, sa.WeekDate)

)
WHERE unemploymentRateCorr<0));

SELECT * FROM Output;







