CREATE TABLE sales (store INTEGER, dept INTEGER, weekdate DATE, weeklysales REAL, isholiday BOOLEAN, type CHAR(1), size INTEGER, temperature REAL, fuelprice REAL, cpi REAL, unemploymentrate REAL, PRIMARY KEY (store, dept, weekdate));
CREATE TABLE holidays (weekdate DATE PRIMARY KEY, isholiday BOOLEAN);
CREATE TABLE stores (store INTEGER PRIMARY KEY, type CHAR(1), size INTEGER);
CREATE TABLE temporaldata (store INTEGER, weekdate DATE, temperature REAL, fuelprice REAL, cpi REAL, unemploymentrate REAL, PRIMARY KEY (store, weekdate), FOREIGN KEY(store) REFERENCES stores(store), FOREIGN KEY(weekdate) REFERENCES holidays(weekdate));
CREATE TABLE salesnew (store INTEGER, dept INTEGER, weekdate DATE, weeklysales REAL, PRIMARY KEY (store, dept, weekdate), FOREIGN KEY(store) REFERENCES stores(store), FOREIGN KEY(weekdate) REFERENCES holidays(weekdate), FOREIGN KEY(store, weekdate) REFERENCES temporaldata(store, weekdate));
.mode csv
.import "SalesFile.dat" sales
INSERT INTO holidays SELECT DISTINCT weekdate, isholiday FROM sales;
INSERT INTO stores SELECT DISTINCT store, type, size FROM sales;
INSERT INTO temporaldata SELECT DISTINCT store, weekdate, temperature, fuelprice, cpi, unemploymentrate FROM sales;
INSERT INTO salesnew SELECT DISTINCT store, dept, weekdate, weeklysales FROM sales;
