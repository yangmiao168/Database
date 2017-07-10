/* 
 * Drop existing tables if any exists.
*/
PRAGMA foreign_keys=OFF;
DROP TABLE IF EXISTS user;
DROP TABLE IF EXISTS administrator;
DROP TABLE IF EXISTS normalUser;
DROP TABLE IF EXISTS publisher;
DROP TABLE IF EXISTS book;
DROP TABLE IF EXISTS author;
DROP TABLE IF EXISTS bookSeller;
DROP TABLE IF EXISTS donor;
DROP TABLE IF EXISTS sells;
DROP TABLE IF EXISTS writes;
DROP TABLE IF EXISTS donates;
DROP TABLE IF EXISTS view;


-- create user table
CREATE Table user (
email varchar(255) Primary key,
password varchar(255),
username varchar(255),
gender varchar(255));


-- Insert user table values
Insert into user values ("group8@wisc.edu", "huang33j","group8","male");
Insert into user values ("group7@wisc.edu", "huang33j","group7","female");
--Show table content 
--SELECT *from user;
--group8@wisc.edu|huang33j|group8|male
--group7@wisc.edu|huang33j|group7|female

-- Administrator table and normalUser table are subclass of user table
-- Create administrator table
CREATE Table administrator (
email varchar(255) Primary key,
phone_number Integer,
address varchar(255));

-- Insert values into administrator
Insert into administrator values ("group8@wisc.edu", 6085091111, "4999 Sheboygan Ave");
-- Show content of the administrator table
-- SELECT *from administrator;
--group8@wisc.edu|6085091111|4999 Sheboygan Ave

-- Create normalUser table
CREATE Table normalUser (
email varchar(255) Primary key,
social_media varchar(255));

-- Insert values into normalUser table 
Insert into normalUser values ("group7@wisc.edu", "Facebook");
-- Show content in normalUser table
-- SELECT *from normalUser;
-- group7@wisc.edu|Facebook

-- Create publisher table
CREATE Table publisher (
name varchar(255) Primary key,
address varchar(255));


-- Insert values into publisher table
Insert into publisher values ("Penguin Books", "1415 Engineering Drive");
Insert into publisher values ("UW press", "100 University Ave");
-- Show content of publisher table
-- SELECT * from publisher;
-- group8|1415 Engineering Drive

-- Create book table and set many-to-one relationship with publisher table
CREATE Table book (
ISBN Integer,
title varchar(255),
genre varchar(255),
name varchar(255),
PRIMARY key(ISBN),
FOREIGN KEY(name) REFERENCES publisher(name) ON DELETE CASCADE);

-- This enforces foreign key constraints 
PRAGMA foreign_keys=ON;
-- Insert values into book table         
Insert into book values (1234567653, "the Originals","Novel","Penguin Books");
Insert into book values (8888888888, "Big Data", "Science", "UW press");
-- Show content of book table
-- SELECT * from book;
-- 1234567653|the Originals|Novel|Penguin Books

-- Create author table
CREATE Table author (
ssn Integer Primary key,
address varchar(255),
name varchar(255));

-- Insert values into author table   
Insert into author values (343443555, "1324 CS Drive","Kevin Williamson");
Insert into author values (666666666, "888 eagle heights", "Mike");
-- Show content of author table
-- SELECT * from author;
-- 343443555|1324 CS Drive|Kevin Williamson

-- Create bookSeller table
CREATE Table bookSeller (
ID Integer Primary key,
name varchar(255),
address varchar(255)); 

      
-- Insert values into bookSeller table    
Insert into bookSeller values (58473, "Nick","4702 Sheboygan Ave");
-- Show content of the bookSeller table
-- SELECT * from bookSeller;
-- 58473|Nick|4702 Sheboygan Ave

-- Create donor table
CREATE Table donor (
donorID Integer Primary key,
name varchar(255));


-- Insert values into donor table         
Insert into donor values (54443233, "Jack");
-- Show content of donor table
-- SELECT * from donor;
-- 54443233|Jack



-- Set Up many-to-many relationship
-- Set many-to-many relationship between book table and bookSeller table
-- Create sells table
CREATE Table sells(
ISBN Integer,
ID Integer,
PRIMARY KEY(ISBN, ID),
FOREIGN KEY(ID) REFERENCES bookSeller(ID) ON DELETE CASCADE,
FOREIGN KEY(ISBN) REFERENCES book(ISBN) ON DELETE CASCADE);


-- This enforces foreign key constraints 
PRAGMA foreign_keys=ON;
-- Insert values into sells table
Insert into sells values(1234567653, 58473);
Insert into sells values(8888888888, 58473);
-- Show content of sells table
-- select *from sells;
-- 1234567653|58473

-- Set many-to-many relationship between book table and author table
-- Create writes table
CREATE Table writes(
ISBN Integer,
ssn Integer,
PRIMARY KEY(ISBN, ssn),
FOREIGN KEY(ISBN) REFERENCES book(ISBN) ON DELETE CASCADE,
FOREIGN KEY(ssn) REFERENCES author(ssn) ON DELETE CASCADE);

-- This enforces foreign key constraints 
PRAGMA foreign_keys=ON;
-- Insert values into writes table
Insert into writes values(1234567653, 343443555);
Insert into writes values(8888888888, 666666666);
-- Show content of writes table
-- select *from writes;
-- 1234567653|343443555

-- Set many-to-many relationship between donor table and book table
-- Create donates table
CREATE Table donates(
donorID Integer,
ISBN Integer,
PRIMARY KEY(donorID, ISBN),
FOREIGN KEY(donorID) REFERENCES donor(donorID) ON DELETE CASCADE,
FOREIGN KEY(ISBN) REFERENCES book(ISBN) ON DELETE CASCADE);


-- This enforces foreign key constraints 
PRAGMA foreign_keys=ON;
-- Insert values into donates table
Insert into donates values(54443233, 1234567653);
Insert into donates values(54443233, 8888888888);

-- Show content of donates table
-- select *from donates;
-- 54443233|1234567653

-- Set many-to-many relationship between book table and user table
-- Create view table
CREATE Table views (
email varchar(255),
ISBN Integer,
primary key(email,ISBN),
foreign key (ISBN) references book(ISBN) ON DELETE CASCADE,
foreign key (email) references user(email) ON DELETE CASCADE);

-- This enforces foreign key constraints 
PRAGMA foreign_keys=ON;
-- Insert values into view tables       
insert into views values("group8@wisc.edu", 1234567653);
insert into views values("group7@wisc.edu", 8888888888);
-- Show content of view table
-- select * from view;
-- group8@wisc.edu|1234567653
-- group7@wisc.edu|1234567653






