-- Imports the IMDB movie dataset

PRAGMA foreign_keys = ON;

CREATE TABLE actor(
  id int PRIMARY KEY,
  fname varchar(30),
  lname varchar(30),
  gender char(1));

CREATE TABLE movie(
  id int PRIMARY KEY,
  name varchar(150),
  year int);

CREATE TABLE directors(
  id int PRIMARY KEY,
  fname varchar(30),
  lname varchar(30));

CREATE TABLE genre(
  mid int,
  genre varchar(50));

CREATE TABLE  casts(
  pid int REFERENCES actor,
  mid int REFERENCES movie,
  role varchar(50));

CREATE TABLE  movie_directors(
  did int REFERENCES directors,
  mid int REFERENCES movie);

.import 'actor.txt' actor
.import 'movie.txt' movie
.import 'casts.txt' casts
.import 'directors.txt' directors
.import 'genre.txt' genre
.import 'movie_directors.txt' movie_directors
