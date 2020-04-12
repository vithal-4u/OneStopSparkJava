CREATE DATABASE ONESTOP_SPARK_DB;

USE ONESTOP_SPARK_DB;

CREATE TABLE Student_Marks (
    id int,
    name varchar(255),
    nationality varchar(255),
    city varchar(255),
    gender varchar(15),
    age int,
    english_grade float,
    math_grade float,
    sciences_grade float,
    portfolio_rating int,
	coverletter_rating int,
    refletter_rating int
);

insert into ONESTOP_SPARK_DB.Student_Marks (id,name,nationality,city,gender,age,english_grade,math_grade,sciences_grade,portfolio_rating,coverletter_rating,refletter_rating) 
	values (1,'Kiana Lor','China','Suzhou','F',22,3.5,3.7,3.1,4,4,4);

show tables;

Select * from ONESTOP_SPARK_DB.Student_Marks;