# MKDB

MKDB is a database that no one should use.

## Tour

### Run Tests

```shell
make test
```

### Start Console Session

```shell
make run-cli
```

### Run Queries

#### Basic CRU~~D~~ Operations

```sql
CREATE DATABASE testdb;

USE testdb;

CREATE TABLE family (
    name varchar(255),
    age int,
    hair varchar(255)
);

CREATE TABLE famous_lines (
    name varchar(255),
    quote varchar(255),
    season int
);

CREATE TABLE season (
    number int,
    year int
);

INSERT INTO family (name, age, hair) VALUES
    ("Walter", 50, "bald"),
    ("Skyler", 40, "blonde"),
    ("Walter Jr.", 16, "brown"),
    ("Holly", 1, "bald");

INSERT INTO season (number, year) VALUES
    (1, 2008),
    (2, 2009),
    (3, 2010),
    (4, 2011),
    (5, 2012);

INSERT INTO famous_lines (name, quote, season) VALUES
    ("Walter", "Chemistry is, well technically, chemistry is the study of matter. But I prefer to see it as the study of change.", 1),
    ("Skyler", "Walt, the Mastercard's the one we don't use.", 1),
    ("Walter", "Oh, yes. Now we just need to figure out a delivery device, and then no more Tuco.", 2),
    ("Walter", "How was I supposed to know you were chauffeuring Tuco to my doorstep?", 2),
    ("Skyler", "We have discussed everything we need to discuss... I thought I made myself very clear.", 3);

SELECT f.name, quote, year
FROM family f
JOIN famous_lines fl ON fl.name = f.name
JOIN season s ON s.number = fl.season
WHERE hair = "bald";

SELECT f.name, quote, year
FROM family f
LEFT JOIN famous_lines fl ON fl.name = f.name
LEFT JOIN season s ON s.number = fl.season
ORDER BY f.name;

SELECT quote, year
FROM famous_lines fl
RIGHT JOIN season s ON s.number = fl.season;

SELECT *
FROM family
LIMIT 2 OFFSET 2;

UPDATE family SET age = 2, hair = "blonde" WHERE name = "Holly";
```

#### View All Tables

```sql
SELECT table_name, page_id FROM sys_pages;
```

#### View All Schemas

```sql
SELECT table_name, field_name, field_length, field_type FROM sys_schema;
```

### Clean Up

```shell
make clean
```
