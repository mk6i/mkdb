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
    hair varchar(255),
);

INSERT INTO family (name, age, hair) VALUES ("Walter", 50, "bald");
INSERT INTO family (name, age, hair) VALUES ("Skyler", 40, "blonde");
INSERT INTO family (name, age, hair) VALUES ("Walter Jr.", 16, "brown");
INSERT INTO family (name, age, hair) VALUES ("Holly", 1, "bald");

SELECT name, age, hair FROM family;

SELECT * FROM family WHERE hair = "bald";
SELECT * FROM family WHERE age = 16 OR age = 1;
SELECT * FROM family WHERE hair = "bald" AND age = 1;
SELECT * FROM family WHERE hair != "bald";

UPDATE family SET age = 2, hair = "blonde" WHERE name = "Holly";
SELECT * FROM family WHERE name = "Holly";
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
