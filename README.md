<p align="center"><img width="742" alt="mkdb console" src="https://user-images.githubusercontent.com/2894330/232939901-d92bf2e4-b4d0-4aa4-80a9-70ecf8a9753e.png"></p>

**mkdb** is a SQL-based relational database management system (RDBMS) written in Golang (1.18+) with zero third-party
dependencies. The goal of the project is to provide a creative outlet for developers who want to experiment with
database development in a low-stakes environment.

The goal of this project is largely inspired by [SerenityOS](https://serenityos.org/).

## 🛠️ Features

- [Recursive-descent](https://en.wikipedia.org/wiki/Recursive_descent_parser) SQL parser that loosely follows
  the [SQL-92 grammar](https://ronsavage.github.io/SQL/sql-92.bnf.html).
- Typical SQL operations:
    - DQL & DML: `SELECT`, `DELETE`, `INSERT`, `UPDATE`
    - DDL: `CREATE DATABASE`, `CREATE TABLE`
    - Joining: `LEFT JOIN`, `RIGHT JOIN`, `INNER JOIN`
    - Aggregation: `GROUP BY`, `COUNT(...)`, `AVG(...)`
    - Ordering & Limiting: `ORDER BY`, `LIMIT`
    - Conditional clauses and boolean expressions: `WHERE`, `AND`, `OR`
- On-disk [B+ tree](https://en.wikipedia.org/wiki/B%2B_tree).
  -  Table rows are limited to 409 bytes in size.
- Basic data durability properties:
    - Write-ahead logging [(WAL)](https://en.wikipedia.org/wiki/Write-ahead_logging).
    - Page cache
      with [`NO FORCE`](http://www.cs.rpi.edu/~sibel/csci4380/spring2016/course_notes/transactions_durability.html#no-force), [`NO STEAL`](http://www.cs.rpi.edu/~sibel/csci4380/spring2016/course_notes/transactions_durability.html#no-steal)
      semantics.
        - By design, the database terminates on `INSERT` when the page cache is completely full of dirty pages. 🙃

## 🔎 Quick Start

**1. Clone the repo**

```shell
git clone https://github.com/mk6i/mkdb.git && cd mkdb/
```

**2. Start a console session**

> To run mkdb, you'll need to install [golang](https://go.dev/doc/install).

```shell
go run ./cmd/console
```

**3. Set up the database and tables**

Run the following queries inside the SQL terminal to set up a database, table, and some data.

```sql
CREATE
DATABASE testdb;

USE
testdb;

CREATE TABLE weather
(
    hour         int,
    city         varchar(255),
    temp         int,
    rel_humidity int
);

INSERT INTO weather (hour, city, temp, rel_humidity)
VALUES (10, 'New York City', 71, 45),
       (12, 'New York City', 84, 50),
       (12, 'San Francisco', 72, 45),
       (12, 'Austin', 90, 40),
       (14, 'New York City', 87, 65),
       (14, 'San Francisco', 75, 60),
       (14, 'Austin', 95, 42),
       (18, 'New York City', 64, 70),
       (18, 'San Francisco', 55, 50),
       (18, 'Austin', 85, 45),
       (20, 'Austin', 79, 40);
```

Run this query to calculate the average temperature and relative humidity per city:

```sql
SELECT city, avg(temp) as avg_temp, avg(rel_humidity)
FROM weather
GROUP BY city;
```

**4. Cleanup**

Ready for a clean slate? The following command clears out the database and its associated files.

```shell
make clean
```

## 🧭 Roadmap

New SQL features will be added on an ad-hoc basis. 

The following engine features will be worked on in 2023:

- B+ Tree indexes
- Non-concurrent transactions
- Client-server mode
- [`[STEAL]`](http://www.cs.rpi.edu/~sibel/csci4380/spring2016/course_notes/transactions_durability.html#steal) semantics

## 🙌 Contributing

Pull requests welcome!

The following resources can help get you up to speed on concepts relevant to developing a database:

- [*Database Internals*](https://www.amazon.com/Database-Internals-Deep-Distributed-Systems/dp/1492040347), Alex Petrov, 2019
  -  An excellent resource for learning about how datastore storage engines work at high-level. The B+ Tree is based on the descriptions in the book.
- [*Crafting Interpreters*](https://craftinginterpreters.com/), Robert Nystrom, 2021
  - The recursive-descent SQL parser is based on the techniques described in this free book.
- *Inside SQLite*, Sibsankar Haldar, 2007
  - Accessible literature on the inner workers in SQLite. Currently out of print, but can be found on the high seas.

While developing, please respect to these two rules:

- Do not introduce any 3rd-party dependencies. ([`golang.org/x`](https://pkg.go.dev/golang.org/x) packages are welcome, however.) Re-inventing the wheel is encouraged in the name of learning.
- Do not base features/fixes on existing open-source database code. Learn what you can by reading the abundant technical database literature available online.

## 📄  License

mkdb is licensed under the MIT license.
