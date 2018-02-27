# REST Layer PostgreSQL Backend

[![godoc](http://img.shields.io/badge/godoc-reference-blue.svg?style=flat)](https://godoc.org/github.com/safad/rest-layer-pgsql) [![license](http://img.shields.io/badge/license-MIT-red.svg?style=flat)](https://raw.githubusercontent.com/safad/rest-layer-pgsql/master/LICENSE)

This [REST Layer](https://github.com/rs/rest-layer) resource storage backend stores data in a PostgreSQL Database using [database/sql](https://godoc.org/database/sql) and [pq](https://github.com/lib/pq).

# UNDER HEAVY DEVELOPMENT BEWARE TO NOT BE USED ONLY UNDER HEAVY TESTING

## Usage

```go
import "github.com/safad/rest-layer-pgsql"
```

Open an SQL connection to PostgreSQL using database/sql:

```go
const (
        DB_USER     = "postgres"
        DB_PASSWORD = "postgres"
        DB_NAME     = "test"
    )
dbinfo := fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable",
            DB_USER, DB_PASSWORD, DB_NAME)
        db, err := sql.Open("postgres", dbinfo)
```

Create a resource storage handler with a given DB/collection:

```go
users_handler := pgsql.NewHandler(db, "users")
```

Use this handler with a resource:

```go
index.Bind("users", users, users_handler, resource.DefaultConf)
```

You may want to create a many mongo handlers as you have resources as long as you want each resources in a different collection. You can share the same `pq` db connection across all you handlers.
