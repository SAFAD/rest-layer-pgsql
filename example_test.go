package pgsql_test

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"

	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/rest"
	"github.com/rs/rest-layer/schema"
	pgsql "github.com/safad/rest-layer-pgsql"
)

var (
	user = schema.Schema{
		Fields: schema.Fields{
			"id":      schema.IDField,
			"created": schema.CreatedField,
			"updated": schema.UpdatedField,
			"name": schema.Field{
				Required:   true,
				Filterable: true,
				Sortable:   true,
				Validator: &schema.String{
					MaxLen: 150,
				},
			},
		},
	}

	// Define a post resource schema
	post = schema.Schema{
		Fields: schema.Fields{

			"id":      schema.IDField,
			"created": schema.CreatedField,
			"updated": schema.UpdatedField,
			"user": schema.Field{
				Required:   true,
				Filterable: true,
				Validator: &schema.Reference{
					Path: "users",
				},
			},
			"public": schema.Field{
				Filterable: true,
				Validator:  &schema.Bool{},
			},
			"title": schema.Field{
				Required: true,
				Validator: &schema.String{
					MaxLen: 150,
				},
			},
			"body": schema.Field{
				Validator: &schema.String{
					MaxLen: 100000,
				},
			},
		},
	}
)

func Example() {
	const (
		DB_USER     = "postgres"
		DB_PASSWORD = ""
		DB_NAME     = "travis_ci_test"
	)
	dbinfo := fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable",
		DB_USER, DB_PASSWORD, DB_NAME)
	db, err := sql.Open("postgres", dbinfo)
	if err != nil {
		log.Fatalf("Can't connect to PostgreSQL: %s", err)
	}

	//time to set some tables up
	setupDB(db)

	index := resource.NewIndex()

	users := index.Bind("users", user, pgsql.NewHandler(db, "users"), resource.Conf{
		AllowedModes: resource.ReadWrite,
	})

	users.Bind("posts", "user", post, pgsql.NewHandler(db, "posts"), resource.Conf{
		AllowedModes: resource.ReadWrite,
	})

	api, err := rest.NewHandler(index)
	if err != nil {
		log.Fatalf("Invalid API configuration: %s", err)
	}

	// Bind the API under /api/ path
	http.Handle("/api/", http.StripPrefix("/api/", api))

	log.Print("Serving API on http://localhost:8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}
func setupDB(db *sql.DB) {
	var err error
	// create users table
	_, err = db.Exec("CREATE TABLE `users` (`id` VARCHAR(128) PRIMARY KEY,`etag` VARCHAR(128),`updated` VARCHAR(128),`created` VARCHAR(128), `name` VARCHAR(150));")
	if err != nil {
		log.Fatal(err)
	}
	// create posts table
	_, err = db.Exec("CREATE TABLE `posts` (`id` VARCHAR(128) PRIMARY KEY,`etag` VARCHAR(128),`updated` VARCHAR(128), `created` VARCHAR(128), `user` VARCHAR(128) REFERENCES users(id) ON DELETE CASCADE, `public` INTEGER, `title` VARCHAR(150), `body` VARCHAR(100000));")
	if err != nil {
		log.Fatal(err)
	}
}
