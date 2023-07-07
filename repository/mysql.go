package repository

import (
	"database/sql"
	"fmt"
)

type DBConfig struct {
	Host     string
	Port     string
	Username string
	Password string
	DBName   string
}

func OpenMySQL(dbc DBConfig) (*sql.DB, error) {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@(%s:%s)/%s", dbc.Username, dbc.Password, dbc.Host, dbc.Port, dbc.DBName))
	if err != nil {
		return nil, err
	}

	if err = db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}
