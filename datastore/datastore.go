package datastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

type DBWrapper interface {
	Ping() error
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	Prepare(query string) (*sql.Stmt, error)
	Begin() (TxWrapper, error)
}

type TxWrapper interface {
	Commit() error
	Rollback() error
	Prepare(query string) (*sql.Stmt, error)
	Stmt(stmt *sql.Stmt) *sql.Stmt
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

var dbWrapper DBWrapper = nil

func SetDB(db DBWrapper) {
	dbWrapper = db
}

func Ping() error {
	return dbWrapper.Ping()
}

func Exec(query string, args ...interface{}) (sql.Result, error) {
	return dbWrapper.Exec(query, args...)
}

func Query(query string, args ...interface{}) (*sql.Rows, error) {
	return dbWrapper.Query(query, args...)
}

func QueryRow(query string, args ...interface{}) *sql.Row {
	return dbWrapper.QueryRow(query, args...)
}

func Prepare(query string) (*sql.Stmt, error) {
	return dbWrapper.Prepare(query)
}

func Begin() (TxWrapper, error) {
	return dbWrapper.Begin()
}

func Transact(txFunc func(TxWrapper) error) error {

	tx, err := Begin()
	if err != nil {
		return err
	}

	defer func() {
		if p := recover(); p != nil {
			switch p := p.(type) {
			case error:
				err = p
			default:
				err = fmt.Errorf("%s", p)
			}
		}
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()
	return txFunc(tx)
}
