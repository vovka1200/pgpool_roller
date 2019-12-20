package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/pgxpool"
	log "github.com/sirupsen/logrus"
	"os"
	"time"
)

var pool *pgxpool.Pool

func main() {
	log.SetReportCaller(true)
	log.SetOutput(os.Stdout)
	log.SetFormatter(&log.TextFormatter{})
	waitForNotify()
	defer pool.Close()
}

// Ожидания уведомления
//
func waitForNotify() {
	db := connect()
	defer release(db)

	// Установка канала уведомлений
	_, err := db.Conn().Exec(context.Background(), fmt.Sprintf("/*NO LOAD BALANCE*/LISTEN \"%s\"", "pgpool_roller"))
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}

	// Цикл
	for {
		log.WithFields(log.Fields{
			"channel": "pgpool_roller",
			"db":      db,
		}).Info("Ожидание уведомлений")
		notification, err := db.Conn().WaitForNotification(context.Background())
		if err != nil {
			log.Fatal(err)
			os.Exit(1)
		}
		log.WithFields(log.Fields{
			"notify": notification,
		}).Info("Notify")

		log.Info("Ожидание...")
		time.Sleep(1 * time.Second)
	}
}

// Получает соединение из пула
//
func connect() *pgxpool.Conn {
	if pool == nil {
		var err error
		pool, err = pgxpool.Connect(
			context.Background(),
			"",
		)
		if err != nil {
			log.WithFields(log.Fields{
				"err": err,
			}).Fatal("Ошибка подключения к БД")
			os.Exit(1)
		}
		log.WithFields(log.Fields{
			"pool": pool,
		}).Info("Соединение с БД")
	}
	db, err := pool.Acquire(context.Background())
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
	log.WithFields(log.Fields{
		"pool": pool,
		"db":   db,
	}).Info("Соединение из пула")
	return db
}

// Возвращает соединение в пул
//
func release(db *pgxpool.Conn) {
	if db != nil {
		db.Release()
	}
}
