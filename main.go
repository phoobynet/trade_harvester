package main

import (
	"context"
	"fmt"
	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata"
	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata/stream"
	"github.com/sirupsen/logrus"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"os"
	"os/signal"
	"sync"
	"time"
)

var mu = sync.RWMutex{}

type Trade struct {
	Symbol string
	Price  float64
	Size   float64
	Time   int64
}

func NewTrade(trade stream.Trade) Trade {
	return Trade{
		Symbol: trade.Symbol,
		Price:  trade.Price,
		Size:   float64(trade.Size),
		Time:   trade.Timestamp.UnixNano(),
	}
}

func main() {
	// recover from panic
	defer func() {
		if r := recover(); r != nil {
			logrus.Error("Recovered in f", r)
		}
	}()

	quitChannel := make(chan os.Signal, 1)
	signal.Notify(quitChannel, os.Interrupt)

	dbPath := fmt.Sprintf("trades_%s.db", time.Now().Format("2006-01-02_15-04-05"))

	logrus.Println("Database path: ", dbPath)

	const batchSize = 100

	db, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{
		SkipDefaultTransaction: true,
		PrepareStmt:            true,
		CreateBatchSize:        batchSize,
		Logger:                 logger.Default.LogMode(logger.Silent),
	})

	if err != nil {
		logrus.Fatal(err)
	}

	err = db.AutoMigrate(&Trade{})
	if err != nil {
		logrus.Fatal(err)
	}

	// Problems with WAL mode, took the risk of setting synchronous to normal
	pragmas := []string{
		"PRAGMA synchronous = normal;",
		"PRAGMA temp_store = MEMORY;",
	}

	for _, pragma := range pragmas {
		if res := db.Exec(pragma); res.Error != nil {
			logrus.Fatal(res.Error)
		}
	}

	const maxIndex = batchSize - 1
	checkoutTicker := time.NewTicker(time.Second * 5)

	trades := make(chan stream.Trade, 500_000)
	totalInserted := 0

	ctx, cancel := context.WithCancel(context.Background())

	stocksStream := stream.NewStocksClient(marketdata.SIP)

	err = stocksStream.Connect(ctx)

	if err != nil {
		logrus.Fatal(err)
	}

	err = stocksStream.SubscribeToTrades(func(trade stream.Trade) {
		trades <- trade
	}, "*")

	if err != nil {
		logrus.Fatal(err)
	}

	go func() {
		var trade stream.Trade
		batch := make([]Trade, batchSize)
		var batchIndex = 0

		for {
			select {
			case <-checkoutTicker.C:
				mu.RLock()
				logrus.Infof("Inserted %d trades", totalInserted)
				mu.RUnlock()
			case <-quitChannel:
				cancel()
				os.Exit(1)
			case trade = <-trades:
				mu.Lock()
				batch[batchIndex] = NewTrade(trade)
				batchIndex++
				if batchIndex == maxIndex {
					err = db.Transaction(func(tx *gorm.DB) error {
						if err := tx.Create(&batch).Error; err != nil {
							return err
						}

						return nil
					})

					if err != nil {
						logrus.Fatal(err)
					}

					totalInserted += batchSize
					batchIndex = 0
				}
				mu.Unlock()
			}
		}
	}()

	<-quitChannel
}
