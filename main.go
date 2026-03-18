package main

import (
	"context"
	"log"
	"os"

	"cloud.google.com/go/bigquery"
	"github.com/FutureGadgetLabs/collection-showcase-backend/internal/handlers"
	"github.com/gin-gonic/gin"
)

func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func main() {
	project := getEnv("BQ_PROJECT", "future-gadget-labs-483502")
	inventoryDataset := getEnv("BQ_INVENTORY_DATASET", "inventory")
	marketDataset := getEnv("BQ_MARKET_DATASET", "market_data")
	port := getEnv("PORT", "8080")

	ctx := context.Background()
	bqClient, err := bigquery.NewClient(ctx, project)
	if err != nil {
		log.Fatalf("failed to create bigquery client: %v", err)
	}
	defer bqClient.Close()

	router := gin.Default()

	router.Use(func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next()
	})

	router.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})

	ph := handlers.NewProductHandler(bqClient, inventoryDataset)
	router.GET("/products", ph.List)
	router.GET("/products/:id", ph.Get)
	router.POST("/products", ph.Create)
	router.PUT("/products/:id", ph.Update)
	router.DELETE("/products/:id", ph.Delete)

	th := handlers.NewTransactionHandler(bqClient, inventoryDataset)
	router.GET("/transactions", th.List)
	router.GET("/transactions/:id", th.Get)
	router.POST("/transactions", th.Create)
	router.PUT("/transactions/:id", th.Update)
	router.DELETE("/transactions/:id", th.Delete)

	ch := handlers.NewCollectionHandler(bqClient, inventoryDataset)
	router.GET("/collection", ch.List)
	router.GET("/collection/:product_id", ch.Get)

	prh := handlers.NewPriceHistoryHandler(bqClient, marketDataset)
	router.GET("/price-history", prh.List)
	router.POST("/price-history", prh.Create)
	router.DELETE("/price-history/:record_id", prh.Delete)

	log.Printf("starting server on :%s", port)
	if err := router.Run(":" + port); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
