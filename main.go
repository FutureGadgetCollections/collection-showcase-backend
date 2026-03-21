package main

import (
	"context"
	"log"
	"os"
	"strings"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	firebase "firebase.google.com/go/v4"
	"github.com/FutureGadgetLabs/collection-showcase-backend/internal/datasync"
	"github.com/FutureGadgetLabs/collection-showcase-backend/internal/handlers"
	"github.com/FutureGadgetLabs/collection-showcase-backend/internal/middleware"
	"github.com/gin-gonic/gin"
)

const version = "1.0.4"

func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

// mask keeps the first and last character and replaces the rest with ***.
func mask(s string) string {
	if len(s) <= 2 {
		return s
	}
	return string(s[0]) + "***" + string(s[len(s)-1])
}

func main() {
	project := getEnv("BQ_PROJECT", "future-gadget-labs-483502")
	inventoryDataset := getEnv("BQ_INVENTORY_DATASET", "inventory")
	marketDataset := getEnv("BQ_MARKET_DATASET", "market_data")
	port := getEnv("PORT", "8080")
	allowedEmails := strings.Split(getEnv("ALLOWED_EMAILS", ""), ",")
	firebaseProjectID := getEnv("FIREBASE_PROJECT_ID", "collection-showcase-auth")
	gcsBucket := getEnv("GCS_DATA_BUCKET", "collection-showcase-data")
	ghToken := getEnv("GITHUB_TOKEN", "")
	ghOwner := getEnv("GITHUB_OWNER", "")
	ghRepo := getEnv("GITHUB_REPO", "")

	ctx := context.Background()
	bqClient, err := bigquery.NewClient(ctx, project)
	if err != nil {
		log.Fatalf("failed to create bigquery client: %v", err)
	}
	defer bqClient.Close()

	gcsClient, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("failed to create gcs client: %v", err)
	}
	defer gcsClient.Close()

	syncer := datasync.New(bqClient, gcsClient, project, inventoryDataset, marketDataset, gcsBucket, ghToken, ghOwner, ghRepo)

	fbApp, err := firebase.NewApp(ctx, &firebase.Config{ProjectID: firebaseProjectID})
	if err != nil {
		log.Fatalf("failed to init firebase app: %v", err)
	}
	authClient, err := fbApp.Auth(ctx)
	if err != nil {
		log.Fatalf("failed to init firebase auth: %v", err)
	}

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

	router.GET("/info", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"version": version,
			"env": gin.H{
				"BQ_PROJECT":          project,
				"BQ_INVENTORY_DATASET": inventoryDataset,
				"BQ_MARKET_DATASET":   marketDataset,
				"GCS_DATA_BUCKET":     gcsBucket,
				"FIREBASE_PROJECT_ID": firebaseProjectID,
				"ALLOWED_EMAILS":      getEnv("ALLOWED_EMAILS", "(not set)"),
				"GITHUB_TOKEN":        mask(ghToken),
				"GITHUB_OWNER":        ghOwner,
				"GITHUB_REPO":         ghRepo,
			},
		})
	})

	requireAuth := middleware.RequireAuth(authClient, allowedEmails)

	ph := handlers.NewProductHandler(bqClient, inventoryDataset, syncer.Trigger)
	router.GET("/products", ph.List)
	router.GET("/products/:id", ph.Get)
	router.POST("/products", requireAuth, ph.Create)
	router.PUT("/products/:id", requireAuth, ph.Update)
	router.DELETE("/products/:id", requireAuth, ph.Delete)

	th := handlers.NewTransactionHandler(bqClient, inventoryDataset, syncer.Trigger)
	router.GET("/transactions", th.List)
	router.GET("/transactions/:id", th.Get)
	router.POST("/transactions", requireAuth, th.Create)
	router.PUT("/transactions/:id", requireAuth, th.Update)
	router.DELETE("/transactions/:id", requireAuth, th.Delete)

	ch := handlers.NewCollectionHandler(bqClient, inventoryDataset)
	router.GET("/collection", ch.List)
	router.GET("/collection/:product_id", ch.Get)

	prh := handlers.NewPriceHistoryHandler(bqClient, marketDataset, syncer.Trigger)
	router.GET("/price-history", prh.List)
	router.POST("/price-history", requireAuth, prh.Create)
	router.DELETE("/price-history/:record_id", requireAuth, prh.Delete)

	log.Printf("starting server on :%s (version %s)", port, version)
	if err := router.Run(":" + port); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
