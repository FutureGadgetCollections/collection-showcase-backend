package main

import (
	"context"
	"log"
	"os"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/FutureGadgetLabs/collection-showcase-backend/internal/datasync"
)

func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func main() {
	project := getEnv("BQ_PROJECT", "future-gadget-labs-483502")
	invDataset := getEnv("BQ_INVENTORY_DATASET", "inventory")
	mktDataset := getEnv("BQ_MARKET_DATASET", "market_data")
	gcsBucket := getEnv("GCS_DATA_BUCKET", "collection-showcase-data")
	ghToken := getEnv("GITHUB_TOKEN", "")
	ghOwner := getEnv("GITHUB_OWNER", "")
	ghRepo := getEnv("GITHUB_REPO", "")

	ctx := context.Background()

	bqClient, err := bigquery.NewClient(ctx, project)
	if err != nil {
		log.Fatalf("syncdata: failed to create bigquery client: %v", err)
	}
	defer bqClient.Close()

	gcsClient, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("syncdata: failed to create gcs client: %v", err)
	}
	defer gcsClient.Close()

	log.Printf("syncdata: starting — project=%s gcs=%s github=%s/%s", project, gcsBucket, ghOwner, ghRepo)

	syncer := datasync.New(bqClient, gcsClient, project, invDataset, mktDataset, gcsBucket, ghToken, ghOwner, ghRepo)
	if err := syncer.SyncAll(ctx); err != nil {
		log.Fatalf("syncdata: %v", err)
	}

	log.Printf("syncdata: done")
}
