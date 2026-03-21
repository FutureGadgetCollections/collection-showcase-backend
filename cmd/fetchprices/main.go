package main

import (
	"context"
	"log"
	"os"

	"cloud.google.com/go/bigquery"
	"github.com/FutureGadgetLabs/collection-showcase-backend/internal/pricefetch"
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

	ctx := context.Background()

	bqClient, err := bigquery.NewClient(ctx, project)
	if err != nil {
		log.Fatalf("failed to create bigquery client: %v", err)
	}
	defer bqClient.Close()

	log.Printf("fetchprices: starting for project=%s inv=%s mkt=%s", project, invDataset, mktDataset)

	results, err := pricefetch.Run(ctx, bqClient, invDataset, mktDataset)
	if err != nil {
		log.Fatalf("fetchprices: %v", err)
	}

	var fetched, skipped, failed int
	for _, r := range results {
		switch {
		case r.Skipped:
			skipped++
		case r.Error != "":
			failed++
			log.Printf("fetchprices: error for %s: %s", r.ProductID, r.Error)
		default:
			fetched++
		}
	}

	log.Printf("fetchprices: done — fetched=%d skipped=%d failed=%d", fetched, skipped, failed)

	if failed > 0 {
		os.Exit(1)
	}
}
