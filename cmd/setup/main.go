package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"
)

func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func isAlreadyExists(err error) bool {
	if e, ok := err.(*googleapi.Error); ok {
		return e.Code == 409
	}
	return false
}

func main() {
	project := getEnv("BQ_PROJECT", "future-gadget-labs-483502")
	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, project)
	if err != nil {
		log.Fatalf("failed to create bigquery client: %v", err)
	}
	defer client.Close()

	inventoryDataset := client.Dataset("inventory")
	if err := inventoryDataset.Create(ctx, &bigquery.DatasetMetadata{Location: "US"}); err != nil {
		if isAlreadyExists(err) {
			fmt.Println("dataset inventory already exists")
		} else {
			log.Fatalf("failed to create inventory dataset: %v", err)
		}
	} else {
		fmt.Println("created dataset: inventory")
	}

	marketDataset := client.Dataset("market_data")
	if err := marketDataset.Create(ctx, &bigquery.DatasetMetadata{Location: "US"}); err != nil {
		if isAlreadyExists(err) {
			fmt.Println("dataset market_data already exists")
		} else {
			log.Fatalf("failed to create market_data dataset: %v", err)
		}
	} else {
		fmt.Println("created dataset: market_data")
	}

	productsSchema := bigquery.Schema{
		{Name: "product_id", Type: bigquery.StringFieldType, Required: true},
		{Name: "name", Type: bigquery.StringFieldType},
		{Name: "game_category", Type: bigquery.StringFieldType},
		{Name: "game_subcategory", Type: bigquery.StringFieldType},
		{Name: "product_category", Type: bigquery.StringFieldType},
		{Name: "tcgplayer_id", Type: bigquery.StringFieldType},
		{Name: "pricecharting_url", Type: bigquery.StringFieldType},
		{Name: "listing_url", Type: bigquery.StringFieldType},
		{Name: "created_at", Type: bigquery.TimestampFieldType},
	}
	productsTable := inventoryDataset.Table("products")
	if err := productsTable.Create(ctx, &bigquery.TableMetadata{Schema: productsSchema}); err != nil {
		if isAlreadyExists(err) {
			fmt.Println("table inventory.products already exists")
		} else {
			log.Fatalf("failed to create products table: %v", err)
		}
	} else {
		fmt.Println("created table: inventory.products")
	}

	transactionsSchema := bigquery.Schema{
		{Name: "transaction_id", Type: bigquery.StringFieldType, Required: true},
		{Name: "product_id", Type: bigquery.StringFieldType},
		{Name: "transaction_date", Type: bigquery.DateFieldType},
		{Name: "price", Type: bigquery.NumericFieldType},
		{Name: "quantity", Type: bigquery.IntegerFieldType},
		{Name: "transaction_type", Type: bigquery.StringFieldType},
		{Name: "platform", Type: bigquery.StringFieldType},
		{Name: "notes", Type: bigquery.StringFieldType},
		{Name: "created_at", Type: bigquery.TimestampFieldType},
	}
	transactionsTable := inventoryDataset.Table("transactions")
	if err := transactionsTable.Create(ctx, &bigquery.TableMetadata{Schema: transactionsSchema}); err != nil {
		if isAlreadyExists(err) {
			fmt.Println("table inventory.transactions already exists")
		} else {
			log.Fatalf("failed to create transactions table: %v", err)
		}
	} else {
		fmt.Println("created table: inventory.transactions")
	}

	priceHistorySchema := bigquery.Schema{
		{Name: "record_id", Type: bigquery.StringFieldType, Required: true},
		{Name: "product_id", Type: bigquery.StringFieldType},
		{Name: "snapshot_date", Type: bigquery.DateFieldType},
		{Name: "source", Type: bigquery.StringFieldType},
		{Name: "market_price", Type: bigquery.NumericFieldType},
		{Name: "median_price", Type: bigquery.NumericFieldType},
		{Name: "sell_through_rate", Type: bigquery.FloatFieldType},
		{Name: "distinct_buyer_count", Type: bigquery.IntegerFieldType},
		{Name: "listed_count", Type: bigquery.IntegerFieldType},
		{Name: "created_at", Type: bigquery.TimestampFieldType},
	}
	priceHistoryTable := marketDataset.Table("price_history")
	if err := priceHistoryTable.Create(ctx, &bigquery.TableMetadata{Schema: priceHistorySchema}); err != nil {
		if isAlreadyExists(err) {
			fmt.Println("table market_data.price_history already exists")
		} else {
			log.Fatalf("failed to create price_history table: %v", err)
		}
	} else {
		fmt.Println("created table: market_data.price_history")
	}

	viewSQL := fmt.Sprintf(`CREATE OR REPLACE VIEW `+"`%s.inventory.collection`"+` AS
SELECT
    product_id,
    SUM(CASE WHEN transaction_type = 'buy' THEN quantity ELSE -quantity END) AS quantity,
    SAFE_DIVIDE(
        SUM(CASE WHEN transaction_type = 'buy' THEN price * quantity ELSE 0 END),
        NULLIF(SUM(CASE WHEN transaction_type = 'buy' THEN quantity ELSE 0 END), 0)
    ) AS avg_unit_cost,
    SUM(CASE WHEN transaction_type = 'buy' THEN price * quantity ELSE 0 END) AS total_invested
FROM `+"`%s.inventory.transactions`"+`
GROUP BY product_id
HAVING SUM(CASE WHEN transaction_type = 'buy' THEN quantity ELSE -quantity END) > 0`, project, project)

	q := client.Query(viewSQL)
	job, err := q.Run(ctx)
	if err != nil {
		log.Fatalf("failed to run view creation query: %v", err)
	}
	if _, err := job.Wait(ctx); err != nil {
		log.Fatalf("failed to create collection view: %v", err)
	}
	fmt.Println("created view: inventory.collection")
}
