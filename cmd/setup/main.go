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

	// NOTE: inventory.products table is deprecated. Products now come from
	// inventory.catalog_products view (backed by catalog.sealed_products in
	// the market tracker). The old table still exists in BQ but is unused.

	transactionsSchema := bigquery.Schema{
		{Name: "transaction_id", Type: bigquery.StringFieldType, Required: true},
		{Name: "product_id", Type: bigquery.StringFieldType},
		{Name: "transaction_date", Type: bigquery.DateFieldType},
		{Name: "unit_price", Type: bigquery.FloatFieldType},
		{Name: "quantity", Type: bigquery.IntegerFieldType},
		{Name: "transaction_type", Type: bigquery.StringFieldType},
		{Name: "platform", Type: bigquery.StringFieldType},
		{Name: "notes", Type: bigquery.StringFieldType},
		{Name: "created_at", Type: bigquery.TimestampFieldType},
	}
	transactionsTable := inventoryDataset.Table("transactions")
	if err := transactionsTable.Create(ctx, &bigquery.TableMetadata{Schema: transactionsSchema}); err != nil {
		if isAlreadyExists(err) {
			fmt.Println("table inventory.transactions already exists, updating schema")
			meta, err := transactionsTable.Metadata(ctx)
			if err != nil {
				log.Fatalf("failed to get transactions table metadata: %v", err)
			}
			if _, err := transactionsTable.Update(ctx, bigquery.TableMetadataToUpdate{Schema: transactionsSchema}, meta.ETag); err != nil {
				log.Fatalf("failed to update transactions table schema: %v", err)
			}
			fmt.Println("updated schema: inventory.transactions")
		} else {
			log.Fatalf("failed to create transactions table: %v", err)
		}
	} else {
		fmt.Println("created table: inventory.transactions")
	}

	boxBreaksSchema := bigquery.Schema{
		{Name: "break_id", Type: bigquery.StringFieldType, Required: true},
		{Name: "sealed_product_id", Type: bigquery.StringFieldType, Required: true},
		{Name: "break_date", Type: bigquery.DateFieldType, Required: true},
		{Name: "sealed_market_value", Type: bigquery.FloatFieldType, Required: true},
		{Name: "binder_id", Type: bigquery.StringFieldType},
		{Name: "notes", Type: bigquery.StringFieldType},
		{Name: "created_at", Type: bigquery.TimestampFieldType, Required: true},
	}
	boxBreaksTable := inventoryDataset.Table("box_breaks")
	if err := boxBreaksTable.Create(ctx, &bigquery.TableMetadata{Schema: boxBreaksSchema}); err != nil {
		if isAlreadyExists(err) {
			fmt.Println("table inventory.box_breaks already exists, updating schema")
			meta, err := boxBreaksTable.Metadata(ctx)
			if err != nil {
				log.Fatalf("failed to get box_breaks table metadata: %v", err)
			}
			if _, err := boxBreaksTable.Update(ctx, bigquery.TableMetadataToUpdate{Schema: boxBreaksSchema}, meta.ETag); err != nil {
				log.Fatalf("failed to update box_breaks table schema: %v", err)
			}
			fmt.Println("updated schema: inventory.box_breaks")
		} else {
			log.Fatalf("failed to create box_breaks table: %v", err)
		}
	} else {
		fmt.Println("created table: inventory.box_breaks")
	}

	boxBreakPullsSchema := bigquery.Schema{
		{Name: "pull_id", Type: bigquery.StringFieldType, Required: true},
		{Name: "break_id", Type: bigquery.StringFieldType, Required: true},
		{Name: "pull_type", Type: bigquery.StringFieldType, Required: true},
		{Name: "product_id", Type: bigquery.StringFieldType},
		{Name: "bulk_game", Type: bigquery.StringFieldType},
		{Name: "bulk_set_code", Type: bigquery.StringFieldType},
		{Name: "bulk_label", Type: bigquery.StringFieldType},
		{Name: "quantity", Type: bigquery.IntegerFieldType, Required: true},
		{Name: "market_value_per_unit", Type: bigquery.FloatFieldType, Required: true},
		{Name: "allocated_cost_basis_per_unit", Type: bigquery.FloatFieldType, Required: true},
		{Name: "notes", Type: bigquery.StringFieldType},
		{Name: "created_at", Type: bigquery.TimestampFieldType, Required: true},
	}
	boxBreakPullsTable := inventoryDataset.Table("box_break_pulls")
	if err := boxBreakPullsTable.Create(ctx, &bigquery.TableMetadata{Schema: boxBreakPullsSchema}); err != nil {
		if isAlreadyExists(err) {
			fmt.Println("table inventory.box_break_pulls already exists, updating schema")
			meta, err := boxBreakPullsTable.Metadata(ctx)
			if err != nil {
				log.Fatalf("failed to get box_break_pulls table metadata: %v", err)
			}
			if _, err := boxBreakPullsTable.Update(ctx, bigquery.TableMetadataToUpdate{Schema: boxBreakPullsSchema}, meta.ETag); err != nil {
				log.Fatalf("failed to update box_break_pulls table schema: %v", err)
			}
			fmt.Println("updated schema: inventory.box_break_pulls")
		} else {
			log.Fatalf("failed to create box_break_pulls table: %v", err)
		}
	} else {
		fmt.Println("created table: inventory.box_break_pulls")
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
			fmt.Println("table market_data.price_history already exists, updating schema")
			meta, err := priceHistoryTable.Metadata(ctx)
			if err != nil {
				log.Fatalf("failed to get price_history table metadata: %v", err)
			}
			if _, err := priceHistoryTable.Update(ctx, bigquery.TableMetadataToUpdate{Schema: priceHistorySchema}, meta.ETag); err != nil {
				log.Fatalf("failed to update price_history table schema: %v", err)
			}
			fmt.Println("updated schema: market_data.price_history")
		} else {
			log.Fatalf("failed to create price_history table: %v", err)
		}
	} else {
		fmt.Println("created table: market_data.price_history")
	}

	viewSQL := fmt.Sprintf(`CREATE OR REPLACE VIEW `+"`%s.inventory.collection`"+` AS
WITH tx_all AS (
  SELECT product_id, transaction_type, transaction_date, unit_price, quantity
  FROM `+"`%s.inventory.transactions`"+`
  UNION ALL
  -- Synthetic "sell" of the sealed product at break time (closes sealed position at market value).
  SELECT sealed_product_id AS product_id, 'sell' AS transaction_type, break_date AS transaction_date,
         sealed_market_value AS unit_price, 1 AS quantity
  FROM `+"`%s.inventory.box_breaks`"+`
  UNION ALL
  -- Synthetic "buy" of each pulled single at its allocated cost basis (opens single position).
  -- Bulk pulls are intentionally excluded: they have no catalog product to attribute to.
  SELECT p.product_id, 'buy' AS transaction_type, b.break_date AS transaction_date,
         p.allocated_cost_basis_per_unit AS unit_price, p.quantity
  FROM `+"`%s.inventory.box_break_pulls`"+` p
  JOIN `+"`%s.inventory.box_breaks`"+` b USING (break_id)
  WHERE p.pull_type = 'single' AND p.product_id IS NOT NULL AND p.product_id != ''
),
tx AS (
  SELECT
    product_id,
    SUM(CASE WHEN transaction_type = 'buy' THEN quantity ELSE 0 END)          AS total_buy_qty,
    SUM(CASE WHEN transaction_type = 'buy' THEN unit_price * quantity ELSE 0 END)  AS total_buy_value,
    SUM(CASE WHEN transaction_type = 'sell' THEN quantity ELSE 0 END)         AS total_sell_qty,
    SUM(CASE WHEN transaction_type = 'sell' THEN unit_price * quantity ELSE 0 END) AS total_sell_value,
    SUM(CASE WHEN transaction_type = 'buy' THEN quantity ELSE -quantity END)  AS quantity,
    MIN(CASE WHEN transaction_type = 'buy' THEN transaction_date END)         AS first_buy_date
  FROM tx_all
  GROUP BY product_id
),
latest_price AS (
  SELECT cp.product_id, ph.market_price AS latest_market_price
  FROM `+"`%s.inventory.catalog_products`"+` cp
  JOIN `+"`%s.market_data.tcgplayer_price_history`"+` ph ON cp.tcgplayer_id = ph.tcgplayer_id
  QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.product_id ORDER BY ph.date DESC) = 1
),
base AS (
  SELECT
    tx.product_id,
    tx.quantity,
    tx.total_buy_value                                                              AS total_invested,
    SAFE_DIVIDE(tx.total_buy_value, tx.total_buy_qty)                               AS avg_unit_cost,
    tx.total_sell_value
      - SAFE_DIVIDE(tx.total_buy_value, tx.total_buy_qty) * tx.total_sell_qty       AS realized_gain,
    lp.latest_market_price,
    lp.latest_market_price * tx.quantity
      - SAFE_DIVIDE(tx.total_buy_value, tx.total_buy_qty) * tx.quantity             AS unrealized_gain,
    tx.first_buy_date,
    DATE_DIFF(CURRENT_DATE(), tx.first_buy_date, DAY)                               AS days_held
  FROM tx
  LEFT JOIN latest_price lp ON lp.product_id = tx.product_id
  WHERE tx.quantity > 0
)
SELECT
  product_id,
  quantity,
  avg_unit_cost,
  total_invested,
  realized_gain,
  unrealized_gain,
  latest_market_price,
  first_buy_date,
  days_held,
  SAFE_DIVIDE(realized_gain + unrealized_gain, total_invested)                      AS roi,
  CASE
    WHEN days_held > 0 THEN
      POWER(1 + SAFE_DIVIDE(realized_gain + unrealized_gain, total_invested), 365.0 / days_held) - 1
    ELSE NULL
  END                                                                                AS annualized_roi
FROM base`, project, project, project, project, project, project, project)

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
