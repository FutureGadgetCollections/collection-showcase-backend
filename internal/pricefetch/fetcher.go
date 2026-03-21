package pricefetch

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"os"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/chromedp/chromedp"
	"github.com/google/uuid"
)

type Result struct {
	ProductID   string
	TcgplayerID string
	MarketPrice *float64
	MedianPrice *float64
	Skipped     bool
	Error       string
}

// Run fetches today's TCGPlayer prices for all products and inserts them into price_history.
func Run(ctx context.Context, bqClient *bigquery.Client, invDataset, mktDataset string) ([]Result, error) {
	today := civil.DateOf(time.Now())

	sql := fmt.Sprintf(
		"SELECT product_id, tcgplayer_id FROM `%s.%s.products` WHERE tcgplayer_id IS NOT NULL AND tcgplayer_id != '' ORDER BY created_at",
		bqClient.Project(), invDataset,
	)
	it, err := bqClient.Query(sql).Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("query products: %w", err)
	}

	type productRef struct {
		ProductID   string `bigquery:"product_id"`
		TcgplayerID string `bigquery:"tcgplayer_id"`
	}

	var products []productRef
	for {
		var row productRef
		if err := it.Next(&row); err != nil {
			break
		}
		products = append(products, row)
	}

	if len(products) == 0 {
		log.Printf("pricefetch: no products with tcgplayer_id found")
		return nil, nil
	}

	// Check which products already have a record for today.
	alreadyFetched := map[string]bool{}
	checkSQL := fmt.Sprintf(
		"SELECT product_id FROM `%s.%s.price_history` WHERE snapshot_date = @date AND source = 'tcgplayer'",
		bqClient.Project(), mktDataset,
	)
	checkQ := bqClient.Query(checkSQL)
	checkQ.Parameters = []bigquery.QueryParameter{{Name: "date", Value: today}}
	if checkIt, err := checkQ.Read(ctx); err == nil {
		for {
			var row struct {
				ProductID string `bigquery:"product_id"`
			}
			if err := checkIt.Next(&row); err != nil {
				break
			}
			alreadyFetched[row.ProductID] = true
		}
	}

	chromiumPath := os.Getenv("CHROMIUM_PATH")
	if chromiumPath == "" {
		chromiumPath = "/usr/bin/chromium-browser"
	}
	opts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.ExecPath(chromiumPath),
		chromedp.Flag("no-sandbox", true),
		chromedp.Flag("disable-dev-shm-usage", true),
		chromedp.Flag("disable-gpu", true),
		chromedp.Flag("disable-software-rasterizer", true),
	)
	allocCtx, allocCancel := chromedp.NewExecAllocator(context.Background(), opts...)
	defer allocCancel()

	browserCtx, browserCancel := chromedp.NewContext(allocCtx)
	defer browserCancel()
	if err := chromedp.Run(browserCtx); err != nil {
		return nil, fmt.Errorf("start browser: %w", err)
	}

	results := make([]Result, 0, len(products))

	for _, p := range products {
		result := Result{ProductID: p.ProductID, TcgplayerID: p.TcgplayerID}

		if alreadyFetched[p.ProductID] {
			log.Printf("pricefetch: %s already fetched today, skipping", p.ProductID)
			result.Skipped = true
			results = append(results, result)
			continue
		}

		price, err := fetchPrice(browserCtx, p.TcgplayerID)
		if err != nil {
			log.Printf("pricefetch: %s (%s): %v", p.ProductID, p.TcgplayerID, err)
			result.Error = err.Error()
			results = append(results, result)
			continue
		}

		id := uuid.New().String()
		insertSQL := fmt.Sprintf(
			`INSERT INTO `+"`%s.%s.price_history`"+`
			(record_id, product_id, snapshot_date, source, market_price, median_price, sell_through_rate, distinct_buyer_count, listed_count, created_at)
			VALUES (@record_id, @product_id, @snapshot_date, @source, @market_price, @median_price, @sell_through_rate, @distinct_buyer_count, @listed_count, @created_at)`,
			bqClient.Project(), mktDataset,
		)
		q := bqClient.Query(insertSQL)
		q.Parameters = []bigquery.QueryParameter{
			{Name: "record_id", Value: id},
			{Name: "product_id", Value: p.ProductID},
			{Name: "snapshot_date", Value: today},
			{Name: "source", Value: "tcgplayer"},
			{Name: "market_price", Value: new(big.Rat).SetFloat64(price.marketPrice)},
			{Name: "median_price", Value: new(big.Rat).SetFloat64(price.medianPrice)},
			{Name: "sell_through_rate", Value: 0.0},
			{Name: "distinct_buyer_count", Value: int64(0)},
			{Name: "listed_count", Value: int64(0)},
			{Name: "created_at", Value: time.Now().UTC()},
		}
		job, err := q.Run(ctx)
		if err != nil {
			result.Error = err.Error()
		} else if _, err := job.Wait(ctx); err != nil {
			result.Error = err.Error()
		} else {
			mp := price.marketPrice
			med := price.medianPrice
			result.MarketPrice = &mp
			result.MedianPrice = &med
			log.Printf("pricefetch: %s market=%.2f median=%.2f", p.ProductID, mp, med)
		}

		results = append(results, result)
	}

	return results, nil
}

type tcgPrice struct {
	marketPrice float64
	medianPrice float64
}

func fetchPrice(browserCtx context.Context, tcgplayerID string) (*tcgPrice, error) {
	tabCtx, tabCancel := chromedp.NewContext(browserCtx)
	defer tabCancel()

	tabCtx, timeoutCancel := context.WithTimeout(tabCtx, 30*time.Second)
	defer timeoutCancel()

	url := fmt.Sprintf("https://www.tcgplayer.com/product/%s?Language=English", tcgplayerID)

	var nextDataJSON string
	if err := chromedp.Run(tabCtx,
		chromedp.Navigate(url),
		chromedp.WaitReady("body"),
		chromedp.Sleep(3*time.Second),
		chromedp.Evaluate(`JSON.stringify(window.__NEXT_DATA__ || null)`, &nextDataJSON),
	); err != nil {
		return nil, err
	}

	if nextDataJSON == "" || nextDataJSON == "null" {
		return nil, fmt.Errorf("__NEXT_DATA__ not found — page structure may have changed")
	}

	var data interface{}
	if err := json.Unmarshal([]byte(nextDataJSON), &data); err != nil {
		return nil, fmt.Errorf("parse __NEXT_DATA__: %w", err)
	}

	marketPrice, found := findFloat(data, "marketPrice")
	if !found {
		return nil, fmt.Errorf("marketPrice not found in page data — field name may have changed")
	}
	medianPrice, _ := findFloat(data, "medianPrice")

	return &tcgPrice{marketPrice: marketPrice, medianPrice: medianPrice}, nil
}

func findFloat(v interface{}, key string) (float64, bool) {
	switch m := v.(type) {
	case map[string]interface{}:
		if val, ok := m[key]; ok {
			if f, ok := val.(float64); ok {
				return f, true
			}
		}
		for _, child := range m {
			if f, ok := findFloat(child, key); ok {
				return f, true
			}
		}
	case []interface{}:
		for _, item := range m {
			if f, ok := findFloat(item, key); ok {
				return f, true
			}
		}
	}
	return 0, false
}
