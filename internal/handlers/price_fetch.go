package handlers

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
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type PriceFetchHandler struct {
	client     *bigquery.Client
	invDataset string
	mktDataset string
}

func NewPriceFetchHandler(client *bigquery.Client, invDataset, mktDataset string) *PriceFetchHandler {
	return &PriceFetchHandler{client: client, invDataset: invDataset, mktDataset: mktDataset}
}

type tcgPrice struct {
	MarketPrice float64
	MedianPrice float64
}

type productFetchResult struct {
	ProductID   string   `json:"product_id"`
	TcgplayerID string   `json:"tcgplayer_id"`
	MarketPrice *float64 `json:"market_price,omitempty"`
	MedianPrice *float64 `json:"median_price,omitempty"`
	Skipped     bool     `json:"skipped,omitempty"`
	Error       string   `json:"error,omitempty"`
}

func (h *PriceFetchHandler) Fetch(c *gin.Context) {
	ctx := c.Request.Context()
	today := civil.DateOf(time.Now())

	sql := fmt.Sprintf(
		"SELECT product_id, tcgplayer_id FROM `%s.%s.products` WHERE tcgplayer_id IS NOT NULL AND tcgplayer_id != '' ORDER BY created_at",
		h.client.Project(), h.invDataset,
	)
	it, err := h.client.Query(sql).Read(ctx)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
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
		c.JSON(200, gin.H{"date": today.String(), "results": []productFetchResult{}})
		return
	}

	// Check which products already have a record for today.
	alreadyFetched := map[string]bool{}
	checkSQL := fmt.Sprintf(
		"SELECT product_id FROM `%s.%s.price_history` WHERE snapshot_date = @date AND source = 'tcgplayer'",
		h.client.Project(), h.mktDataset,
	)
	checkQ := h.client.Query(checkSQL)
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

	// Launch one headless browser for all products.
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
		c.JSON(500, gin.H{"error": "failed to start browser: " + err.Error()})
		return
	}

	results := make([]productFetchResult, 0, len(products))

	for _, p := range products {
		result := productFetchResult{ProductID: p.ProductID, TcgplayerID: p.TcgplayerID}

		if alreadyFetched[p.ProductID] {
			result.Skipped = true
			results = append(results, result)
			continue
		}

		price, err := fetchTCGPlayerPrice(browserCtx, p.TcgplayerID)
		if err != nil {
			log.Printf("price_fetch: %s (%s): %v", p.ProductID, p.TcgplayerID, err)
			result.Error = err.Error()
			results = append(results, result)
			continue
		}

		id := uuid.New().String()
		insertSQL := fmt.Sprintf(
			`INSERT INTO `+"`%s.%s.price_history`"+`
			(record_id, product_id, snapshot_date, source, market_price, median_price, sell_through_rate, distinct_buyer_count, listed_count, created_at)
			VALUES (@record_id, @product_id, @snapshot_date, @source, @market_price, @median_price, @sell_through_rate, @distinct_buyer_count, @listed_count, @created_at)`,
			h.client.Project(), h.mktDataset,
		)
		q := h.client.Query(insertSQL)
		q.Parameters = []bigquery.QueryParameter{
			{Name: "record_id", Value: id},
			{Name: "product_id", Value: p.ProductID},
			{Name: "snapshot_date", Value: today},
			{Name: "source", Value: "tcgplayer"},
			{Name: "market_price", Value: new(big.Rat).SetFloat64(price.MarketPrice)},
			{Name: "median_price", Value: new(big.Rat).SetFloat64(price.MedianPrice)},
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
			result.MarketPrice = &price.MarketPrice
			result.MedianPrice = &price.MedianPrice
		}

		results = append(results, result)
	}

	c.JSON(200, gin.H{"date": today.String(), "results": results})
}

func fetchTCGPlayerPrice(browserCtx context.Context, tcgplayerID string) (*tcgPrice, error) {
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
		return nil, fmt.Errorf("__NEXT_DATA__ not found — page may have changed structure")
	}

	var data interface{}
	if err := json.Unmarshal([]byte(nextDataJSON), &data); err != nil {
		return nil, fmt.Errorf("failed to parse __NEXT_DATA__: %w", err)
	}

	marketPrice, foundMarket := findFloat(data, "marketPrice")
	medianPrice, _ := findFloat(data, "medianPrice")

	if !foundMarket {
		return nil, fmt.Errorf("marketPrice not found in page data — field name may have changed")
	}

	return &tcgPrice{
		MarketPrice: marketPrice,
		MedianPrice: medianPrice,
	}, nil
}

// findFloat recursively searches a JSON value tree for the first occurrence of key
// and returns its float64 value.
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
