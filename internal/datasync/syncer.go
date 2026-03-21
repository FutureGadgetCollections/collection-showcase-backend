package datasync

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

type Syncer struct {
	bq          *bigquery.Client
	gcs         *storage.Client
	project     string
	inventoryDS string
	marketDS    string
	gcsBucket   string
	ghToken     string
	ghOwner     string
	ghRepo      string
}

func New(bq *bigquery.Client, gcs *storage.Client, project, inventoryDS, marketDS, gcsBucket, ghToken, ghOwner, ghRepo string) *Syncer {
	return &Syncer{
		bq:          bq,
		gcs:         gcs,
		project:     project,
		inventoryDS: inventoryDS,
		marketDS:    marketDS,
		gcsBucket:   gcsBucket,
		ghToken:     ghToken,
		ghOwner:     ghOwner,
		ghRepo:      ghRepo,
	}
}

// Trigger fires SyncAll in the background. Safe to call from HTTP handlers.
func (s *Syncer) Trigger() {
	log.Printf("datasync: trigger called")
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		log.Printf("datasync: starting sync")
		if err := s.SyncAll(ctx); err != nil {
			log.Printf("datasync: sync failed: %v", err)
		} else {
			log.Printf("datasync: sync complete")
		}
	}()
}

func (s *Syncer) SyncAll(ctx context.Context) error {
	products, err := queryAll[productRow](ctx, s.bq,
		fmt.Sprintf("SELECT * FROM `%s.%s.products` ORDER BY created_at", s.project, s.inventoryDS))
	if err != nil {
		return fmt.Errorf("query products: %w", err)
	}

	transactions, err := queryAll[transactionRow](ctx, s.bq,
		fmt.Sprintf("SELECT * FROM `%s.%s.transactions` ORDER BY transaction_date, created_at", s.project, s.inventoryDS))
	if err != nil {
		return fmt.Errorf("query transactions: %w", err)
	}

	collection, err := queryAll[collectionRow](ctx, s.bq,
		fmt.Sprintf("SELECT * FROM `%s.%s.collection`", s.project, s.inventoryDS))
	if err != nil {
		return fmt.Errorf("query collection: %w", err)
	}

	priceHistory, err := queryAll[priceHistoryRow](ctx, s.bq,
		fmt.Sprintf("SELECT * FROM `%s.%s.price_history` ORDER BY snapshot_date, created_at", s.project, s.marketDS))
	if err != nil {
		return fmt.Errorf("query price_history: %w", err)
	}

	files := []struct {
		name string
		data interface{}
	}{
		{"products.json", products},
		{"transactions.json", transactions},
		{"collection.json", collection},
		{"price_history.json", priceHistory},
	}

	for _, f := range files {
		b, err := json.Marshal(f.data)
		if err != nil {
			return fmt.Errorf("marshal %s: %w", f.name, err)
		}
		log.Printf("datasync: uploading %s to GCS (%d bytes)", f.name, len(b))
		if err := s.uploadGCS(ctx, f.name, b); err != nil {
			log.Printf("datasync: GCS upload failed for %s: %v", f.name, err)
			return fmt.Errorf("gcs upload %s: %w", f.name, err)
		}
		log.Printf("datasync: GCS upload OK for %s", f.name)
		if s.ghToken != "" {
			log.Printf("datasync: upserting %s to GitHub", f.name)
			if err := s.upsertGitHub(ctx, f.name, b); err != nil {
				log.Printf("datasync: GitHub upsert failed for %s: %v", f.name, err)
				return fmt.Errorf("github upsert %s: %w", f.name, err)
			}
			log.Printf("datasync: GitHub upsert OK for %s", f.name)
		}
	}
	return nil
}

func (s *Syncer) uploadGCS(ctx context.Context, name string, data []byte) error {
	w := s.gcs.Bucket(s.gcsBucket).Object(name).NewWriter(ctx)
	w.ContentType = "application/json"
	if _, err := w.Write(data); err != nil {
		w.Close()
		return err
	}
	return w.Close()
}

// upsertGitHub creates or updates a file in the GitHub repo via the Contents API.
func (s *Syncer) upsertGitHub(ctx context.Context, name string, data []byte) error {
	url := fmt.Sprintf("https://api.github.com/repos/%s/%s/contents/%s", s.ghOwner, s.ghRepo, name)

	// Get current file SHA (required for updates).
	sha, err := s.getGitHubFileSHA(ctx, url)
	if err != nil {
		return err
	}

	body := map[string]string{
		"message": fmt.Sprintf("chore: sync %s", name),
		"content": base64.StdEncoding.EncodeToString(data),
	}
	if sha != "" {
		body["sha"] = sha
	}

	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+s.ghToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(b))
	}
	return nil
}

// getGitHubFileSHA returns the blob SHA of an existing file, or "" if it doesn't exist.
func (s *Syncer) getGitHubFileSHA(ctx context.Context, url string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+s.ghToken)
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return "", nil
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("get file SHA status %d: %s", resp.StatusCode, string(b))
	}

	var result struct {
		SHA string `json:"sha"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}
	return result.SHA, nil
}

func queryAll[T any](ctx context.Context, bq *bigquery.Client, sql string) ([]T, error) {
	it, err := bq.Query(sql).Read(ctx)
	if err != nil {
		return nil, err
	}
	var rows []T
	for {
		var row T
		if err := it.Next(&row); err == iterator.Done {
			break
		} else if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	if rows == nil {
		rows = []T{}
	}
	return rows, nil
}

// BQ row types — mirrors the table schemas for JSON serialization.

type productRow struct {
	ProductID        string    `json:"product_id" bigquery:"product_id"`
	Name             string    `json:"name" bigquery:"name"`
	GameCategory     string    `json:"game_category" bigquery:"game_category"`
	GameSubcategory  string    `json:"game_subcategory" bigquery:"game_subcategory"`
	ProductCategory  string    `json:"product_category" bigquery:"product_category"`
	TcgplayerID      string    `json:"tcgplayer_id" bigquery:"tcgplayer_id"`
	PricechartingURL string    `json:"pricecharting_url" bigquery:"pricecharting_url"`
	ListingURL       string    `json:"listing_url" bigquery:"listing_url"`
	ImageURL         string    `json:"image_url" bigquery:"image_url"`
	CreatedAt        time.Time `json:"created_at" bigquery:"created_at"`
}

type transactionRow struct {
	TransactionID   string    `json:"transaction_id" bigquery:"transaction_id"`
	ProductID       string    `json:"product_id" bigquery:"product_id"`
	TransactionDate civil.Date `json:"transaction_date" bigquery:"transaction_date"`
	Price           float64   `json:"price" bigquery:"price"`
	Quantity        int64     `json:"quantity" bigquery:"quantity"`
	TransactionType string    `json:"transaction_type" bigquery:"transaction_type"`
	Platform        string    `json:"platform" bigquery:"platform"`
	Notes           string    `json:"notes" bigquery:"notes"`
	CreatedAt       time.Time `json:"created_at" bigquery:"created_at"`
}

type collectionRow struct {
	ProductID     string  `json:"product_id" bigquery:"product_id"`
	Quantity      int64   `json:"quantity" bigquery:"quantity"`
	AvgUnitCost   float64 `json:"avg_unit_cost" bigquery:"avg_unit_cost"`
	TotalInvested float64 `json:"total_invested" bigquery:"total_invested"`
}

type priceHistoryRow struct {
	RecordID           string    `json:"record_id" bigquery:"record_id"`
	ProductID          string    `json:"product_id" bigquery:"product_id"`
	SnapshotDate       civil.Date `json:"snapshot_date" bigquery:"snapshot_date"`
	Source             string    `json:"source" bigquery:"source"`
	MarketPrice        float64   `json:"market_price" bigquery:"market_price"`
	MedianPrice        float64   `json:"median_price" bigquery:"median_price"`
	SellThroughRate    float64   `json:"sell_through_rate" bigquery:"sell_through_rate"`
	DistinctBuyerCount int64     `json:"distinct_buyer_count" bigquery:"distinct_buyer_count"`
	ListedCount        int64     `json:"listed_count" bigquery:"listed_count"`
	CreatedAt          time.Time `json:"created_at" bigquery:"created_at"`
}
