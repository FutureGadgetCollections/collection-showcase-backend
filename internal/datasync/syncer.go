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
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// Status holds the result of the most recent sync attempt.
type Status struct {
	LastSyncAt  *time.Time `json:"last_sync_at"`
	LastSyncErr *string    `json:"last_sync_error"`
	InProgress  bool       `json:"in_progress"`
}

type Syncer struct {
	bq          *bigquery.Client
	gcs         *storage.Client
	project     string
	inventoryDS string
	marketDS    string
	catalogDS   string
	gcsBucket   string
	ghToken     string
	ghOwner     string
	ghRepo      string

	mu          sync.Mutex
	lastSyncAt  *time.Time
	lastSyncErr *string
	inProgress  bool
}

func New(bq *bigquery.Client, gcs *storage.Client, project, inventoryDS, marketDS, catalogDS, gcsBucket, ghToken, ghOwner, ghRepo string) *Syncer {
	return &Syncer{
		bq:          bq,
		gcs:         gcs,
		project:     project,
		inventoryDS: inventoryDS,
		marketDS:    marketDS,
		catalogDS:   catalogDS,
		gcsBucket:   gcsBucket,
		ghToken:     ghToken,
		ghOwner:     ghOwner,
		ghRepo:      ghRepo,
	}
}

// Status returns the result of the most recent sync attempt.
func (s *Syncer) Status() Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Status{
		LastSyncAt:  s.lastSyncAt,
		LastSyncErr: s.lastSyncErr,
		InProgress:  s.inProgress,
	}
}

// Trigger fires SyncAll in the background. Safe to call from HTTP handlers.
func (s *Syncer) Trigger() {
	log.Printf("datasync: trigger called")
	s.mu.Lock()
	s.inProgress = true
	s.mu.Unlock()

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		log.Printf("datasync: starting sync")
		err := s.SyncAll(ctx)

		now := time.Now().UTC()
		s.mu.Lock()
		s.lastSyncAt = &now
		s.inProgress = false
		if err != nil {
			msg := err.Error()
			s.lastSyncErr = &msg
			log.Printf("datasync: sync failed: %v", err)
		} else {
			s.lastSyncErr = nil
			log.Printf("datasync: sync complete")
		}
		s.mu.Unlock()
	}()
}

func (s *Syncer) SyncAll(ctx context.Context) error {
	products, err := queryAll[productRow](ctx, s.bq,
		fmt.Sprintf(`SELECT
		  cp.product_id,
		  cp.name,
		  cp.game AS game_category,
		  cp.era AS game_subcategory,
		  cp.product_subtype AS product_category,
		  COALESCE(cp.tcgplayer_id, '') AS tcgplayer_id,
		  COALESCE(cp.pricecharting_url, '') AS pricecharting_url,
		  '' AS listing_url,
		  COALESCE(cp.image_url, '') AS image_url,
		  CURRENT_TIMESTAMP() AS created_at
		FROM `+"`%s.%s.catalog_products`"+` cp
		ORDER BY cp.product_type, cp.game, cp.set_code, cp.product_id`, s.project, s.inventoryDS))
	if err != nil {
		return fmt.Errorf("query products: %w", err)
	}

	transactions, err := queryAll[transactionRow](ctx, s.bq,
		fmt.Sprintf("SELECT * FROM `%s.%s.transactions` ORDER BY transaction_date, transaction_id", s.project, s.inventoryDS))
	if err != nil {
		return fmt.Errorf("query transactions: %w", err)
	}

	collection, err := queryAll[collectionRow](ctx, s.bq,
		fmt.Sprintf("SELECT * FROM `%s.%s.collection`", s.project, s.inventoryDS))
	if err != nil {
		return fmt.Errorf("query collection: %w", err)
	}

	// Enrich collection with display/binder placement data.
	displayPlacements, err := queryAll[displayPlacementRow](ctx, s.bq,
		fmt.Sprintf(`SELECT i.product_id, i.display_id, d.name AS display_name, SUM(i.quantity) AS quantity
		FROM `+"`%s.%s.showcase_display_items`"+` i
		JOIN `+"`%s.%s.showcase_displays`"+` d ON i.display_id = d.display_id
		GROUP BY i.product_id, i.display_id, d.name`, s.project, s.inventoryDS, s.project, s.inventoryDS))
	if err != nil {
		return fmt.Errorf("query display placements: %w", err)
	}

	binderPlacements, err := queryAll[binderPlacementRow](ctx, s.bq,
		fmt.Sprintf(`SELECT
		  CONCAT('card:', s.card_game, ':', s.card_set_code, ':', s.card_number) AS product_id,
		  s.binder_id, b.name AS binder_name, COUNT(*) AS card_count
		FROM `+"`%s.%s.binder_slots`"+` s
		JOIN `+"`%s.%s.binders`"+` b ON s.binder_id = b.binder_id
		WHERE s.card_game != '' AND s.card_set_code != '' AND s.card_number != ''
		GROUP BY product_id, s.binder_id, b.name`, s.project, s.inventoryDS, s.project, s.inventoryDS))
	if err != nil {
		return fmt.Errorf("query binder placements: %w", err)
	}

	enrichCollectionLocations(collection, displayPlacements, binderPlacements)

	priceHistory, err := queryAll[priceHistoryRow](ctx, s.bq,
		fmt.Sprintf("SELECT * FROM `%s.%s.price_history` ORDER BY snapshot_date, record_id", s.project, s.marketDS))
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

	if err := s.SyncBinders(ctx); err != nil {
		return fmt.Errorf("sync binders: %w", err)
	}
	if err := s.SyncDisplays(ctx); err != nil {
		return fmt.Errorf("sync displays: %w", err)
	}
	if err := s.SyncBoxBreaks(ctx); err != nil {
		return fmt.Errorf("sync box breaks: %w", err)
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

func nullFloat(n bigquery.NullFloat64) *float64 {
	if !n.Valid {
		return nil
	}
	return &n.Float64
}

func nullFloatVal(n bigquery.NullFloat64) float64 {
	return n.Float64
}

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
	TransactionID   string     `json:"transaction_id" bigquery:"transaction_id"`
	ProductID       string     `json:"product_id" bigquery:"product_id"`
	TransactionDate civil.Date `json:"transaction_date" bigquery:"transaction_date"`
	UnitPrice       float64    `json:"unit_price" bigquery:"unit_price"`
	Quantity        int64      `json:"quantity" bigquery:"quantity"`
	TransactionType string     `json:"transaction_type" bigquery:"transaction_type"`
	Platform        string     `json:"platform" bigquery:"platform"`
	Notes           string     `json:"notes" bigquery:"notes"`
	CreatedAt       time.Time  `json:"created_at" bigquery:"created_at"`
}

type collectionRow struct {
	ProductID         string               `json:"product_id" bigquery:"product_id"`
	Quantity          int64                `json:"quantity" bigquery:"quantity"`
	AvgUnitCost       bigquery.NullFloat64 `json:"-" bigquery:"avg_unit_cost"`
	TotalInvested     bigquery.NullFloat64 `json:"-" bigquery:"total_invested"`
	RealizedGain      bigquery.NullFloat64 `json:"-" bigquery:"realized_gain"`
	UnrealizedGain    bigquery.NullFloat64 `json:"-" bigquery:"unrealized_gain"`
	LatestMarketPrice bigquery.NullFloat64 `json:"-" bigquery:"latest_market_price"`
	FirstBuyDate      civil.Date           `json:"first_buy_date" bigquery:"first_buy_date"`
	DaysHeld          int64                `json:"days_held" bigquery:"days_held"`
	ROI               bigquery.NullFloat64 `json:"-" bigquery:"roi"`
	AnnualizedROI     bigquery.NullFloat64 `json:"-" bigquery:"annualized_roi"`

	// Enriched at sync time — not from BQ.
	Locations []collectionLocation `json:"-"`
}

type collectionLocation struct {
	Type     string `json:"type"`               // "display", "binder", or "unorganized"
	Name     string `json:"name,omitempty"`      // display/binder name
	ID       string `json:"id,omitempty"`        // display_id or binder_id
	Quantity int64  `json:"quantity"`
}

func (r collectionRow) MarshalJSON() ([]byte, error) {
	locs := r.Locations
	if locs == nil {
		locs = []collectionLocation{}
	}
	return json.Marshal(struct {
		ProductID         string               `json:"product_id"`
		Quantity          int64                `json:"quantity"`
		AvgUnitCost       float64              `json:"avg_unit_cost"`
		TotalInvested     float64              `json:"total_invested"`
		RealizedGain      float64              `json:"realized_gain"`
		UnrealizedGain    *float64             `json:"unrealized_gain"`
		LatestMarketPrice *float64             `json:"latest_market_price"`
		FirstBuyDate      civil.Date           `json:"first_buy_date"`
		DaysHeld          int64                `json:"days_held"`
		ROI               *float64             `json:"roi"`
		AnnualizedROI     *float64             `json:"annualized_roi"`
		Locations         []collectionLocation `json:"locations"`
	}{
		r.ProductID, r.Quantity,
		nullFloatVal(r.AvgUnitCost), nullFloatVal(r.TotalInvested), nullFloatVal(r.RealizedGain),
		nullFloat(r.UnrealizedGain), nullFloat(r.LatestMarketPrice),
		r.FirstBuyDate, r.DaysHeld,
		nullFloat(r.ROI), nullFloat(r.AnnualizedROI),
		locs,
	})
}

// displayPlacementRow aggregates display item quantities per product.
type displayPlacementRow struct {
	ProductID   string `bigquery:"product_id"`
	DisplayID   string `bigquery:"display_id"`
	DisplayName string `bigquery:"display_name"`
	Quantity    int64  `bigquery:"quantity"`
}

// binderPlacementRow aggregates binder slot counts per card product.
type binderPlacementRow struct {
	ProductID  string `bigquery:"product_id"`
	BinderID   string `bigquery:"binder_id"`
	BinderName string `bigquery:"binder_name"`
	CardCount  int64  `bigquery:"card_count"`
}

// enrichCollectionLocations merges display/binder placement data into collection rows.
func enrichCollectionLocations(collection []collectionRow, displayPlacements []displayPlacementRow, binderPlacements []binderPlacementRow) {
	// Build maps: product_id → []location
	locMap := make(map[string][]collectionLocation)
	placedQty := make(map[string]int64)

	for _, dp := range displayPlacements {
		locMap[dp.ProductID] = append(locMap[dp.ProductID], collectionLocation{
			Type: "display", Name: dp.DisplayName, ID: dp.DisplayID, Quantity: dp.Quantity,
		})
		placedQty[dp.ProductID] += dp.Quantity
	}
	for _, bp := range binderPlacements {
		locMap[bp.ProductID] = append(locMap[bp.ProductID], collectionLocation{
			Type: "binder", Name: bp.BinderName, ID: bp.BinderID, Quantity: bp.CardCount,
		})
		placedQty[bp.ProductID] += bp.CardCount
	}

	for i := range collection {
		pid := collection[i].ProductID
		locs := locMap[pid]
		unorganized := collection[i].Quantity - placedQty[pid]
		if unorganized > 0 {
			locs = append(locs, collectionLocation{Type: "unorganized", Quantity: unorganized})
		}
		if locs == nil {
			locs = []collectionLocation{{Type: "unorganized", Quantity: collection[i].Quantity}}
		}
		collection[i].Locations = locs
	}
}

type priceHistoryRow struct {
	RecordID           string     `json:"record_id" bigquery:"record_id"`
	ProductID          string     `json:"product_id" bigquery:"product_id"`
	SnapshotDate       civil.Date `json:"snapshot_date" bigquery:"snapshot_date"`
	Source             string     `json:"source" bigquery:"source"`
	MarketPrice        bigquery.NullFloat64 `json:"-" bigquery:"market_price"`
	MedianPrice        bigquery.NullFloat64 `json:"-" bigquery:"median_price"`
	SellThroughRate    float64    `json:"sell_through_rate" bigquery:"sell_through_rate"`
	DistinctBuyerCount int64      `json:"distinct_buyer_count" bigquery:"distinct_buyer_count"`
	ListedCount        int64      `json:"listed_count" bigquery:"listed_count"`
	CreatedAt          time.Time  `json:"created_at" bigquery:"created_at"`
}

func (r priceHistoryRow) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		RecordID           string     `json:"record_id"`
		ProductID          string     `json:"product_id"`
		SnapshotDate       civil.Date `json:"snapshot_date"`
		Source             string     `json:"source"`
		MarketPrice        float64    `json:"market_price"`
		MedianPrice        float64    `json:"median_price"`
		SellThroughRate    float64    `json:"sell_through_rate"`
		DistinctBuyerCount int64      `json:"distinct_buyer_count"`
		ListedCount        int64      `json:"listed_count"`
		CreatedAt          time.Time  `json:"created_at"`
	}{
		r.RecordID, r.ProductID, r.SnapshotDate, r.Source,
		nullFloatVal(r.MarketPrice), nullFloatVal(r.MedianPrice),
		r.SellThroughRate, r.DistinctBuyerCount, r.ListedCount, r.CreatedAt,
	})
}

// ── Binder / display sync row types ──────────────────────────────────────────

type binderSyncRow struct {
	BinderID     string    `bigquery:"binder_id"`
	Name         string    `bigquery:"name"`
	Description  string    `bigquery:"description"`
	SlotsPerPage int64     `bigquery:"slots_per_page"`
	CreatedAt    time.Time `bigquery:"created_at"`
}

type binderSlotSyncRow struct {
	BinderID     string              `bigquery:"binder_id"`
	PageNumber   int64               `bigquery:"page_number"`
	Side         string              `bigquery:"side"`
	SlotPosition int64               `bigquery:"slot_position"`
	CardGame     string              `bigquery:"card_game"`
	CardSetCode  string              `bigquery:"card_set_code"`
	CardNumber   string              `bigquery:"card_number"`
	CardNotes    string              `bigquery:"card_notes"`
	TcgplayerID  bigquery.NullString `bigquery:"tcgplayer_id"`
	UpdatedAt    time.Time           `bigquery:"updated_at"`
}

type displaySyncRow struct {
	DisplayID   string    `bigquery:"display_id"`
	Name        string    `bigquery:"name"`
	DisplayType string    `bigquery:"display_type"`
	Description string    `bigquery:"description"`
	CreatedAt   time.Time `bigquery:"created_at"`
}

type displayItemSyncRow struct {
	DisplayID       string              `bigquery:"display_id"`
	Position        int64               `bigquery:"position"`
	ProductID       string              `bigquery:"product_id"`
	Quantity        int64               `bigquery:"quantity"`
	Notes           string              `bigquery:"notes"`
	ProductName     bigquery.NullString `bigquery:"product_name"`
	GameCategory    bigquery.NullString `bigquery:"game_category"`
	GameSubcategory bigquery.NullString `bigquery:"game_subcategory"`
	ProductCategory bigquery.NullString `bigquery:"product_category"`
	ImageURL        bigquery.NullString `bigquery:"image_url"`
	UpdatedAt       time.Time           `bigquery:"updated_at"`
}

// ── Output JSON types ─────────────────────────────────────────────────────────

type binderIndexEntry struct {
	BinderID     string `json:"binder_id"`
	Name         string `json:"name"`
	SlotsPerPage int64  `json:"slots_per_page"`
	PageCount    int    `json:"page_count"`
}

type binderDetail struct {
	BinderID     string       `json:"binder_id"`
	Name         string       `json:"name"`
	Description  string       `json:"description"`
	SlotsPerPage int64        `json:"slots_per_page"`
	CreatedAt    time.Time    `json:"created_at"`
	Pages        []binderPage `json:"pages"`
}

type binderPage struct {
	PageNumber int             `json:"page_number"`
	Front      []binderSlotOut `json:"front"`
	Back       []binderSlotOut `json:"back"`
}

type binderSlotOut struct {
	SlotPosition int    `json:"slot_position"`
	CardGame     string `json:"card_game"`
	CardSetCode  string `json:"card_set_code"`
	CardNumber   string `json:"card_number"`
	CardNotes    string `json:"card_notes"`
	ImageURL     string `json:"image_url"`
	Empty        bool   `json:"empty"`
}

type displayIndexEntry struct {
	DisplayID   string `json:"display_id"`
	Name        string `json:"name"`
	DisplayType string `json:"display_type"`
}

type displayDetail struct {
	DisplayID   string           `json:"display_id"`
	Name        string           `json:"name"`
	DisplayType string           `json:"display_type"`
	Description string           `json:"description"`
	CreatedAt   time.Time        `json:"created_at"`
	Items       []displayItemOut `json:"items"`
}

type displayItemOut struct {
	Position        int64  `json:"position"`
	ProductID       string `json:"product_id"`
	Quantity        int64  `json:"quantity"`
	Notes           string `json:"notes"`
	ProductName     string `json:"product_name"`
	ImageURL        string `json:"image_url"`
	GameCategory    string `json:"game_category"`
	GameSubcategory string `json:"game_subcategory"`
	ProductCategory string `json:"product_category"`
}

// ── SyncBinders ───────────────────────────────────────────────────────────────

func (s *Syncer) SyncBinders(ctx context.Context) error {
	binders, err := queryAll[binderSyncRow](ctx, s.bq,
		fmt.Sprintf("SELECT * FROM `%s.%s.binders` ORDER BY created_at", s.project, s.inventoryDS))
	if err != nil {
		return fmt.Errorf("query binders: %w", err)
	}

	index := make([]binderIndexEntry, 0, len(binders))

	for _, b := range binders {
		slotSQL := fmt.Sprintf(`
SELECT s.binder_id, s.page_number, s.side, s.slot_position,
       s.card_game, s.card_set_code, s.card_number, s.card_notes,
       c.tcgplayer_id, s.updated_at
FROM `+"`%s.%s.binder_slots`"+` s
LEFT JOIN `+"`%s.%s.single_cards`"+` c
  ON s.card_game = c.game AND s.card_set_code = c.set_code AND s.card_number = c.card_number
WHERE s.binder_id = @binder_id
ORDER BY s.page_number, CASE WHEN s.side = 'front' THEN 0 ELSE 1 END, s.slot_position`,
			s.project, s.inventoryDS, s.project, s.catalogDS)

		q := s.bq.Query(slotSQL)
		q.Parameters = []bigquery.QueryParameter{{Name: "binder_id", Value: b.BinderID}}
		it, err := q.Read(ctx)
		if err != nil {
			return fmt.Errorf("query slots for binder %s: %w", b.BinderID, err)
		}

		// Group slots by page -> side
		type pageKey = int64
		pageSlots := make(map[pageKey]map[string][]binderSlotSyncRow)
		pageOrder := []int64{}
		pageSet := make(map[int64]bool)

		for {
			var row binderSlotSyncRow
			if err := it.Next(&row); err != nil {
				if err == iterator.Done {
					break
				}
				return fmt.Errorf("iterate slots for binder %s: %w", b.BinderID, err)
			}
			if _, ok := pageSlots[row.PageNumber]; !ok {
				pageSlots[row.PageNumber] = make(map[string][]binderSlotSyncRow)
				if !pageSet[row.PageNumber] {
					pageOrder = append(pageOrder, row.PageNumber)
					pageSet[row.PageNumber] = true
				}
			}
			pageSlots[row.PageNumber][row.Side] = append(pageSlots[row.PageNumber][row.Side], row)
		}

		pages := make([]binderPage, 0, len(pageOrder))
		for _, pn := range pageOrder {
			sides := pageSlots[pn]
			front := buildSideSlots(sides["front"], int(b.SlotsPerPage))
			back := buildSideSlots(sides["back"], int(b.SlotsPerPage))
			pages = append(pages, binderPage{
				PageNumber: int(pn),
				Front:      front,
				Back:       back,
			})
		}

		detail := binderDetail{
			BinderID:     b.BinderID,
			Name:         b.Name,
			Description:  b.Description,
			SlotsPerPage: b.SlotsPerPage,
			CreatedAt:    b.CreatedAt,
			Pages:        pages,
		}

		detailBytes, err := json.Marshal(detail)
		if err != nil {
			return fmt.Errorf("marshal binder %s: %w", b.BinderID, err)
		}

		detailPath := fmt.Sprintf("binders/%s.json", b.BinderID)
		if err := s.uploadFile(ctx, detailPath, detailBytes); err != nil {
			return fmt.Errorf("upload %s: %w", detailPath, err)
		}

		index = append(index, binderIndexEntry{
			BinderID:     b.BinderID,
			Name:         b.Name,
			SlotsPerPage: b.SlotsPerPage,
			PageCount:    len(pageOrder),
		})
	}

	indexBytes, err := json.Marshal(index)
	if err != nil {
		return fmt.Errorf("marshal binders index: %w", err)
	}
	return s.uploadFile(ctx, "binders/index.json", indexBytes)
}

// buildSideSlots returns a full array of slotsPerPage entries filling gaps with empty slots.
func buildSideSlots(rows []binderSlotSyncRow, slotsPerPage int) []binderSlotOut {
	byPos := make(map[int]binderSlotSyncRow)
	for _, r := range rows {
		byPos[int(r.SlotPosition)] = r
	}
	out := make([]binderSlotOut, slotsPerPage)
	for i := 0; i < slotsPerPage; i++ {
		pos := i + 1
		row, ok := byPos[pos]
		if !ok || (row.CardGame == "" && row.CardSetCode == "" && row.CardNumber == "") {
			out[i] = binderSlotOut{SlotPosition: pos, Empty: true}
			continue
		}
		imgURL := ""
		if row.TcgplayerID.Valid && row.TcgplayerID.StringVal != "" {
			imgURL = fmt.Sprintf("https://tcgplayer-cdn.tcgplayer.com/product/%s_in_1000x1000.jpg", row.TcgplayerID.StringVal)
		}
		out[i] = binderSlotOut{
			SlotPosition: pos,
			CardGame:     row.CardGame,
			CardSetCode:  row.CardSetCode,
			CardNumber:   row.CardNumber,
			CardNotes:    row.CardNotes,
			ImageURL:     imgURL,
			Empty:        false,
		}
	}
	return out
}

// ── SyncDisplays ──────────────────────────────────────────────────────────────

func (s *Syncer) SyncDisplays(ctx context.Context) error {
	displays, err := queryAll[displaySyncRow](ctx, s.bq,
		fmt.Sprintf("SELECT * FROM `%s.%s.showcase_displays` ORDER BY created_at", s.project, s.inventoryDS))
	if err != nil {
		return fmt.Errorf("query displays: %w", err)
	}

	index := make([]displayIndexEntry, 0, len(displays))

	for _, d := range displays {
		itemSQL := fmt.Sprintf(`
SELECT i.display_id, i.position, i.product_id, i.quantity, i.notes,
       cp.name AS product_name,
       cp.game AS game_category,
       cp.era AS game_subcategory,
       cp.product_subtype AS product_category,
       cp.image_url, i.updated_at
FROM `+"`%s.%s.showcase_display_items`"+` i
LEFT JOIN `+"`%s.%s.catalog_products`"+` cp ON i.product_id = cp.product_id
WHERE i.display_id = @display_id
ORDER BY i.position`,
			s.project, s.inventoryDS, s.project, s.inventoryDS)

		q := s.bq.Query(itemSQL)
		q.Parameters = []bigquery.QueryParameter{{Name: "display_id", Value: d.DisplayID}}
		it, err := q.Read(ctx)
		if err != nil {
			return fmt.Errorf("query items for display %s: %w", d.DisplayID, err)
		}

		var items []displayItemOut
		for {
			var row displayItemSyncRow
			if err := it.Next(&row); err != nil {
				if err == iterator.Done {
					break
				}
				return fmt.Errorf("iterate items for display %s: %w", d.DisplayID, err)
			}
			items = append(items, displayItemOut{
				Position:        row.Position,
				ProductID:       row.ProductID,
				Quantity:        row.Quantity,
				Notes:           row.Notes,
				ProductName:     nullStr(row.ProductName),
				ImageURL:        nullStr(row.ImageURL),
				GameCategory:    nullStr(row.GameCategory),
				GameSubcategory: nullStr(row.GameSubcategory),
				ProductCategory: nullStr(row.ProductCategory),
			})
		}
		if items == nil {
			items = []displayItemOut{}
		}

		detail := displayDetail{
			DisplayID:   d.DisplayID,
			Name:        d.Name,
			DisplayType: d.DisplayType,
			Description: d.Description,
			CreatedAt:   d.CreatedAt,
			Items:       items,
		}

		detailBytes, err := json.Marshal(detail)
		if err != nil {
			return fmt.Errorf("marshal display %s: %w", d.DisplayID, err)
		}

		detailPath := fmt.Sprintf("displays/%s.json", d.DisplayID)
		if err := s.uploadFile(ctx, detailPath, detailBytes); err != nil {
			return fmt.Errorf("upload %s: %w", detailPath, err)
		}

		index = append(index, displayIndexEntry{
			DisplayID:   d.DisplayID,
			Name:        d.Name,
			DisplayType: d.DisplayType,
		})
	}

	indexBytes, err := json.Marshal(index)
	if err != nil {
		return fmt.Errorf("marshal displays index: %w", err)
	}
	return s.uploadFile(ctx, "displays/index.json", indexBytes)
}

// nullStr extracts a string from a NullString, returning "" if not valid.
func nullStr(n bigquery.NullString) string {
	if n.Valid {
		return n.StringVal
	}
	return ""
}

// ── Box break sync row / output types ─────────────────────────────────────────

type boxBreakSyncRow struct {
	BreakID           string     `bigquery:"break_id"`
	SealedProductID   string     `bigquery:"sealed_product_id"`
	BreakDate         civil.Date `bigquery:"break_date"`
	SealedMarketValue float64    `bigquery:"sealed_market_value"`
	BinderID          bigquery.NullString `bigquery:"binder_id"`
	Notes             bigquery.NullString `bigquery:"notes"`
	CreatedAt         time.Time  `bigquery:"created_at"`
	// Joined sealed product metadata.
	SealedName      bigquery.NullString `bigquery:"sealed_name"`
	SealedImageURL  bigquery.NullString `bigquery:"sealed_image_url"`
	SealedGame      bigquery.NullString `bigquery:"sealed_game"`
	SealedEra       bigquery.NullString `bigquery:"sealed_era"`
	SealedSubtype   bigquery.NullString `bigquery:"sealed_subtype"`
	// Joined buy-side aggregates for the sealed product.
	SealedBuyQty   bigquery.NullInt64   `bigquery:"sealed_buy_qty"`
	SealedAvgCost  bigquery.NullFloat64 `bigquery:"sealed_avg_cost"`
}

type boxBreakPullSyncRow struct {
	PullID                    string              `bigquery:"pull_id"`
	BreakID                   string              `bigquery:"break_id"`
	PullType                  string              `bigquery:"pull_type"`
	ProductID                 bigquery.NullString `bigquery:"product_id"`
	BulkGame                  bigquery.NullString `bigquery:"bulk_game"`
	BulkSetCode               bigquery.NullString `bigquery:"bulk_set_code"`
	BulkLabel                 bigquery.NullString `bigquery:"bulk_label"`
	Quantity                  int64               `bigquery:"quantity"`
	MarketValuePerUnit        float64             `bigquery:"market_value_per_unit"`
	AllocatedCostBasisPerUnit float64             `bigquery:"allocated_cost_basis_per_unit"`
	Notes                     bigquery.NullString `bigquery:"notes"`
	CreatedAt                 time.Time           `bigquery:"created_at"`
	// Joined single-card metadata (for "single" pulls).
	ProductName     bigquery.NullString `bigquery:"product_name"`
	ProductImageURL bigquery.NullString `bigquery:"product_image_url"`
	// Joined latest market price for unrealized P&L on singles.
	LatestMarketPrice bigquery.NullFloat64 `bigquery:"latest_market_price"`
}

type boxBreakIndexEntry struct {
	BreakID            string     `json:"break_id"`
	SealedProductID    string     `json:"sealed_product_id"`
	SealedName         string     `json:"sealed_name"`
	SealedImageURL     string     `json:"sealed_image_url"`
	BreakDate          civil.Date `json:"break_date"`
	SealedMarketValue  float64    `json:"sealed_market_value"`
	SealedAvgCost      float64    `json:"sealed_avg_cost"`
	SealedRealizedGain float64    `json:"sealed_realized_gain"`
	PullCount          int        `json:"pull_count"`
	TotalPullValue     float64    `json:"total_pull_market_value"`
	BinderID           string     `json:"binder_id"`
}

type boxBreakDetail struct {
	BreakID            string            `json:"break_id"`
	SealedProductID    string            `json:"sealed_product_id"`
	SealedName         string            `json:"sealed_name"`
	SealedImageURL     string            `json:"sealed_image_url"`
	SealedGame         string            `json:"sealed_game"`
	SealedEra          string            `json:"sealed_era"`
	SealedSubtype      string            `json:"sealed_subtype"`
	BreakDate          civil.Date        `json:"break_date"`
	SealedMarketValue  float64           `json:"sealed_market_value"`
	SealedAvgCost      float64           `json:"sealed_avg_cost"`
	SealedRealizedGain float64           `json:"sealed_realized_gain"`
	BinderID           string            `json:"binder_id"`
	Notes              string            `json:"notes"`
	CreatedAt          time.Time         `json:"created_at"`
	Pulls              []boxBreakPullOut `json:"pulls"`
}

type boxBreakPullOut struct {
	PullID                    string   `json:"pull_id"`
	PullType                  string   `json:"pull_type"`
	ProductID                 string   `json:"product_id"`
	ProductName               string   `json:"product_name"`
	ProductImageURL           string   `json:"product_image_url"`
	BulkGame                  string   `json:"bulk_game"`
	BulkSetCode               string   `json:"bulk_set_code"`
	BulkLabel                 string   `json:"bulk_label"`
	Quantity                  int64    `json:"quantity"`
	MarketValuePerUnit        float64  `json:"market_value_per_unit"`
	AllocatedCostBasisPerUnit float64  `json:"allocated_cost_basis_per_unit"`
	LatestMarketPrice         *float64 `json:"latest_market_price"`
	Notes                     string   `json:"notes"`
}

// ── SyncBoxBreaks ─────────────────────────────────────────────────────────────

func (s *Syncer) SyncBoxBreaks(ctx context.Context) error {
	// One query gives us break metadata enriched with sealed product info and buy-side aggregates.
	// sealed_avg_cost is the average buy price for the sealed product across all buy transactions
	// (not including the synthetic sell at break time); sealed_realized_gain is computed client-side.
	breakSQL := fmt.Sprintf(`
SELECT
  b.break_id, b.sealed_product_id, b.break_date, b.sealed_market_value,
  b.binder_id, b.notes, b.created_at,
  cp.name AS sealed_name,
  cp.image_url AS sealed_image_url,
  cp.game AS sealed_game,
  cp.era AS sealed_era,
  cp.product_subtype AS sealed_subtype,
  agg.total_buy_qty AS sealed_buy_qty,
  SAFE_DIVIDE(agg.total_buy_value, agg.total_buy_qty) AS sealed_avg_cost
FROM `+"`%s.%s.box_breaks`"+` b
LEFT JOIN `+"`%s.%s.catalog_products`"+` cp ON b.sealed_product_id = cp.product_id
LEFT JOIN (
  SELECT
    product_id,
    SUM(CASE WHEN transaction_type = 'buy' THEN quantity ELSE 0 END) AS total_buy_qty,
    SUM(CASE WHEN transaction_type = 'buy' THEN unit_price * quantity ELSE 0 END) AS total_buy_value
  FROM `+"`%s.%s.transactions`"+`
  GROUP BY product_id
) agg ON agg.product_id = b.sealed_product_id
ORDER BY b.break_date DESC, b.created_at DESC`,
		s.project, s.inventoryDS, s.project, s.inventoryDS, s.project, s.inventoryDS)

	breaks, err := queryAll[boxBreakSyncRow](ctx, s.bq, breakSQL)
	if err != nil {
		return fmt.Errorf("query box_breaks: %w", err)
	}

	index := make([]boxBreakIndexEntry, 0, len(breaks))

	for _, b := range breaks {
		pulls, err := s.queryBoxBreakPulls(ctx, b.BreakID)
		if err != nil {
			return fmt.Errorf("query pulls for break %s: %w", b.BreakID, err)
		}

		totalPullValue := 0.0
		for _, p := range pulls {
			totalPullValue += float64(p.Quantity) * p.MarketValuePerUnit
		}

		sealedAvgCost := nullFloatVal(b.SealedAvgCost)
		sealedRealized := b.SealedMarketValue - sealedAvgCost // per-box realized P&L

		detail := boxBreakDetail{
			BreakID:            b.BreakID,
			SealedProductID:    b.SealedProductID,
			SealedName:         nullStr(b.SealedName),
			SealedImageURL:     nullStr(b.SealedImageURL),
			SealedGame:         nullStr(b.SealedGame),
			SealedEra:          nullStr(b.SealedEra),
			SealedSubtype:      nullStr(b.SealedSubtype),
			BreakDate:          b.BreakDate,
			SealedMarketValue:  b.SealedMarketValue,
			SealedAvgCost:      sealedAvgCost,
			SealedRealizedGain: sealedRealized,
			BinderID:           nullStr(b.BinderID),
			Notes:              nullStr(b.Notes),
			CreatedAt:          b.CreatedAt,
			Pulls:              pulls,
		}

		detailBytes, err := json.Marshal(detail)
		if err != nil {
			return fmt.Errorf("marshal box break %s: %w", b.BreakID, err)
		}
		detailPath := fmt.Sprintf("box-breaks/%s.json", b.BreakID)
		if err := s.uploadFile(ctx, detailPath, detailBytes); err != nil {
			return fmt.Errorf("upload %s: %w", detailPath, err)
		}

		index = append(index, boxBreakIndexEntry{
			BreakID:            b.BreakID,
			SealedProductID:    b.SealedProductID,
			SealedName:         nullStr(b.SealedName),
			SealedImageURL:     nullStr(b.SealedImageURL),
			BreakDate:          b.BreakDate,
			SealedMarketValue:  b.SealedMarketValue,
			SealedAvgCost:      sealedAvgCost,
			SealedRealizedGain: sealedRealized,
			PullCount:          len(pulls),
			TotalPullValue:     totalPullValue,
			BinderID:           nullStr(b.BinderID),
		})
	}

	indexBytes, err := json.Marshal(index)
	if err != nil {
		return fmt.Errorf("marshal box_breaks index: %w", err)
	}
	return s.uploadFile(ctx, "box-breaks/index.json", indexBytes)
}

func (s *Syncer) queryBoxBreakPulls(ctx context.Context, breakID string) ([]boxBreakPullOut, error) {
	// Join single-card metadata (via catalog_products) + latest tcgplayer price for unrealized P&L.
	pullSQL := fmt.Sprintf(`
SELECT
  p.pull_id, p.break_id, p.pull_type, p.product_id, p.bulk_game, p.bulk_set_code, p.bulk_label,
  p.quantity, p.market_value_per_unit, p.allocated_cost_basis_per_unit, p.notes, p.created_at,
  cp.name AS product_name,
  cp.image_url AS product_image_url,
  lp.latest_market_price
FROM `+"`%s.%s.box_break_pulls`"+` p
LEFT JOIN `+"`%s.%s.catalog_products`"+` cp ON p.product_id = cp.product_id
LEFT JOIN (
  SELECT cp.product_id, ph.market_price AS latest_market_price
  FROM `+"`%s.%s.catalog_products`"+` cp
  JOIN `+"`%s.%s.tcgplayer_price_history`"+` ph ON cp.tcgplayer_id = ph.tcgplayer_id
  QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.product_id ORDER BY ph.date DESC) = 1
) lp ON lp.product_id = p.product_id
WHERE p.break_id = @break_id
ORDER BY p.pull_type, p.market_value_per_unit DESC, p.created_at`,
		s.project, s.inventoryDS, s.project, s.inventoryDS, s.project, s.inventoryDS, s.project, s.marketDS)

	q := s.bq.Query(pullSQL)
	q.Parameters = []bigquery.QueryParameter{{Name: "break_id", Value: breakID}}
	it, err := q.Read(ctx)
	if err != nil {
		return nil, err
	}

	var out []boxBreakPullOut
	for {
		var row boxBreakPullSyncRow
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			return nil, err
		}
		out = append(out, boxBreakPullOut{
			PullID:                    row.PullID,
			PullType:                  row.PullType,
			ProductID:                 nullStr(row.ProductID),
			ProductName:               nullStr(row.ProductName),
			ProductImageURL:           nullStr(row.ProductImageURL),
			BulkGame:                  nullStr(row.BulkGame),
			BulkSetCode:               nullStr(row.BulkSetCode),
			BulkLabel:                 nullStr(row.BulkLabel),
			Quantity:                  row.Quantity,
			MarketValuePerUnit:        row.MarketValuePerUnit,
			AllocatedCostBasisPerUnit: row.AllocatedCostBasisPerUnit,
			LatestMarketPrice:         nullFloat(row.LatestMarketPrice),
			Notes:                     nullStr(row.Notes),
		})
	}
	if out == nil {
		out = []boxBreakPullOut{}
	}
	return out, nil
}

// uploadFile uploads to GCS and optionally to GitHub.
func (s *Syncer) uploadFile(ctx context.Context, name string, data []byte) error {
	log.Printf("datasync: uploading %s to GCS (%d bytes)", name, len(data))
	if err := s.uploadGCS(ctx, name, data); err != nil {
		log.Printf("datasync: GCS upload failed for %s: %v", name, err)
		return fmt.Errorf("gcs upload %s: %w", name, err)
	}
	log.Printf("datasync: GCS upload OK for %s", name)
	if s.ghToken != "" {
		log.Printf("datasync: upserting %s to GitHub", name)
		if err := s.upsertGitHub(ctx, name, data); err != nil {
			log.Printf("datasync: GitHub upsert failed for %s: %v", name, err)
			return fmt.Errorf("github upsert %s: %w", name, err)
		}
		log.Printf("datasync: GitHub upsert OK for %s", name)
	}
	return nil
}
