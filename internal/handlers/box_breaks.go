package handlers

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type BoxBreakHandler struct {
	client      *bigquery.Client
	dataset     string
	triggerSync func()
}

func NewBoxBreakHandler(client *bigquery.Client, dataset string, triggerSync func()) *BoxBreakHandler {
	return &BoxBreakHandler{client: client, dataset: dataset, triggerSync: triggerSync}
}

// Row types — mirror the BQ schema.

type BoxBreak struct {
	BreakID           string    `json:"break_id" bigquery:"break_id"`
	SealedProductID   string    `json:"sealed_product_id" bigquery:"sealed_product_id"`
	BreakDate         string    `json:"break_date" bigquery:"break_date"`
	SealedMarketValue float64   `json:"sealed_market_value" bigquery:"sealed_market_value"`
	BinderID          string    `json:"binder_id" bigquery:"binder_id"`
	Notes             string    `json:"notes" bigquery:"notes"`
	CreatedAt         time.Time `json:"created_at" bigquery:"created_at"`
}

type BoxBreakPull struct {
	PullID                    string    `json:"pull_id" bigquery:"pull_id"`
	BreakID                   string    `json:"break_id" bigquery:"break_id"`
	PullType                  string    `json:"pull_type" bigquery:"pull_type"`
	ProductID                 string    `json:"product_id" bigquery:"product_id"`
	BulkGame                  string    `json:"bulk_game" bigquery:"bulk_game"`
	BulkSetCode               string    `json:"bulk_set_code" bigquery:"bulk_set_code"`
	BulkLabel                 string    `json:"bulk_label" bigquery:"bulk_label"`
	Quantity                  int64     `json:"quantity" bigquery:"quantity"`
	MarketValuePerUnit        float64   `json:"market_value_per_unit" bigquery:"market_value_per_unit"`
	AllocatedCostBasisPerUnit float64   `json:"allocated_cost_basis_per_unit" bigquery:"allocated_cost_basis_per_unit"`
	Notes                     string    `json:"notes" bigquery:"notes"`
	CreatedAt                 time.Time `json:"created_at" bigquery:"created_at"`
}

// Request types.

type PullInput struct {
	PullType           string  `json:"pull_type" binding:"required"`
	ProductID          string  `json:"product_id"`
	BulkGame           string  `json:"bulk_game"`
	BulkSetCode        string  `json:"bulk_set_code"`
	BulkLabel          string  `json:"bulk_label"`
	Quantity           int64   `json:"quantity" binding:"required"`
	MarketValuePerUnit float64 `json:"market_value_per_unit"`
	Notes              string  `json:"notes"`
}

type CreateBoxBreakRequest struct {
	SealedProductID   string      `json:"sealed_product_id" binding:"required"`
	BreakDate         string      `json:"break_date" binding:"required"`
	SealedMarketValue float64     `json:"sealed_market_value" binding:"required"`
	BinderID          string      `json:"binder_id"`
	Notes             string      `json:"notes"`
	Pulls             []PullInput `json:"pulls"`
}

type UpdateBoxBreakRequest struct {
	BreakDate         string  `json:"break_date"`
	SealedMarketValue float64 `json:"sealed_market_value"`
	BinderID          string  `json:"binder_id"`
	Notes             string  `json:"notes"`
}

type ReplacePullsRequest struct {
	Pulls []PullInput `json:"pulls" binding:"required"`
}

func validatePulls(pulls []PullInput) error {
	for i, p := range pulls {
		if p.PullType != "single" && p.PullType != "bulk" {
			return fmt.Errorf("pulls[%d]: pull_type must be 'single' or 'bulk'", i)
		}
		if p.Quantity <= 0 {
			return fmt.Errorf("pulls[%d]: quantity must be greater than 0", i)
		}
		if p.MarketValuePerUnit < 0 {
			return fmt.Errorf("pulls[%d]: market_value_per_unit must be >= 0", i)
		}
		if p.PullType == "single" && p.ProductID == "" {
			return fmt.Errorf("pulls[%d]: product_id is required for single pulls", i)
		}
		if p.PullType == "bulk" && p.BulkGame == "" {
			return fmt.Errorf("pulls[%d]: bulk_game is required for bulk pulls", i)
		}
	}
	return nil
}

// allocateCostBasis computes per-pull allocated_cost_basis_per_unit using pro-rata-by-market-value.
// Result sums to sealedMarketValue when weighted by quantity. Returns zeros if total pull value is 0.
func allocateCostBasis(pulls []PullInput, sealedMarketValue float64) []float64 {
	total := 0.0
	for _, p := range pulls {
		total += float64(p.Quantity) * p.MarketValuePerUnit
	}
	out := make([]float64, len(pulls))
	if total <= 0 {
		return out
	}
	ratio := sealedMarketValue / total
	for i, p := range pulls {
		out[i] = p.MarketValuePerUnit * ratio
	}
	return out
}

func (h *BoxBreakHandler) List(c *gin.Context) {
	sql := fmt.Sprintf("SELECT * FROM `%s.%s.box_breaks` ORDER BY break_date DESC, created_at DESC",
		h.client.Project(), h.dataset)
	it, err := h.client.Query(sql).Read(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	var breaks []BoxBreak
	for {
		var row BoxBreak
		if err := it.Next(&row); err != nil {
			break
		}
		breaks = append(breaks, row)
	}
	if breaks == nil {
		breaks = []BoxBreak{}
	}
	c.JSON(http.StatusOK, breaks)
}

func (h *BoxBreakHandler) Get(c *gin.Context) {
	id := c.Param("id")
	ctx := c.Request.Context()

	sql := fmt.Sprintf("SELECT * FROM `%s.%s.box_breaks` WHERE break_id = @break_id LIMIT 1",
		h.client.Project(), h.dataset)
	q := h.client.Query(sql)
	q.Parameters = []bigquery.QueryParameter{{Name: "break_id", Value: id}}
	it, err := q.Read(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	var br BoxBreak
	if err := it.Next(&br); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "box break not found"})
		return
	}

	pulls, err := h.queryPulls(ctx, id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"break_id":            br.BreakID,
		"sealed_product_id":   br.SealedProductID,
		"break_date":          br.BreakDate,
		"sealed_market_value": br.SealedMarketValue,
		"binder_id":           br.BinderID,
		"notes":               br.Notes,
		"created_at":          br.CreatedAt,
		"pulls":               pulls,
	})
}

func (h *BoxBreakHandler) queryPulls(ctx context.Context, breakID string) ([]BoxBreakPull, error) {
	sql := fmt.Sprintf("SELECT * FROM `%s.%s.box_break_pulls` WHERE break_id = @break_id ORDER BY pull_type, market_value_per_unit DESC, created_at",
		h.client.Project(), h.dataset)
	q := h.client.Query(sql)
	q.Parameters = []bigquery.QueryParameter{{Name: "break_id", Value: breakID}}
	it, err := q.Read(ctx)
	if err != nil {
		return nil, err
	}
	var pulls []BoxBreakPull
	for {
		var row BoxBreakPull
		if err := it.Next(&row); err != nil {
			break
		}
		pulls = append(pulls, row)
	}
	if pulls == nil {
		pulls = []BoxBreakPull{}
	}
	return pulls, nil
}

func (h *BoxBreakHandler) Create(c *gin.Context) {
	var req CreateBoxBreakRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.SealedMarketValue <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "sealed_market_value must be greater than 0"})
		return
	}
	if err := validatePulls(req.Pulls); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	breakID := uuid.New().String()
	createdAt := time.Now().UTC()
	ctx := c.Request.Context()

	insertBreakSQL := fmt.Sprintf(`INSERT INTO `+"`%s.%s.box_breaks`"+`
		(break_id, sealed_product_id, break_date, sealed_market_value, binder_id, notes, created_at)
		VALUES (@break_id, @sealed_product_id, @break_date, @sealed_market_value, @binder_id, @notes, @created_at)`,
		h.client.Project(), h.dataset)
	q := h.client.Query(insertBreakSQL)
	q.Parameters = []bigquery.QueryParameter{
		{Name: "break_id", Value: breakID},
		{Name: "sealed_product_id", Value: req.SealedProductID},
		{Name: "break_date", Value: req.BreakDate},
		{Name: "sealed_market_value", Value: req.SealedMarketValue},
		{Name: "binder_id", Value: req.BinderID},
		{Name: "notes", Value: req.Notes},
		{Name: "created_at", Value: createdAt},
	}
	job, err := q.Run(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if _, err := job.Wait(ctx); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if len(req.Pulls) > 0 {
		if err := h.insertPulls(ctx, breakID, req.Pulls, req.SealedMarketValue, createdAt); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
	}

	c.JSON(http.StatusCreated, gin.H{"break_id": breakID})
	if h.triggerSync != nil {
		h.triggerSync()
	}
}

// insertPulls computes cost basis allocation and bulk-inserts the pull rows.
func (h *BoxBreakHandler) insertPulls(ctx context.Context, breakID string, pulls []PullInput, sealedMarketValue float64, createdAt time.Time) error {
	allocated := allocateCostBasis(pulls, sealedMarketValue)

	valueRows := make([]string, len(pulls))
	params := []bigquery.QueryParameter{}
	for i, p := range pulls {
		s := fmt.Sprintf("_%d", i)
		valueRows[i] = fmt.Sprintf(
			"(@pull_id%s, @break_id%s, @pull_type%s, @product_id%s, @bulk_game%s, @bulk_set_code%s, @bulk_label%s, @quantity%s, @mv%s, @alloc%s, @notes%s, @created_at%s)",
			s, s, s, s, s, s, s, s, s, s, s, s,
		)
		params = append(params,
			bigquery.QueryParameter{Name: "pull_id" + s, Value: uuid.New().String()},
			bigquery.QueryParameter{Name: "break_id" + s, Value: breakID},
			bigquery.QueryParameter{Name: "pull_type" + s, Value: p.PullType},
			bigquery.QueryParameter{Name: "product_id" + s, Value: p.ProductID},
			bigquery.QueryParameter{Name: "bulk_game" + s, Value: p.BulkGame},
			bigquery.QueryParameter{Name: "bulk_set_code" + s, Value: p.BulkSetCode},
			bigquery.QueryParameter{Name: "bulk_label" + s, Value: p.BulkLabel},
			bigquery.QueryParameter{Name: "quantity" + s, Value: p.Quantity},
			bigquery.QueryParameter{Name: "mv" + s, Value: p.MarketValuePerUnit},
			bigquery.QueryParameter{Name: "alloc" + s, Value: allocated[i]},
			bigquery.QueryParameter{Name: "notes" + s, Value: p.Notes},
			bigquery.QueryParameter{Name: "created_at" + s, Value: createdAt},
		)
	}

	sql := fmt.Sprintf(`INSERT INTO `+"`%s.%s.box_break_pulls`"+`
		(pull_id, break_id, pull_type, product_id, bulk_game, bulk_set_code, bulk_label, quantity, market_value_per_unit, allocated_cost_basis_per_unit, notes, created_at)
		VALUES %s`,
		h.client.Project(), h.dataset, strings.Join(valueRows, ", "))
	q := h.client.Query(sql)
	q.Parameters = params

	job, err := q.Run(ctx)
	if err != nil {
		return err
	}
	if _, err := job.Wait(ctx); err != nil {
		return err
	}
	return nil
}

func (h *BoxBreakHandler) Update(c *gin.Context) {
	id := c.Param("id")
	var req UpdateBoxBreakRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	setClauses := []string{}
	params := []bigquery.QueryParameter{}

	if req.BreakDate != "" {
		setClauses = append(setClauses, "break_date = @break_date")
		params = append(params, bigquery.QueryParameter{Name: "break_date", Value: req.BreakDate})
	}
	if req.SealedMarketValue > 0 {
		setClauses = append(setClauses, "sealed_market_value = @sealed_market_value")
		params = append(params, bigquery.QueryParameter{Name: "sealed_market_value", Value: req.SealedMarketValue})
	}
	if req.BinderID != "" {
		setClauses = append(setClauses, "binder_id = @binder_id")
		params = append(params, bigquery.QueryParameter{Name: "binder_id", Value: req.BinderID})
	}
	if req.Notes != "" {
		setClauses = append(setClauses, "notes = @notes")
		params = append(params, bigquery.QueryParameter{Name: "notes", Value: req.Notes})
	}

	if len(setClauses) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no fields to update"})
		return
	}

	params = append(params, bigquery.QueryParameter{Name: "break_id", Value: id})
	sql := fmt.Sprintf("UPDATE `%s.%s.box_breaks` SET %s WHERE break_id = @break_id",
		h.client.Project(), h.dataset, strings.Join(setClauses, ", "))
	q := h.client.Query(sql)
	q.Parameters = params

	ctx := c.Request.Context()
	job, err := q.Run(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if _, err := job.Wait(ctx); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// If sealed_market_value changed, re-allocate cost basis on existing pulls.
	if req.SealedMarketValue > 0 {
		if err := h.reallocateFromDB(ctx, id, req.SealedMarketValue); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
	}

	c.Status(http.StatusNoContent)
	if h.triggerSync != nil {
		h.triggerSync()
	}
}

// reallocateFromDB recomputes allocated_cost_basis_per_unit for all pulls of a break,
// using the current pulls (read from BQ) and a new sealed_market_value.
func (h *BoxBreakHandler) reallocateFromDB(ctx context.Context, breakID string, sealedMarketValue float64) error {
	pulls, err := h.queryPulls(ctx, breakID)
	if err != nil {
		return err
	}
	total := 0.0
	for _, p := range pulls {
		total += float64(p.Quantity) * p.MarketValuePerUnit
	}
	if total <= 0 {
		return nil
	}
	ratio := sealedMarketValue / total
	// Update each pull row. Small N; a single CASE expression is doable but less readable.
	for _, p := range pulls {
		newAlloc := p.MarketValuePerUnit * ratio
		sql := fmt.Sprintf("UPDATE `%s.%s.box_break_pulls` SET allocated_cost_basis_per_unit = @alloc WHERE pull_id = @pull_id",
			h.client.Project(), h.dataset)
		q := h.client.Query(sql)
		q.Parameters = []bigquery.QueryParameter{
			{Name: "alloc", Value: newAlloc},
			{Name: "pull_id", Value: p.PullID},
		}
		job, err := q.Run(ctx)
		if err != nil {
			return err
		}
		if _, err := job.Wait(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (h *BoxBreakHandler) ReplacePulls(c *gin.Context) {
	id := c.Param("id")
	var req ReplacePullsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := validatePulls(req.Pulls); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx := c.Request.Context()

	// Fetch sealed_market_value from the break (needed for allocation).
	sealedSQL := fmt.Sprintf("SELECT sealed_market_value FROM `%s.%s.box_breaks` WHERE break_id = @break_id LIMIT 1",
		h.client.Project(), h.dataset)
	sq := h.client.Query(sealedSQL)
	sq.Parameters = []bigquery.QueryParameter{{Name: "break_id", Value: id}}
	sit, err := sq.Read(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	var row struct {
		SealedMarketValue float64 `bigquery:"sealed_market_value"`
	}
	if err := sit.Next(&row); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "box break not found"})
		return
	}

	// Delete existing pulls.
	delSQL := fmt.Sprintf("DELETE FROM `%s.%s.box_break_pulls` WHERE break_id = @break_id",
		h.client.Project(), h.dataset)
	dq := h.client.Query(delSQL)
	dq.Parameters = []bigquery.QueryParameter{{Name: "break_id", Value: id}}
	djob, err := dq.Run(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if _, err := djob.Wait(ctx); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Insert new pulls.
	if len(req.Pulls) > 0 {
		if err := h.insertPulls(ctx, id, req.Pulls, row.SealedMarketValue, time.Now().UTC()); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
	}

	c.Status(http.StatusNoContent)
	if h.triggerSync != nil {
		h.triggerSync()
	}
}

func (h *BoxBreakHandler) Delete(c *gin.Context) {
	id := c.Param("id")
	ctx := c.Request.Context()

	// Delete pulls first.
	delPullsSQL := fmt.Sprintf("DELETE FROM `%s.%s.box_break_pulls` WHERE break_id = @break_id",
		h.client.Project(), h.dataset)
	pq := h.client.Query(delPullsSQL)
	pq.Parameters = []bigquery.QueryParameter{{Name: "break_id", Value: id}}
	pjob, err := pq.Run(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if _, err := pjob.Wait(ctx); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Delete break.
	delSQL := fmt.Sprintf("DELETE FROM `%s.%s.box_breaks` WHERE break_id = @break_id",
		h.client.Project(), h.dataset)
	q := h.client.Query(delSQL)
	q.Parameters = []bigquery.QueryParameter{{Name: "break_id", Value: id}}
	job, err := q.Run(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if _, err := job.Wait(ctx); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.Status(http.StatusNoContent)
	if h.triggerSync != nil {
		h.triggerSync()
	}
}
