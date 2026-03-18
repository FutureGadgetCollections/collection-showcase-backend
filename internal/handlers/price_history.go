package handlers

import (
	"fmt"
	"strconv"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type PriceHistoryHandler struct {
	client  *bigquery.Client
	dataset string
}

func NewPriceHistoryHandler(client *bigquery.Client, dataset string) *PriceHistoryHandler {
	return &PriceHistoryHandler{client: client, dataset: dataset}
}

type PriceHistory struct {
	RecordID           string    `json:"record_id" bigquery:"record_id"`
	ProductID          string    `json:"product_id" bigquery:"product_id"`
	SnapshotDate       string    `json:"snapshot_date" bigquery:"snapshot_date"`
	Source             string    `json:"source" bigquery:"source"`
	MarketPrice        float64   `json:"market_price" bigquery:"market_price"`
	MedianPrice        float64   `json:"median_price" bigquery:"median_price"`
	SellThroughRate    float64   `json:"sell_through_rate" bigquery:"sell_through_rate"`
	DistinctBuyerCount int64     `json:"distinct_buyer_count" bigquery:"distinct_buyer_count"`
	ListedCount        int64     `json:"listed_count" bigquery:"listed_count"`
	CreatedAt          time.Time `json:"created_at" bigquery:"created_at"`
}

type CreatePriceHistoryRequest struct {
	ProductID          string  `json:"product_id" binding:"required"`
	SnapshotDate       string  `json:"snapshot_date" binding:"required"`
	Source             string  `json:"source" binding:"required"`
	MarketPrice        float64 `json:"market_price"`
	MedianPrice        float64 `json:"median_price"`
	SellThroughRate    float64 `json:"sell_through_rate"`
	DistinctBuyerCount int64   `json:"distinct_buyer_count"`
	ListedCount        int64   `json:"listed_count"`
}

func (h *PriceHistoryHandler) List(c *gin.Context) {
	limit := 1000
	offset := 0
	if l := c.Query("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil {
			limit = v
		}
	}
	if o := c.Query("offset"); o != "" {
		if v, err := strconv.Atoi(o); err == nil {
			offset = v
		}
	}

	sql := fmt.Sprintf("SELECT * FROM `%s.%s.price_history` WHERE 1=1",
		h.client.Project(), h.dataset)
	params := []bigquery.QueryParameter{}

	if pid := c.Query("product_id"); pid != "" {
		sql += " AND product_id = @product_id"
		params = append(params, bigquery.QueryParameter{Name: "product_id", Value: pid})
	}
	if src := c.Query("source"); src != "" {
		sql += " AND source = @source"
		params = append(params, bigquery.QueryParameter{Name: "source", Value: src})
	}

	sql += fmt.Sprintf(" LIMIT %d OFFSET %d", limit, offset)

	q := h.client.Query(sql)
	q.Parameters = params
	it, err := q.Read(c.Request.Context())
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}

	var records []PriceHistory
	for {
		var row PriceHistory
		if err := it.Next(&row); err != nil {
			break
		}
		records = append(records, row)
	}
	if records == nil {
		records = []PriceHistory{}
	}
	c.JSON(200, records)
}

func (h *PriceHistoryHandler) Create(c *gin.Context) {
	var req CreatePriceHistoryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	id := uuid.New().String()
	createdAt := time.Now().UTC()

	sql := fmt.Sprintf(`INSERT INTO `+"`%s.%s.price_history`"+`
		(record_id, product_id, snapshot_date, source, market_price, median_price, sell_through_rate, distinct_buyer_count, listed_count, created_at)
		VALUES (@record_id, @product_id, @snapshot_date, @source, @market_price, @median_price, @sell_through_rate, @distinct_buyer_count, @listed_count, @created_at)`,
		h.client.Project(), h.dataset)
	q := h.client.Query(sql)
	q.Parameters = []bigquery.QueryParameter{
		{Name: "record_id", Value: id},
		{Name: "product_id", Value: req.ProductID},
		{Name: "snapshot_date", Value: req.SnapshotDate},
		{Name: "source", Value: req.Source},
		{Name: "market_price", Value: fmt.Sprintf("%f", req.MarketPrice)},
		{Name: "median_price", Value: fmt.Sprintf("%f", req.MedianPrice)},
		{Name: "sell_through_rate", Value: req.SellThroughRate},
		{Name: "distinct_buyer_count", Value: req.DistinctBuyerCount},
		{Name: "listed_count", Value: req.ListedCount},
		{Name: "created_at", Value: createdAt},
	}

	job, err := q.Run(c.Request.Context())
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	if _, err := job.Wait(c.Request.Context()); err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}

	c.JSON(201, gin.H{"record_id": id})
}

func (h *PriceHistoryHandler) Delete(c *gin.Context) {
	id := c.Param("record_id")
	ctx := c.Request.Context()

	checkSQL := fmt.Sprintf("SELECT record_id FROM `%s.%s.price_history` WHERE record_id = @record_id LIMIT 1",
		h.client.Project(), h.dataset)
	checkQ := h.client.Query(checkSQL)
	checkQ.Parameters = []bigquery.QueryParameter{
		{Name: "record_id", Value: id},
	}
	it, err := checkQ.Read(ctx)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	var tmp struct {
		RecordID string `bigquery:"record_id"`
	}
	if err := it.Next(&tmp); err != nil {
		c.JSON(404, gin.H{"error": "price history record not found"})
		return
	}

	sql := fmt.Sprintf("DELETE FROM `%s.%s.price_history` WHERE record_id = @record_id",
		h.client.Project(), h.dataset)
	q := h.client.Query(sql)
	q.Parameters = []bigquery.QueryParameter{
		{Name: "record_id", Value: id},
	}

	job, err := q.Run(ctx)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	if _, err := job.Wait(ctx); err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}

	c.Status(204)
}
