package handlers

import (
	"encoding/json"
	"fmt"
	"math/big"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/gin-gonic/gin"
)

type CollectionHandler struct {
	client  *bigquery.Client
	dataset string
}

func NewCollectionHandler(client *bigquery.Client, dataset string) *CollectionHandler {
	return &CollectionHandler{client: client, dataset: dataset}
}

func ratToFloat(r *big.Rat) float64 {
	if r == nil {
		return 0
	}
	f, _ := r.Float64()
	return f
}

func ratToFloatPtr(r *big.Rat) *float64 {
	if r == nil {
		return nil
	}
	f, _ := r.Float64()
	return &f
}

type CollectionItem struct {
	ProductID         string     `json:"product_id" bigquery:"product_id"`
	Quantity          int64      `json:"quantity" bigquery:"quantity"`
	AvgUnitCost       *big.Rat   `json:"-" bigquery:"avg_unit_cost"`
	TotalInvested     *big.Rat   `json:"-" bigquery:"total_invested"`
	RealizedGain      *big.Rat   `json:"-" bigquery:"realized_gain"`
	UnrealizedGain    *big.Rat   `json:"-" bigquery:"unrealized_gain"`
	LatestMarketPrice *big.Rat   `json:"-" bigquery:"latest_market_price"`
	FirstBuyDate      civil.Date `json:"first_buy_date" bigquery:"first_buy_date"`
	DaysHeld          int64      `json:"days_held" bigquery:"days_held"`
	ROI               *big.Rat   `json:"-" bigquery:"roi"`
	AnnualizedROI     *big.Rat   `json:"-" bigquery:"annualized_roi"`
}

func (item CollectionItem) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		ProductID         string     `json:"product_id"`
		Quantity          int64      `json:"quantity"`
		AvgUnitCost       float64    `json:"avg_unit_cost"`
		TotalInvested     float64    `json:"total_invested"`
		RealizedGain      float64    `json:"realized_gain"`
		UnrealizedGain    *float64   `json:"unrealized_gain"`
		LatestMarketPrice *float64   `json:"latest_market_price"`
		FirstBuyDate      civil.Date `json:"first_buy_date"`
		DaysHeld          int64      `json:"days_held"`
		ROI               *float64   `json:"roi"`
		AnnualizedROI     *float64   `json:"annualized_roi"`
	}{
		item.ProductID, item.Quantity,
		ratToFloat(item.AvgUnitCost), ratToFloat(item.TotalInvested), ratToFloat(item.RealizedGain),
		ratToFloatPtr(item.UnrealizedGain), ratToFloatPtr(item.LatestMarketPrice),
		item.FirstBuyDate, item.DaysHeld,
		ratToFloatPtr(item.ROI), ratToFloatPtr(item.AnnualizedROI),
	})
}

func (h *CollectionHandler) List(c *gin.Context) {
	sql := fmt.Sprintf("SELECT * FROM `%s.%s.collection`", h.client.Project(), h.dataset)
	q := h.client.Query(sql)
	it, err := q.Read(c.Request.Context())
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}

	var items []CollectionItem
	for {
		var row CollectionItem
		if err := it.Next(&row); err != nil {
			break
		}
		items = append(items, row)
	}
	if items == nil {
		items = []CollectionItem{}
	}
	c.JSON(200, items)
}

func (h *CollectionHandler) Get(c *gin.Context) {
	productID := c.Param("product_id")
	sql := fmt.Sprintf("SELECT * FROM `%s.%s.collection` WHERE product_id = @product_id LIMIT 1",
		h.client.Project(), h.dataset)
	q := h.client.Query(sql)
	q.Parameters = []bigquery.QueryParameter{
		{Name: "product_id", Value: productID},
	}
	it, err := q.Read(c.Request.Context())
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}

	var row CollectionItem
	if err := it.Next(&row); err != nil {
		c.JSON(404, gin.H{"error": "collection item not found"})
		return
	}
	c.JSON(200, row)
}
