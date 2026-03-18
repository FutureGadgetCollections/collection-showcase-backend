package handlers

import (
	"fmt"

	"cloud.google.com/go/bigquery"
	"github.com/gin-gonic/gin"
)

type CollectionHandler struct {
	client  *bigquery.Client
	dataset string
}

func NewCollectionHandler(client *bigquery.Client, dataset string) *CollectionHandler {
	return &CollectionHandler{client: client, dataset: dataset}
}

type CollectionItem struct {
	ProductID     string  `json:"product_id" bigquery:"product_id"`
	Quantity      int64   `json:"quantity" bigquery:"quantity"`
	AvgUnitCost   float64 `json:"avg_unit_cost" bigquery:"avg_unit_cost"`
	TotalInvested float64 `json:"total_invested" bigquery:"total_invested"`
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
