package handlers

import (
	"fmt"
	"strconv"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/gin-gonic/gin"
)

// ProductHandler provides read-only access to products via the
// inventory.catalog_products view (backed by catalog.sealed_products
// and catalog.single_cards in the market tracker).
type ProductHandler struct {
	client      *bigquery.Client
	dataset     string
	triggerSync func()
}

func NewProductHandler(client *bigquery.Client, dataset string, triggerSync func()) *ProductHandler {
	return &ProductHandler{client: client, dataset: dataset, triggerSync: triggerSync}
}

// CatalogProduct is the API/JSON response type for a product from catalog_products.
type CatalogProduct struct {
	ProductID       string `json:"product_id" bigquery:"product_id"`
	ProductType     string `json:"product_type" bigquery:"product_type"`
	ProductSubtype  string `json:"product_subtype" bigquery:"product_subtype"`
	Name            string `json:"name" bigquery:"name"`
	Game            string `json:"game" bigquery:"game"`
	Era             string `json:"era" bigquery:"era"`
	SetCode         string `json:"set_code" bigquery:"set_code"`
	TcgplayerID     string `json:"tcgplayer_id" bigquery:"tcgplayer_id"`
	PricechartingURL string `json:"pricecharting_url" bigquery:"pricecharting_url"`
	ImageURL        string `json:"image_url" bigquery:"image_url"`
}

// List returns products from inventory.catalog_products.
// Query params: type (sealed|card), game, limit, offset.
func (h *ProductHandler) List(c *gin.Context) {
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

	var clauses []string
	params := []bigquery.QueryParameter{}
	if t := c.Query("type"); t != "" {
		clauses = append(clauses, "product_type = @product_type")
		params = append(params, bigquery.QueryParameter{Name: "product_type", Value: t})
	}
	if g := c.Query("game"); g != "" {
		clauses = append(clauses, "game = @game")
		params = append(params, bigquery.QueryParameter{Name: "game", Value: g})
	}
	if s := c.Query("set_code"); s != "" {
		clauses = append(clauses, "set_code = @set_code")
		params = append(params, bigquery.QueryParameter{Name: "set_code", Value: s})
	}
	where := ""
	if len(clauses) > 0 {
		where = " WHERE " + strings.Join(clauses, " AND ")
	}

	sql := fmt.Sprintf("SELECT * FROM `%s.%s.catalog_products`%s ORDER BY game, set_code, product_id LIMIT %d OFFSET %d",
		h.client.Project(), h.dataset, where, limit, offset)
	q := h.client.Query(sql)
	q.Parameters = params
	it, err := q.Read(c.Request.Context())
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}

	var products []CatalogProduct
	for {
		var row CatalogProduct
		if err := it.Next(&row); err != nil {
			break
		}
		products = append(products, row)
	}
	if products == nil {
		products = []CatalogProduct{}
	}
	c.JSON(200, products)
}

// Get returns a single product by composite product_id.
func (h *ProductHandler) Get(c *gin.Context) {
	id := c.Param("id")
	sql := fmt.Sprintf("SELECT * FROM `%s.%s.catalog_products` WHERE product_id = @product_id LIMIT 1",
		h.client.Project(), h.dataset)
	q := h.client.Query(sql)
	q.Parameters = []bigquery.QueryParameter{
		{Name: "product_id", Value: id},
	}
	it, err := q.Read(c.Request.Context())
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}

	var row CatalogProduct
	if err := it.Next(&row); err != nil {
		c.JSON(404, gin.H{"error": "product not found"})
		return
	}
	c.JSON(200, row)
}
