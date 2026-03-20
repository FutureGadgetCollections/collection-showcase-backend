package handlers

import (
	"fmt"
	"strconv"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type ProductHandler struct {
	client      *bigquery.Client
	dataset     string
	triggerSync func()
}

func NewProductHandler(client *bigquery.Client, dataset string, triggerSync func()) *ProductHandler {
	return &ProductHandler{client: client, dataset: dataset, triggerSync: triggerSync}
}

type Product struct {
	ProductID        string    `json:"product_id" bigquery:"product_id"`
	Name             string    `json:"name" bigquery:"name"`
	GameCategory     string    `json:"game_category" bigquery:"game_category"`
	GameSubcategory  string    `json:"game_subcategory" bigquery:"game_subcategory"`
	ProductCategory  string    `json:"product_category" bigquery:"product_category"`
	TcgplayerID      string    `json:"tcgplayer_id" bigquery:"tcgplayer_id"`
	PricechartingURL string    `json:"pricecharting_url" bigquery:"pricecharting_url"`
	ListingURL       string    `json:"listing_url" bigquery:"listing_url"`
	CreatedAt        time.Time `json:"created_at" bigquery:"created_at"`
}

type CreateProductRequest struct {
	Name             string `json:"name" binding:"required"`
	GameCategory     string `json:"game_category"`
	GameSubcategory  string `json:"game_subcategory"`
	ProductCategory  string `json:"product_category"`
	TcgplayerID      string `json:"tcgplayer_id"`
	PricechartingURL string `json:"pricecharting_url"`
	ListingURL       string `json:"listing_url"`
}

type UpdateProductRequest struct {
	Name             string `json:"name,omitempty"`
	GameCategory     string `json:"game_category,omitempty"`
	GameSubcategory  string `json:"game_subcategory,omitempty"`
	ProductCategory  string `json:"product_category,omitempty"`
	TcgplayerID      string `json:"tcgplayer_id,omitempty"`
	PricechartingURL string `json:"pricecharting_url,omitempty"`
	ListingURL       string `json:"listing_url,omitempty"`
}

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

	sql := fmt.Sprintf("SELECT * FROM `%s.%s.products` LIMIT %d OFFSET %d",
		h.client.Project(), h.dataset, limit, offset)
	q := h.client.Query(sql)
	it, err := q.Read(c.Request.Context())
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}

	var products []Product
	for {
		var row Product
		err := it.Next(&row)
		if err != nil {
			break
		}
		products = append(products, row)
	}
	if products == nil {
		products = []Product{}
	}
	c.JSON(200, products)
}

func (h *ProductHandler) Get(c *gin.Context) {
	id := c.Param("id")
	sql := fmt.Sprintf("SELECT * FROM `%s.%s.products` WHERE product_id = @product_id LIMIT 1",
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

	var row Product
	if err := it.Next(&row); err != nil {
		c.JSON(404, gin.H{"error": "product not found"})
		return
	}
	c.JSON(200, row)
}

func (h *ProductHandler) Create(c *gin.Context) {
	var req CreateProductRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	id := uuid.New().String()
	createdAt := time.Now().UTC()

	sql := fmt.Sprintf(`INSERT INTO `+"`%s.%s.products`"+`
		(product_id, name, game_category, game_subcategory, product_category, tcgplayer_id, pricecharting_url, listing_url, created_at)
		VALUES (@product_id, @name, @game_category, @game_subcategory, @product_category, @tcgplayer_id, @pricecharting_url, @listing_url, @created_at)`,
		h.client.Project(), h.dataset)
	q := h.client.Query(sql)
	q.Parameters = []bigquery.QueryParameter{
		{Name: "product_id", Value: id},
		{Name: "name", Value: req.Name},
		{Name: "game_category", Value: req.GameCategory},
		{Name: "game_subcategory", Value: req.GameSubcategory},
		{Name: "product_category", Value: req.ProductCategory},
		{Name: "tcgplayer_id", Value: req.TcgplayerID},
		{Name: "pricecharting_url", Value: req.PricechartingURL},
		{Name: "listing_url", Value: req.ListingURL},
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

	c.JSON(201, gin.H{"product_id": id})
	if h.triggerSync != nil {
		h.triggerSync()
	}
}

func (h *ProductHandler) Update(c *gin.Context) {
	id := c.Param("id")
	var req UpdateProductRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	setClauses := []string{}
	params := []bigquery.QueryParameter{}

	if req.Name != "" {
		setClauses = append(setClauses, "name = @name")
		params = append(params, bigquery.QueryParameter{Name: "name", Value: req.Name})
	}
	if req.GameCategory != "" {
		setClauses = append(setClauses, "game_category = @game_category")
		params = append(params, bigquery.QueryParameter{Name: "game_category", Value: req.GameCategory})
	}
	if req.GameSubcategory != "" {
		setClauses = append(setClauses, "game_subcategory = @game_subcategory")
		params = append(params, bigquery.QueryParameter{Name: "game_subcategory", Value: req.GameSubcategory})
	}
	if req.ProductCategory != "" {
		setClauses = append(setClauses, "product_category = @product_category")
		params = append(params, bigquery.QueryParameter{Name: "product_category", Value: req.ProductCategory})
	}
	if req.TcgplayerID != "" {
		setClauses = append(setClauses, "tcgplayer_id = @tcgplayer_id")
		params = append(params, bigquery.QueryParameter{Name: "tcgplayer_id", Value: req.TcgplayerID})
	}
	if req.PricechartingURL != "" {
		setClauses = append(setClauses, "pricecharting_url = @pricecharting_url")
		params = append(params, bigquery.QueryParameter{Name: "pricecharting_url", Value: req.PricechartingURL})
	}
	if req.ListingURL != "" {
		setClauses = append(setClauses, "listing_url = @listing_url")
		params = append(params, bigquery.QueryParameter{Name: "listing_url", Value: req.ListingURL})
	}

	if len(setClauses) == 0 {
		c.JSON(400, gin.H{"error": "no fields to update"})
		return
	}

	setStr := ""
	for i, s := range setClauses {
		if i > 0 {
			setStr += ", "
		}
		setStr += s
	}

	params = append(params, bigquery.QueryParameter{Name: "product_id", Value: id})
	sql := fmt.Sprintf("UPDATE `%s.%s.products` SET %s WHERE product_id = @product_id",
		h.client.Project(), h.dataset, setStr)
	q := h.client.Query(sql)
	q.Parameters = params

	job, err := q.Run(c.Request.Context())
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	if _, err := job.Wait(c.Request.Context()); err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}

	c.Status(200)
	if h.triggerSync != nil {
		h.triggerSync()
	}
}

func (h *ProductHandler) Delete(c *gin.Context) {
	id := c.Param("id")
	ctx := c.Request.Context()

	queries := []string{
		fmt.Sprintf("DELETE FROM `%s.market_data.price_history` WHERE product_id = @product_id", h.client.Project()),
		fmt.Sprintf("DELETE FROM `%s.%s.transactions` WHERE product_id = @product_id", h.client.Project(), h.dataset),
		fmt.Sprintf("DELETE FROM `%s.%s.products` WHERE product_id = @product_id", h.client.Project(), h.dataset),
	}

	for _, sql := range queries {
		q := h.client.Query(sql)
		q.Parameters = []bigquery.QueryParameter{
			{Name: "product_id", Value: id},
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
	}

	c.Status(204)
	if h.triggerSync != nil {
		h.triggerSync()
	}
}
