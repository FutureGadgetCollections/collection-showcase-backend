package handlers

import (
	"fmt"
	"strconv"
	"strings"
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
	ImageURL         string    `json:"image_url" bigquery:"image_url"`
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
	ImageURL         string `json:"image_url"`
}

type UpdateProductRequest struct {
	Name             string `json:"name,omitempty"`
	GameCategory     string `json:"game_category,omitempty"`
	GameSubcategory  string `json:"game_subcategory,omitempty"`
	ProductCategory  string `json:"product_category,omitempty"`
	TcgplayerID      string `json:"tcgplayer_id,omitempty"`
	PricechartingURL string `json:"pricecharting_url,omitempty"`
	ListingURL       string `json:"listing_url,omitempty"`
	ImageURL         string `json:"image_url,omitempty"`
}

type BulkCreateProductsRequest struct {
	Products []CreateProductRequest `json:"products" binding:"required,min=1"`
}

type BulkUpdateProductItem struct {
	ProductID        string `json:"product_id" binding:"required"`
	Name             string `json:"name,omitempty"`
	GameCategory     string `json:"game_category,omitempty"`
	GameSubcategory  string `json:"game_subcategory,omitempty"`
	ProductCategory  string `json:"product_category,omitempty"`
	TcgplayerID      string `json:"tcgplayer_id,omitempty"`
	PricechartingURL string `json:"pricecharting_url,omitempty"`
	ListingURL       string `json:"listing_url,omitempty"`
	ImageURL         string `json:"image_url,omitempty"`
}

type BulkUpdateProductsRequest struct {
	Products []BulkUpdateProductItem `json:"products" binding:"required,min=1"`
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

	// Prevent duplicate products by tcgplayer_id
	if req.TcgplayerID != "" {
		checkSQL := fmt.Sprintf("SELECT product_id FROM `%s.%s.products` WHERE tcgplayer_id = @tcgplayer_id LIMIT 1",
			h.client.Project(), h.dataset)
		checkQ := h.client.Query(checkSQL)
		checkQ.Parameters = []bigquery.QueryParameter{
			{Name: "tcgplayer_id", Value: req.TcgplayerID},
		}
		it, err := checkQ.Read(c.Request.Context())
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		var existing Product
		if err := it.Next(&existing); err == nil {
			c.JSON(409, gin.H{"error": "product with this tcgplayer_id already exists", "product_id": existing.ProductID})
			return
		}
	}

	// Auto-populate image_url from tcgplayer_id if not provided
	imageURL := req.ImageURL
	if imageURL == "" && req.TcgplayerID != "" {
		imageURL = fmt.Sprintf("https://tcgplayer-cdn.tcgplayer.com/product/%s_in_1000x1000.jpg", req.TcgplayerID)
	}

	id := uuid.New().String()
	createdAt := time.Now().UTC()

	sql := fmt.Sprintf(`INSERT INTO `+"`%s.%s.products`"+`
		(product_id, name, game_category, game_subcategory, product_category, tcgplayer_id, pricecharting_url, listing_url, image_url, created_at)
		VALUES (@product_id, @name, @game_category, @game_subcategory, @product_category, @tcgplayer_id, @pricecharting_url, @listing_url, @image_url, @created_at)`,
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
		{Name: "image_url", Value: imageURL},
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
	if req.ImageURL != "" {
		setClauses = append(setClauses, "image_url = @image_url")
		params = append(params, bigquery.QueryParameter{Name: "image_url", Value: req.ImageURL})
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

	c.Status(204)
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

func (h *ProductHandler) BulkCreate(c *gin.Context) {
	var req BulkCreateProductsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	// Collect all tcgplayer_ids and check for duplicates in one query
	tcgIDs := []string{}
	for _, item := range req.Products {
		if item.TcgplayerID != "" {
			tcgIDs = append(tcgIDs, item.TcgplayerID)
		}
	}
	if len(tcgIDs) > 0 {
		placeholders := make([]string, len(tcgIDs))
		checkParams := make([]bigquery.QueryParameter, len(tcgIDs))
		for i, tid := range tcgIDs {
			name := fmt.Sprintf("tcg_%d", i)
			placeholders[i] = "@" + name
			checkParams[i] = bigquery.QueryParameter{Name: name, Value: tid}
		}
		checkSQL := fmt.Sprintf("SELECT tcgplayer_id FROM `%s.%s.products` WHERE tcgplayer_id IN (%s) LIMIT 1",
			h.client.Project(), h.dataset, strings.Join(placeholders, ", "))
		checkQ := h.client.Query(checkSQL)
		checkQ.Parameters = checkParams
		it, err := checkQ.Read(c.Request.Context())
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		var existing struct {
			TcgplayerID string `bigquery:"tcgplayer_id"`
		}
		if err := it.Next(&existing); err == nil {
			c.JSON(409, gin.H{"error": "one or more products with provided tcgplayer_id already exist", "tcgplayer_id": existing.TcgplayerID})
			return
		}
	}

	ids := make([]string, len(req.Products))
	createdAt := time.Now().UTC()
	valueRows := make([]string, len(req.Products))
	params := []bigquery.QueryParameter{}

	for i, item := range req.Products {
		ids[i] = uuid.New().String()
		imageURL := item.ImageURL
		if imageURL == "" && item.TcgplayerID != "" {
			imageURL = fmt.Sprintf("https://tcgplayer-cdn.tcgplayer.com/product/%s_in_1000x1000.jpg", item.TcgplayerID)
		}
		s := fmt.Sprintf("_%d", i)
		valueRows[i] = fmt.Sprintf(
			"(@product_id%s, @name%s, @game_category%s, @game_subcategory%s, @product_category%s, @tcgplayer_id%s, @pricecharting_url%s, @listing_url%s, @image_url%s, @created_at%s)",
			s, s, s, s, s, s, s, s, s, s,
		)
		params = append(params,
			bigquery.QueryParameter{Name: "product_id" + s, Value: ids[i]},
			bigquery.QueryParameter{Name: "name" + s, Value: item.Name},
			bigquery.QueryParameter{Name: "game_category" + s, Value: item.GameCategory},
			bigquery.QueryParameter{Name: "game_subcategory" + s, Value: item.GameSubcategory},
			bigquery.QueryParameter{Name: "product_category" + s, Value: item.ProductCategory},
			bigquery.QueryParameter{Name: "tcgplayer_id" + s, Value: item.TcgplayerID},
			bigquery.QueryParameter{Name: "pricecharting_url" + s, Value: item.PricechartingURL},
			bigquery.QueryParameter{Name: "listing_url" + s, Value: item.ListingURL},
			bigquery.QueryParameter{Name: "image_url" + s, Value: imageURL},
			bigquery.QueryParameter{Name: "created_at" + s, Value: createdAt},
		)
	}

	sql := fmt.Sprintf(
		`INSERT INTO `+"`%s.%s.products`"+`
		(product_id, name, game_category, game_subcategory, product_category, tcgplayer_id, pricecharting_url, listing_url, image_url, created_at)
		VALUES %s`,
		h.client.Project(), h.dataset, strings.Join(valueRows, ", "),
	)
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

	c.JSON(201, gin.H{"product_ids": ids})
	if h.triggerSync != nil {
		h.triggerSync()
	}
}

func (h *ProductHandler) BulkUpdate(c *gin.Context) {
	var req BulkUpdateProductsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	for i, item := range req.Products {
		if item.Name == "" && item.GameCategory == "" && item.GameSubcategory == "" && item.ProductCategory == "" && item.TcgplayerID == "" && item.PricechartingURL == "" && item.ListingURL == "" && item.ImageURL == "" {
			c.JSON(400, gin.H{"error": fmt.Sprintf("products[%d]: no fields to update", i)})
			return
		}
	}

	ctx := c.Request.Context()
	for _, item := range req.Products {
		setClauses := []string{}
		params := []bigquery.QueryParameter{}

		if item.Name != "" {
			setClauses = append(setClauses, "name = @name")
			params = append(params, bigquery.QueryParameter{Name: "name", Value: item.Name})
		}
		if item.GameCategory != "" {
			setClauses = append(setClauses, "game_category = @game_category")
			params = append(params, bigquery.QueryParameter{Name: "game_category", Value: item.GameCategory})
		}
		if item.GameSubcategory != "" {
			setClauses = append(setClauses, "game_subcategory = @game_subcategory")
			params = append(params, bigquery.QueryParameter{Name: "game_subcategory", Value: item.GameSubcategory})
		}
		if item.ProductCategory != "" {
			setClauses = append(setClauses, "product_category = @product_category")
			params = append(params, bigquery.QueryParameter{Name: "product_category", Value: item.ProductCategory})
		}
		if item.TcgplayerID != "" {
			setClauses = append(setClauses, "tcgplayer_id = @tcgplayer_id")
			params = append(params, bigquery.QueryParameter{Name: "tcgplayer_id", Value: item.TcgplayerID})
		}
		if item.PricechartingURL != "" {
			setClauses = append(setClauses, "pricecharting_url = @pricecharting_url")
			params = append(params, bigquery.QueryParameter{Name: "pricecharting_url", Value: item.PricechartingURL})
		}
		if item.ListingURL != "" {
			setClauses = append(setClauses, "listing_url = @listing_url")
			params = append(params, bigquery.QueryParameter{Name: "listing_url", Value: item.ListingURL})
		}
		if item.ImageURL != "" {
			setClauses = append(setClauses, "image_url = @image_url")
			params = append(params, bigquery.QueryParameter{Name: "image_url", Value: item.ImageURL})
		}

		params = append(params, bigquery.QueryParameter{Name: "product_id", Value: item.ProductID})
		sql := fmt.Sprintf("UPDATE `%s.%s.products` SET %s WHERE product_id = @product_id",
			h.client.Project(), h.dataset, strings.Join(setClauses, ", "))
		q := h.client.Query(sql)
		q.Parameters = params

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
