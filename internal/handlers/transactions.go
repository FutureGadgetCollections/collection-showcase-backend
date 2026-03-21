package handlers

import (
	"fmt"
	"strconv"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type TransactionHandler struct {
	client      *bigquery.Client
	dataset     string
	triggerSync func()
}

func NewTransactionHandler(client *bigquery.Client, dataset string, triggerSync func()) *TransactionHandler {
	return &TransactionHandler{client: client, dataset: dataset, triggerSync: triggerSync}
}

type Transaction struct {
	TransactionID   string    `json:"transaction_id" bigquery:"transaction_id"`
	ProductID       string    `json:"product_id" bigquery:"product_id"`
	TransactionDate string    `json:"transaction_date" bigquery:"transaction_date"`
	Price           float64   `json:"price" bigquery:"price"`
	Quantity        int64     `json:"quantity" bigquery:"quantity"`
	TransactionType string    `json:"transaction_type" bigquery:"transaction_type"`
	Platform        string    `json:"platform" bigquery:"platform"`
	Notes           string    `json:"notes" bigquery:"notes"`
	CreatedAt       time.Time `json:"created_at" bigquery:"created_at"`
}

type CreateTransactionRequest struct {
	ProductID       string  `json:"product_id" binding:"required"`
	TransactionDate string  `json:"transaction_date" binding:"required"`
	Price           float64 `json:"price" binding:"required"`
	Quantity        int64   `json:"quantity" binding:"required"`
	TransactionType string  `json:"transaction_type" binding:"required"`
	Platform        string  `json:"platform"`
	Notes           string  `json:"notes"`
}

type UpdateTransactionRequest struct {
	ProductID       string  `json:"product_id,omitempty"`
	TransactionDate string  `json:"transaction_date,omitempty"`
	Price           float64 `json:"price,omitempty"`
	Quantity        int64   `json:"quantity,omitempty"`
	TransactionType string  `json:"transaction_type,omitempty"`
	Platform        string  `json:"platform,omitempty"`
	Notes           string  `json:"notes,omitempty"`
}

func (h *TransactionHandler) List(c *gin.Context) {
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

	sql := fmt.Sprintf("SELECT * FROM `%s.%s.transactions` LIMIT %d OFFSET %d",
		h.client.Project(), h.dataset, limit, offset)
	q := h.client.Query(sql)
	it, err := q.Read(c.Request.Context())
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}

	var transactions []Transaction
	for {
		var row Transaction
		if err := it.Next(&row); err != nil {
			break
		}
		transactions = append(transactions, row)
	}
	if transactions == nil {
		transactions = []Transaction{}
	}
	c.JSON(200, transactions)
}

func (h *TransactionHandler) Get(c *gin.Context) {
	id := c.Param("id")
	sql := fmt.Sprintf("SELECT * FROM `%s.%s.transactions` WHERE transaction_id = @transaction_id LIMIT 1",
		h.client.Project(), h.dataset)
	q := h.client.Query(sql)
	q.Parameters = []bigquery.QueryParameter{
		{Name: "transaction_id", Value: id},
	}
	it, err := q.Read(c.Request.Context())
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}

	var row Transaction
	if err := it.Next(&row); err != nil {
		c.JSON(404, gin.H{"error": "transaction not found"})
		return
	}
	c.JSON(200, row)
}

func (h *TransactionHandler) Create(c *gin.Context) {
	var req CreateTransactionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	if req.TransactionType != "buy" && req.TransactionType != "sell" {
		c.JSON(400, gin.H{"error": "transaction_type must be 'buy' or 'sell'"})
		return
	}
	if req.Price <= 0 {
		c.JSON(400, gin.H{"error": "price must be greater than 0"})
		return
	}
	if req.Quantity <= 0 {
		c.JSON(400, gin.H{"error": "quantity must be greater than 0"})
		return
	}

	id := uuid.New().String()
	createdAt := time.Now().UTC()

	sql := fmt.Sprintf(`INSERT INTO `+"`%s.%s.transactions`"+`
		(transaction_id, product_id, transaction_date, price, quantity, transaction_type, platform, notes, created_at)
		VALUES (@transaction_id, @product_id, @transaction_date, @price, @quantity, @transaction_type, @platform, @notes, @created_at)`,
		h.client.Project(), h.dataset)
	q := h.client.Query(sql)
	q.Parameters = []bigquery.QueryParameter{
		{Name: "transaction_id", Value: id},
		{Name: "product_id", Value: req.ProductID},
		{Name: "transaction_date", Value: req.TransactionDate},
		{Name: "price", Value: req.Price},
		{Name: "quantity", Value: req.Quantity},
		{Name: "transaction_type", Value: req.TransactionType},
		{Name: "platform", Value: req.Platform},
		{Name: "notes", Value: req.Notes},
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

	c.JSON(201, gin.H{"transaction_id": id})
	if h.triggerSync != nil {
		h.triggerSync()
	}
}

func (h *TransactionHandler) Update(c *gin.Context) {
	id := c.Param("id")
	var req UpdateTransactionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	if req.TransactionType != "" && req.TransactionType != "buy" && req.TransactionType != "sell" {
		c.JSON(400, gin.H{"error": "transaction_type must be 'buy' or 'sell'"})
		return
	}
	if req.Price < 0 {
		c.JSON(400, gin.H{"error": "price must be greater than 0"})
		return
	}
	if req.Quantity < 0 {
		c.JSON(400, gin.H{"error": "quantity must be greater than 0"})
		return
	}

	setClauses := []string{}
	params := []bigquery.QueryParameter{}

	if req.ProductID != "" {
		setClauses = append(setClauses, "product_id = @product_id_val")
		params = append(params, bigquery.QueryParameter{Name: "product_id_val", Value: req.ProductID})
	}
	if req.TransactionDate != "" {
		setClauses = append(setClauses, "transaction_date = @transaction_date")
		params = append(params, bigquery.QueryParameter{Name: "transaction_date", Value: req.TransactionDate})
	}
	if req.Price != 0 {
		setClauses = append(setClauses, "price = @price")
		params = append(params, bigquery.QueryParameter{Name: "price", Value: req.Price})
	}
	if req.Quantity != 0 {
		setClauses = append(setClauses, "quantity = @quantity")
		params = append(params, bigquery.QueryParameter{Name: "quantity", Value: req.Quantity})
	}
	if req.TransactionType != "" {
		setClauses = append(setClauses, "transaction_type = @transaction_type")
		params = append(params, bigquery.QueryParameter{Name: "transaction_type", Value: req.TransactionType})
	}
	if req.Platform != "" {
		setClauses = append(setClauses, "platform = @platform")
		params = append(params, bigquery.QueryParameter{Name: "platform", Value: req.Platform})
	}
	if req.Notes != "" {
		setClauses = append(setClauses, "notes = @notes")
		params = append(params, bigquery.QueryParameter{Name: "notes", Value: req.Notes})
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

	params = append(params, bigquery.QueryParameter{Name: "transaction_id", Value: id})
	sql := fmt.Sprintf("UPDATE `%s.%s.transactions` SET %s WHERE transaction_id = @transaction_id",
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

func (h *TransactionHandler) Delete(c *gin.Context) {
	id := c.Param("id")
	sql := fmt.Sprintf("DELETE FROM `%s.%s.transactions` WHERE transaction_id = @transaction_id",
		h.client.Project(), h.dataset)
	q := h.client.Query(sql)
	q.Parameters = []bigquery.QueryParameter{
		{Name: "transaction_id", Value: id},
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

	c.Status(204)
	if h.triggerSync != nil {
		h.triggerSync()
	}
}
