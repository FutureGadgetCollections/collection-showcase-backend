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
	UnitPrice       float64   `json:"unit_price" bigquery:"unit_price"`
	Quantity        int64     `json:"quantity" bigquery:"quantity"`
	TransactionType string    `json:"transaction_type" bigquery:"transaction_type"`
	Platform        string    `json:"platform" bigquery:"platform"`
	Notes           string    `json:"notes" bigquery:"notes"`
	CreatedAt       time.Time `json:"created_at" bigquery:"created_at"`
}

type CreateTransactionRequest struct {
	ProductID       string  `json:"product_id" binding:"required"`
	TransactionDate string  `json:"transaction_date" binding:"required"`
	UnitPrice       float64 `json:"unit_price" binding:"required"`
	Quantity        int64   `json:"quantity" binding:"required"`
	TransactionType string  `json:"transaction_type" binding:"required"`
	Platform        string  `json:"platform"`
	Notes           string  `json:"notes"`
}

type UpdateTransactionRequest struct {
	ProductID       string  `json:"product_id,omitempty"`
	TransactionDate string  `json:"transaction_date,omitempty"`
	UnitPrice       float64 `json:"unit_price,omitempty"`
	Quantity        int64   `json:"quantity,omitempty"`
	TransactionType string  `json:"transaction_type,omitempty"`
	Platform        string  `json:"platform,omitempty"`
	Notes           string  `json:"notes,omitempty"`
}

type BulkCreateTransactionsRequest struct {
	Transactions []CreateTransactionRequest `json:"transactions" binding:"required,min=1"`
}

type BulkUpdateTransactionItem struct {
	TransactionID   string  `json:"transaction_id" binding:"required"`
	ProductID       string  `json:"product_id,omitempty"`
	TransactionDate string  `json:"transaction_date,omitempty"`
	UnitPrice       float64 `json:"unit_price,omitempty"`
	Quantity        int64   `json:"quantity,omitempty"`
	TransactionType string  `json:"transaction_type,omitempty"`
	Platform        string  `json:"platform,omitempty"`
	Notes           string  `json:"notes,omitempty"`
}

type BulkUpdateTransactionsRequest struct {
	Transactions []BulkUpdateTransactionItem `json:"transactions" binding:"required,min=1"`
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
	if req.UnitPrice <= 0 {
		c.JSON(400, gin.H{"error": "unit_price must be greater than 0"})
		return
	}
	if req.Quantity <= 0 {
		c.JSON(400, gin.H{"error": "quantity must be greater than 0"})
		return
	}

	id := uuid.New().String()
	createdAt := time.Now().UTC()

	sql := fmt.Sprintf(`INSERT INTO `+"`%s.%s.transactions`"+`
		(transaction_id, product_id, transaction_date, unit_price, quantity, transaction_type, platform, notes, created_at)
		VALUES (@transaction_id, @product_id, @transaction_date, @unit_price, @quantity, @transaction_type, @platform, @notes, @created_at)`,
		h.client.Project(), h.dataset)
	q := h.client.Query(sql)
	q.Parameters = []bigquery.QueryParameter{
		{Name: "transaction_id", Value: id},
		{Name: "product_id", Value: req.ProductID},
		{Name: "transaction_date", Value: req.TransactionDate},
		{Name: "unit_price", Value: req.UnitPrice},
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
	if req.UnitPrice < 0 {
		c.JSON(400, gin.H{"error": "unit_price must be greater than 0"})
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
	if req.UnitPrice != 0 {
		setClauses = append(setClauses, "unit_price = @unit_price")
		params = append(params, bigquery.QueryParameter{Name: "unit_price", Value: req.UnitPrice})
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

func (h *TransactionHandler) BulkCreate(c *gin.Context) {
	var req BulkCreateTransactionsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	for i, item := range req.Transactions {
		if item.TransactionType != "buy" && item.TransactionType != "sell" {
			c.JSON(400, gin.H{"error": fmt.Sprintf("transactions[%d]: transaction_type must be 'buy' or 'sell'", i)})
			return
		}
		if item.UnitPrice <= 0 {
			c.JSON(400, gin.H{"error": fmt.Sprintf("transactions[%d]: unit_price must be greater than 0", i)})
			return
		}
		if item.Quantity <= 0 {
			c.JSON(400, gin.H{"error": fmt.Sprintf("transactions[%d]: quantity must be greater than 0", i)})
			return
		}
	}

	ids := make([]string, len(req.Transactions))
	createdAt := time.Now().UTC()
	valueRows := make([]string, len(req.Transactions))
	params := []bigquery.QueryParameter{}

	for i, item := range req.Transactions {
		ids[i] = uuid.New().String()
		s := fmt.Sprintf("_%d", i)
		valueRows[i] = fmt.Sprintf(
			"(@transaction_id%s, @product_id%s, @transaction_date%s, @unit_price%s, @quantity%s, @transaction_type%s, @platform%s, @notes%s, @created_at%s)",
			s, s, s, s, s, s, s, s, s,
		)
		params = append(params,
			bigquery.QueryParameter{Name: "transaction_id" + s, Value: ids[i]},
			bigquery.QueryParameter{Name: "product_id" + s, Value: item.ProductID},
			bigquery.QueryParameter{Name: "transaction_date" + s, Value: item.TransactionDate},
			bigquery.QueryParameter{Name: "unit_price" + s, Value: item.UnitPrice},
			bigquery.QueryParameter{Name: "quantity" + s, Value: item.Quantity},
			bigquery.QueryParameter{Name: "transaction_type" + s, Value: item.TransactionType},
			bigquery.QueryParameter{Name: "platform" + s, Value: item.Platform},
			bigquery.QueryParameter{Name: "notes" + s, Value: item.Notes},
			bigquery.QueryParameter{Name: "created_at" + s, Value: createdAt},
		)
	}

	sql := fmt.Sprintf(
		`INSERT INTO `+"`%s.%s.transactions`"+`
		(transaction_id, product_id, transaction_date, unit_price, quantity, transaction_type, platform, notes, created_at)
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

	c.JSON(201, gin.H{"transaction_ids": ids})
	if h.triggerSync != nil {
		h.triggerSync()
	}
}

func (h *TransactionHandler) BulkUpdate(c *gin.Context) {
	var req BulkUpdateTransactionsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	for i, item := range req.Transactions {
		if item.TransactionType != "" && item.TransactionType != "buy" && item.TransactionType != "sell" {
			c.JSON(400, gin.H{"error": fmt.Sprintf("transactions[%d]: transaction_type must be 'buy' or 'sell'", i)})
			return
		}
		if item.UnitPrice < 0 {
			c.JSON(400, gin.H{"error": fmt.Sprintf("transactions[%d]: unit_price must be greater than 0", i)})
			return
		}
		if item.Quantity < 0 {
			c.JSON(400, gin.H{"error": fmt.Sprintf("transactions[%d]: quantity must be greater than 0", i)})
			return
		}
		if item.ProductID == "" && item.TransactionDate == "" && item.UnitPrice == 0 && item.Quantity == 0 && item.TransactionType == "" && item.Platform == "" && item.Notes == "" {
			c.JSON(400, gin.H{"error": fmt.Sprintf("transactions[%d]: no fields to update", i)})
			return
		}
	}

	ctx := c.Request.Context()
	for _, item := range req.Transactions {
		setClauses := []string{}
		params := []bigquery.QueryParameter{}

		if item.ProductID != "" {
			setClauses = append(setClauses, "product_id = @product_id_val")
			params = append(params, bigquery.QueryParameter{Name: "product_id_val", Value: item.ProductID})
		}
		if item.TransactionDate != "" {
			setClauses = append(setClauses, "transaction_date = @transaction_date")
			params = append(params, bigquery.QueryParameter{Name: "transaction_date", Value: item.TransactionDate})
		}
		if item.UnitPrice != 0 {
			setClauses = append(setClauses, "unit_price = @unit_price")
			params = append(params, bigquery.QueryParameter{Name: "unit_price", Value: item.UnitPrice})
		}
		if item.Quantity != 0 {
			setClauses = append(setClauses, "quantity = @quantity")
			params = append(params, bigquery.QueryParameter{Name: "quantity", Value: item.Quantity})
		}
		if item.TransactionType != "" {
			setClauses = append(setClauses, "transaction_type = @transaction_type")
			params = append(params, bigquery.QueryParameter{Name: "transaction_type", Value: item.TransactionType})
		}
		if item.Platform != "" {
			setClauses = append(setClauses, "platform = @platform")
			params = append(params, bigquery.QueryParameter{Name: "platform", Value: item.Platform})
		}
		if item.Notes != "" {
			setClauses = append(setClauses, "notes = @notes")
			params = append(params, bigquery.QueryParameter{Name: "notes", Value: item.Notes})
		}

		params = append(params, bigquery.QueryParameter{Name: "transaction_id", Value: item.TransactionID})
		sql := fmt.Sprintf("UPDATE `%s.%s.transactions` SET %s WHERE transaction_id = @transaction_id",
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
