package handlers

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type DisplayHandler struct {
	client      *bigquery.Client
	dataset     string
	triggerSync func()
}

func NewDisplayHandler(client *bigquery.Client, dataset string, triggerSync func()) *DisplayHandler {
	return &DisplayHandler{client: client, dataset: dataset, triggerSync: triggerSync}
}

type ShowcaseDisplay struct {
	DisplayID   string    `json:"display_id" bigquery:"display_id"`
	Name        string    `json:"name" bigquery:"name"`
	DisplayType string    `json:"display_type" bigquery:"display_type"`
	Description string    `json:"description" bigquery:"description"`
	CreatedAt   time.Time `json:"created_at" bigquery:"created_at"`
}

type DisplayItem struct {
	DisplayID string    `json:"display_id" bigquery:"display_id"`
	Position  int64     `json:"position" bigquery:"position"`
	ProductID string    `json:"product_id" bigquery:"product_id"`
	Quantity  int64     `json:"quantity" bigquery:"quantity"`
	Notes     string    `json:"notes" bigquery:"notes"`
	UpdatedAt time.Time `json:"updated_at" bigquery:"updated_at"`
}

type CreateDisplayRequest struct {
	Name        string `json:"name" binding:"required"`
	DisplayType string `json:"display_type" binding:"required"`
	Description string `json:"description"`
}

type UpdateDisplayRequest struct {
	Name        string `json:"name"`
	DisplayType string `json:"display_type"`
	Description string `json:"description"`
}

type DisplayItemInput struct {
	Position  int64  `json:"position"`
	ProductID string `json:"product_id"`
	Quantity  int64  `json:"quantity"`
	Notes     string `json:"notes"`
}

type UpsertDisplayItemsRequest struct {
	Items []DisplayItemInput `json:"items" binding:"required"`
}

func (h *DisplayHandler) List(c *gin.Context) {
	sql := fmt.Sprintf("SELECT * FROM `%s.%s.showcase_displays` ORDER BY created_at",
		h.client.Project(), h.dataset)
	q := h.client.Query(sql)
	it, err := q.Read(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	var displays []ShowcaseDisplay
	for {
		var row ShowcaseDisplay
		if err := it.Next(&row); err != nil {
			break
		}
		displays = append(displays, row)
	}
	if displays == nil {
		displays = []ShowcaseDisplay{}
	}
	c.JSON(http.StatusOK, displays)
}

func (h *DisplayHandler) Get(c *gin.Context) {
	id := c.Param("id")

	sql := fmt.Sprintf("SELECT * FROM `%s.%s.showcase_displays` WHERE display_id = @display_id LIMIT 1",
		h.client.Project(), h.dataset)
	q := h.client.Query(sql)
	q.Parameters = []bigquery.QueryParameter{
		{Name: "display_id", Value: id},
	}
	it, err := q.Read(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	var display ShowcaseDisplay
	if err := it.Next(&display); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "display not found"})
		return
	}

	itemSQL := fmt.Sprintf("SELECT * FROM `%s.%s.showcase_display_items` WHERE display_id = @display_id ORDER BY position",
		h.client.Project(), h.dataset)
	iq := h.client.Query(itemSQL)
	iq.Parameters = []bigquery.QueryParameter{
		{Name: "display_id", Value: id},
	}
	iit, err := iq.Read(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	var items []DisplayItem
	for {
		var row DisplayItem
		if err := iit.Next(&row); err != nil {
			break
		}
		items = append(items, row)
	}
	if items == nil {
		items = []DisplayItem{}
	}

	c.JSON(http.StatusOK, gin.H{
		"display_id":   display.DisplayID,
		"name":         display.Name,
		"display_type": display.DisplayType,
		"description":  display.Description,
		"created_at":   display.CreatedAt,
		"items":        items,
	})
}

func (h *DisplayHandler) Create(c *gin.Context) {
	var req CreateDisplayRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if req.DisplayType != "bookshelf" && req.DisplayType != "bin" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "display_type must be 'bookshelf' or 'bin'"})
		return
	}

	id := uuid.New().String()
	createdAt := time.Now().UTC()

	sql := fmt.Sprintf(`INSERT INTO `+"`%s.%s.showcase_displays`"+`
		(display_id, name, display_type, description, created_at)
		VALUES (@display_id, @name, @display_type, @description, @created_at)`,
		h.client.Project(), h.dataset)
	q := h.client.Query(sql)
	q.Parameters = []bigquery.QueryParameter{
		{Name: "display_id", Value: id},
		{Name: "name", Value: req.Name},
		{Name: "display_type", Value: req.DisplayType},
		{Name: "description", Value: req.Description},
		{Name: "created_at", Value: createdAt},
	}

	job, err := q.Run(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if _, err := job.Wait(c.Request.Context()); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, gin.H{"display_id": id})
	if h.triggerSync != nil {
		h.triggerSync()
	}
}

func (h *DisplayHandler) Update(c *gin.Context) {
	id := c.Param("id")
	var req UpdateDisplayRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if req.DisplayType != "" && req.DisplayType != "bookshelf" && req.DisplayType != "bin" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "display_type must be 'bookshelf' or 'bin'"})
		return
	}

	setClauses := []string{}
	params := []bigquery.QueryParameter{}

	if req.Name != "" {
		setClauses = append(setClauses, "name = @name")
		params = append(params, bigquery.QueryParameter{Name: "name", Value: req.Name})
	}
	if req.DisplayType != "" {
		setClauses = append(setClauses, "display_type = @display_type")
		params = append(params, bigquery.QueryParameter{Name: "display_type", Value: req.DisplayType})
	}
	if req.Description != "" {
		setClauses = append(setClauses, "description = @description")
		params = append(params, bigquery.QueryParameter{Name: "description", Value: req.Description})
	}

	if len(setClauses) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no fields to update"})
		return
	}

	params = append(params, bigquery.QueryParameter{Name: "display_id", Value: id})
	sql := fmt.Sprintf("UPDATE `%s.%s.showcase_displays` SET %s WHERE display_id = @display_id",
		h.client.Project(), h.dataset, strings.Join(setClauses, ", "))
	q := h.client.Query(sql)
	q.Parameters = params

	job, err := q.Run(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if _, err := job.Wait(c.Request.Context()); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.Status(http.StatusNoContent)
	if h.triggerSync != nil {
		h.triggerSync()
	}
}

func (h *DisplayHandler) Delete(c *gin.Context) {
	id := c.Param("id")
	ctx := c.Request.Context()

	// Delete items first
	itemSQL := fmt.Sprintf("DELETE FROM `%s.%s.showcase_display_items` WHERE display_id = @display_id",
		h.client.Project(), h.dataset)
	iq := h.client.Query(itemSQL)
	iq.Parameters = []bigquery.QueryParameter{{Name: "display_id", Value: id}}
	job, err := iq.Run(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if _, err := job.Wait(ctx); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Delete display
	sql := fmt.Sprintf("DELETE FROM `%s.%s.showcase_displays` WHERE display_id = @display_id",
		h.client.Project(), h.dataset)
	q := h.client.Query(sql)
	q.Parameters = []bigquery.QueryParameter{{Name: "display_id", Value: id}}
	job2, err := q.Run(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if _, err := job2.Wait(ctx); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.Status(http.StatusNoContent)
	if h.triggerSync != nil {
		h.triggerSync()
	}
}

func (h *DisplayHandler) UpsertItems(c *gin.Context) {
	id := c.Param("id")
	var req UpsertDisplayItemsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx := c.Request.Context()

	// Delete existing items
	delSQL := fmt.Sprintf("DELETE FROM `%s.%s.showcase_display_items` WHERE display_id = @display_id",
		h.client.Project(), h.dataset)
	dq := h.client.Query(delSQL)
	dq.Parameters = []bigquery.QueryParameter{{Name: "display_id", Value: id}}
	job, err := dq.Run(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if _, err := job.Wait(ctx); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if len(req.Items) == 0 {
		c.Status(http.StatusNoContent)
		if h.triggerSync != nil {
			h.triggerSync()
		}
		return
	}

	updatedAt := time.Now().UTC()
	valueRows := make([]string, len(req.Items))
	params := []bigquery.QueryParameter{}

	for i, item := range req.Items {
		qty := item.Quantity
		if qty == 0 {
			qty = 1
		}
		s := fmt.Sprintf("_%d", i)
		valueRows[i] = fmt.Sprintf(
			"(@display_id%s, @position%s, @product_id%s, @quantity%s, @notes%s, @updated_at%s)",
			s, s, s, s, s, s,
		)
		params = append(params,
			bigquery.QueryParameter{Name: "display_id" + s, Value: id},
			bigquery.QueryParameter{Name: "position" + s, Value: item.Position},
			bigquery.QueryParameter{Name: "product_id" + s, Value: item.ProductID},
			bigquery.QueryParameter{Name: "quantity" + s, Value: qty},
			bigquery.QueryParameter{Name: "notes" + s, Value: item.Notes},
			bigquery.QueryParameter{Name: "updated_at" + s, Value: updatedAt},
		)
	}

	insertSQL := fmt.Sprintf(`INSERT INTO `+"`%s.%s.showcase_display_items`"+`
		(display_id, position, product_id, quantity, notes, updated_at)
		VALUES %s`,
		h.client.Project(), h.dataset, strings.Join(valueRows, ", "))
	iq := h.client.Query(insertSQL)
	iq.Parameters = params

	job2, err := iq.Run(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if _, err := job2.Wait(ctx); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.Status(http.StatusNoContent)
	if h.triggerSync != nil {
		h.triggerSync()
	}
}
