package handlers

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type BinderHandler struct {
	client      *bigquery.Client
	dataset     string
	catalogDS   string
	triggerSync func()
}

func NewBinderHandler(client *bigquery.Client, dataset, catalogDS string, triggerSync func()) *BinderHandler {
	return &BinderHandler{client: client, dataset: dataset, catalogDS: catalogDS, triggerSync: triggerSync}
}

// Row types

type Binder struct {
	BinderID     string    `json:"binder_id" bigquery:"binder_id"`
	Name         string    `json:"name" bigquery:"name"`
	Description  string    `json:"description" bigquery:"description"`
	SlotsPerPage int64     `json:"slots_per_page" bigquery:"slots_per_page"`
	CreatedAt    time.Time `json:"created_at" bigquery:"created_at"`
}

type BinderSlot struct {
	BinderID     string    `json:"binder_id" bigquery:"binder_id"`
	PageNumber   int64     `json:"page_number" bigquery:"page_number"`
	Side         string    `json:"side" bigquery:"side"`
	SlotPosition int64     `json:"slot_position" bigquery:"slot_position"`
	CardGame     string    `json:"card_game" bigquery:"card_game"`
	CardSetCode  string    `json:"card_set_code" bigquery:"card_set_code"`
	CardNumber   string    `json:"card_number" bigquery:"card_number"`
	CardNotes    string    `json:"card_notes" bigquery:"card_notes"`
	UpdatedAt    time.Time `json:"updated_at" bigquery:"updated_at"`
}

// Request types

type CreateBinderRequest struct {
	Name         string `json:"name" binding:"required"`
	Description  string `json:"description"`
	SlotsPerPage int64  `json:"slots_per_page"`
}

type UpdateBinderRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

type SlotInput struct {
	Side         string `json:"side"`
	SlotPosition int64  `json:"slot_position"`
	CardGame     string `json:"card_game"`
	CardSetCode  string `json:"card_set_code"`
	CardNumber   string `json:"card_number"`
	CardNotes    string `json:"card_notes"`
}

type UpsertPageSlotsRequest struct {
	Slots []SlotInput `json:"slots" binding:"required"`
}

func (h *BinderHandler) List(c *gin.Context) {
	sql := fmt.Sprintf("SELECT * FROM `%s.%s.binders` ORDER BY created_at",
		h.client.Project(), h.dataset)
	q := h.client.Query(sql)
	it, err := q.Read(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	var binders []Binder
	for {
		var row Binder
		if err := it.Next(&row); err != nil {
			break
		}
		binders = append(binders, row)
	}
	if binders == nil {
		binders = []Binder{}
	}
	c.JSON(http.StatusOK, binders)
}

func (h *BinderHandler) Get(c *gin.Context) {
	id := c.Param("id")
	sql := fmt.Sprintf("SELECT * FROM `%s.%s.binders` WHERE binder_id = @binder_id LIMIT 1",
		h.client.Project(), h.dataset)
	q := h.client.Query(sql)
	q.Parameters = []bigquery.QueryParameter{
		{Name: "binder_id", Value: id},
	}
	it, err := q.Read(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	var binder Binder
	if err := it.Next(&binder); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "binder not found"})
		return
	}

	// Also fetch slots
	slotSQL := fmt.Sprintf("SELECT * FROM `%s.%s.binder_slots` WHERE binder_id = @binder_id ORDER BY page_number, CASE WHEN side = 'front' THEN 0 ELSE 1 END, slot_position",
		h.client.Project(), h.dataset)
	sq := h.client.Query(slotSQL)
	sq.Parameters = []bigquery.QueryParameter{
		{Name: "binder_id", Value: id},
	}
	sit, err := sq.Read(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	var slots []BinderSlot
	for {
		var row BinderSlot
		if err := sit.Next(&row); err != nil {
			break
		}
		slots = append(slots, row)
	}
	if slots == nil {
		slots = []BinderSlot{}
	}

	c.JSON(http.StatusOK, gin.H{
		"binder_id":     binder.BinderID,
		"name":          binder.Name,
		"description":   binder.Description,
		"slots_per_page": binder.SlotsPerPage,
		"created_at":    binder.CreatedAt,
		"slots":         slots,
	})
}

func (h *BinderHandler) Create(c *gin.Context) {
	var req CreateBinderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if req.SlotsPerPage == 0 {
		req.SlotsPerPage = 9
	}

	id := uuid.New().String()
	createdAt := time.Now().UTC()

	sql := fmt.Sprintf(`INSERT INTO `+"`%s.%s.binders`"+`
		(binder_id, name, description, slots_per_page, created_at)
		VALUES (@binder_id, @name, @description, @slots_per_page, @created_at)`,
		h.client.Project(), h.dataset)
	q := h.client.Query(sql)
	q.Parameters = []bigquery.QueryParameter{
		{Name: "binder_id", Value: id},
		{Name: "name", Value: req.Name},
		{Name: "description", Value: req.Description},
		{Name: "slots_per_page", Value: req.SlotsPerPage},
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

	c.JSON(http.StatusCreated, gin.H{"binder_id": id})
	if h.triggerSync != nil {
		h.triggerSync()
	}
}

func (h *BinderHandler) Update(c *gin.Context) {
	id := c.Param("id")
	var req UpdateBinderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Check exists
	checkSQL := fmt.Sprintf("SELECT binder_id FROM `%s.%s.binders` WHERE binder_id = @binder_id LIMIT 1",
		h.client.Project(), h.dataset)
	cq := h.client.Query(checkSQL)
	cq.Parameters = []bigquery.QueryParameter{{Name: "binder_id", Value: id}}
	cit, err := cq.Read(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	var existing Binder
	if err := cit.Next(&existing); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "binder not found"})
		return
	}

	setClauses := []string{}
	params := []bigquery.QueryParameter{}

	if req.Name != "" {
		setClauses = append(setClauses, "name = @name")
		params = append(params, bigquery.QueryParameter{Name: "name", Value: req.Name})
	}
	if req.Description != "" {
		setClauses = append(setClauses, "description = @description")
		params = append(params, bigquery.QueryParameter{Name: "description", Value: req.Description})
	}

	if len(setClauses) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no fields to update"})
		return
	}

	params = append(params, bigquery.QueryParameter{Name: "binder_id", Value: id})
	sql := fmt.Sprintf("UPDATE `%s.%s.binders` SET %s WHERE binder_id = @binder_id",
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

func (h *BinderHandler) Delete(c *gin.Context) {
	id := c.Param("id")
	ctx := c.Request.Context()

	// Delete slots first
	slotSQL := fmt.Sprintf("DELETE FROM `%s.%s.binder_slots` WHERE binder_id = @binder_id",
		h.client.Project(), h.dataset)
	sq := h.client.Query(slotSQL)
	sq.Parameters = []bigquery.QueryParameter{{Name: "binder_id", Value: id}}
	job, err := sq.Run(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if _, err := job.Wait(ctx); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Delete binder
	sql := fmt.Sprintf("DELETE FROM `%s.%s.binders` WHERE binder_id = @binder_id",
		h.client.Project(), h.dataset)
	q := h.client.Query(sql)
	q.Parameters = []bigquery.QueryParameter{{Name: "binder_id", Value: id}}
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

func (h *BinderHandler) AddPage(c *gin.Context) {
	id := c.Param("id")
	ctx := c.Request.Context()

	// Get max page number
	maxSQL := fmt.Sprintf("SELECT COALESCE(MAX(page_number), 0) AS max_page FROM `%s.%s.binder_slots` WHERE binder_id = @binder_id",
		h.client.Project(), h.dataset)
	mq := h.client.Query(maxSQL)
	mq.Parameters = []bigquery.QueryParameter{{Name: "binder_id", Value: id}}
	mit, err := mq.Read(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	var maxRow struct {
		MaxPage int64 `bigquery:"max_page"`
	}
	if err := mit.Next(&maxRow); err != nil {
		maxRow.MaxPage = 0
	}
	newPage := maxRow.MaxPage + 1

	updatedAt := time.Now().UTC()
	insertSQL := fmt.Sprintf(`INSERT INTO `+"`%s.%s.binder_slots`"+`
		(binder_id, page_number, side, slot_position, card_game, card_set_code, card_number, card_notes, updated_at)
		VALUES (@binder_id, @page_number, @side, @slot_position, @card_game, @card_set_code, @card_number, @card_notes, @updated_at)`,
		h.client.Project(), h.dataset)
	iq := h.client.Query(insertSQL)
	iq.Parameters = []bigquery.QueryParameter{
		{Name: "binder_id", Value: id},
		{Name: "page_number", Value: newPage},
		{Name: "side", Value: "front"},
		{Name: "slot_position", Value: int64(1)},
		{Name: "card_game", Value: ""},
		{Name: "card_set_code", Value: ""},
		{Name: "card_number", Value: ""},
		{Name: "card_notes", Value: ""},
		{Name: "updated_at", Value: updatedAt},
	}

	job, err := iq.Run(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if _, err := job.Wait(ctx); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, gin.H{"page_number": newPage})
	if h.triggerSync != nil {
		h.triggerSync()
	}
}

func (h *BinderHandler) RemoveLastPage(c *gin.Context) {
	id := c.Param("id")
	pageStr := c.Param("page")
	page, err := strconv.ParseInt(pageStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid page number"})
		return
	}
	ctx := c.Request.Context()

	// Get max page number
	maxSQL := fmt.Sprintf("SELECT COALESCE(MAX(page_number), 0) AS max_page FROM `%s.%s.binder_slots` WHERE binder_id = @binder_id",
		h.client.Project(), h.dataset)
	mq := h.client.Query(maxSQL)
	mq.Parameters = []bigquery.QueryParameter{{Name: "binder_id", Value: id}}
	mit, err := mq.Read(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	var maxRow struct {
		MaxPage int64 `bigquery:"max_page"`
	}
	if err := mit.Next(&maxRow); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no pages found"})
		return
	}

	if page != maxRow.MaxPage {
		c.JSON(http.StatusBadRequest, gin.H{"error": "can only remove the last page"})
		return
	}

	delSQL := fmt.Sprintf("DELETE FROM `%s.%s.binder_slots` WHERE binder_id = @binder_id AND page_number = @page_number",
		h.client.Project(), h.dataset)
	dq := h.client.Query(delSQL)
	dq.Parameters = []bigquery.QueryParameter{
		{Name: "binder_id", Value: id},
		{Name: "page_number", Value: page},
	}

	job, err := dq.Run(ctx)
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

func (h *BinderHandler) UpsertPageSlots(c *gin.Context) {
	id := c.Param("id")
	pageStr := c.Param("page")
	page, err := strconv.ParseInt(pageStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid page number"})
		return
	}

	var req UpsertPageSlotsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx := c.Request.Context()

	// Delete existing slots for this page
	delSQL := fmt.Sprintf("DELETE FROM `%s.%s.binder_slots` WHERE binder_id = @binder_id AND page_number = @page_number",
		h.client.Project(), h.dataset)
	dq := h.client.Query(delSQL)
	dq.Parameters = []bigquery.QueryParameter{
		{Name: "binder_id", Value: id},
		{Name: "page_number", Value: page},
	}
	job, err := dq.Run(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if _, err := job.Wait(ctx); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Filter out fully empty slots
	var nonEmpty []SlotInput
	for _, s := range req.Slots {
		if s.CardGame != "" || s.CardSetCode != "" || s.CardNumber != "" {
			nonEmpty = append(nonEmpty, s)
		}
	}

	if len(nonEmpty) == 0 {
		c.Status(http.StatusNoContent)
		if h.triggerSync != nil {
			h.triggerSync()
		}
		return
	}

	updatedAt := time.Now().UTC()
	valueRows := make([]string, len(nonEmpty))
	params := []bigquery.QueryParameter{}

	for i, slot := range nonEmpty {
		s := fmt.Sprintf("_%d", i)
		valueRows[i] = fmt.Sprintf(
			"(@binder_id%s, @page_number%s, @side%s, @slot_position%s, @card_game%s, @card_set_code%s, @card_number%s, @card_notes%s, @updated_at%s)",
			s, s, s, s, s, s, s, s, s,
		)
		params = append(params,
			bigquery.QueryParameter{Name: "binder_id" + s, Value: id},
			bigquery.QueryParameter{Name: "page_number" + s, Value: page},
			bigquery.QueryParameter{Name: "side" + s, Value: slot.Side},
			bigquery.QueryParameter{Name: "slot_position" + s, Value: slot.SlotPosition},
			bigquery.QueryParameter{Name: "card_game" + s, Value: slot.CardGame},
			bigquery.QueryParameter{Name: "card_set_code" + s, Value: slot.CardSetCode},
			bigquery.QueryParameter{Name: "card_number" + s, Value: slot.CardNumber},
			bigquery.QueryParameter{Name: "card_notes" + s, Value: slot.CardNotes},
			bigquery.QueryParameter{Name: "updated_at" + s, Value: updatedAt},
		)
	}

	insertSQL := fmt.Sprintf(`INSERT INTO `+"`%s.%s.binder_slots`"+`
		(binder_id, page_number, side, slot_position, card_game, card_set_code, card_number, card_notes, updated_at)
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
