package handlers

import (
	"cloud.google.com/go/bigquery"
	"github.com/FutureGadgetLabs/collection-showcase-backend/internal/pricefetch"
	"github.com/gin-gonic/gin"
)

type PriceFetchHandler struct {
	client     *bigquery.Client
	invDataset string
	mktDataset string
}

func NewPriceFetchHandler(client *bigquery.Client, invDataset, mktDataset string) *PriceFetchHandler {
	return &PriceFetchHandler{client: client, invDataset: invDataset, mktDataset: mktDataset}
}

func (h *PriceFetchHandler) Fetch(c *gin.Context) {
	results, err := pricefetch.Run(c.Request.Context(), h.client, h.invDataset, h.mktDataset)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"results": results})
}
