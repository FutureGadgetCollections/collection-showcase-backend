package middleware

import (
	"net/http"
	"strings"

	"firebase.google.com/go/v4/auth"
	"github.com/gin-gonic/gin"
)

// RequireAuth verifies a Firebase ID token and checks the caller's email
// against the allowlist. Attach to any route group that should be write-only.
func RequireAuth(authClient *auth.Client, allowedEmails []string) gin.HandlerFunc {
	allowed := make(map[string]struct{}, len(allowedEmails))
	for _, e := range allowedEmails {
		allowed[strings.ToLower(strings.TrimSpace(e))] = struct{}{}
	}

	return func(c *gin.Context) {
		header := c.GetHeader("Authorization")
		if !strings.HasPrefix(header, "Bearer ") {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing authorization header"})
			return
		}
		idToken := strings.TrimPrefix(header, "Bearer ")

		token, err := authClient.VerifyIDToken(c.Request.Context(), idToken)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid token"})
			return
		}

		email, _ := token.Claims["email"].(string)
		if _, ok := allowed[strings.ToLower(email)]; !ok {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "not authorized"})
			return
		}

		c.Set("email", email)
		c.Next()
	}
}
