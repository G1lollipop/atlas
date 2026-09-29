package api

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/G1lollipop/atlas/internal/store"
)

type tenantSubjectContextKey struct{}

func tenantSubjectFromContext(ctx context.Context) string {
	if subject, ok := ctx.Value(tenantSubjectContextKey{}).(string); ok {
		return store.NormalizeTenant(subject)
	}
	return store.DefaultTenant
}

// jwtAuth still uses a shared-secret credential with broad API access. The verified
// subject is carried separately for scheduler tenant quotas; it does not grant or
// restrict authorization to particular jobs.
func jwtAuth(secret string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			header := r.Header.Get("Authorization")
			const prefix = "Bearer "
			if !strings.HasPrefix(header, prefix) || strings.TrimSpace(header[len(prefix):]) == "" {
				writeError(w, http.StatusUnauthorized, "missing or malformed authorization header")
				return
			}
			tokenStr := strings.TrimSpace(header[len(prefix):])

			token, err := jwt.Parse(tokenStr, func(t *jwt.Token) (interface{}, error) {
				if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
					return nil, errors.New("unexpected signing method")
				}
				return []byte(secret), nil
			}, jwt.WithValidMethods([]string{"HS256"}))
			if err != nil || !token.Valid {
				writeError(w, http.StatusUnauthorized, "invalid or expired token")
				return
			}

			subject, err := token.Claims.GetSubject()
			if err != nil {
				writeError(w, http.StatusUnauthorized, "invalid token subject")
				return
			}
			subject = store.NormalizeTenant(subject)
			if err := store.ValidateTenant(subject); err != nil {
				writeError(w, http.StatusUnauthorized, "invalid token subject")
				return
			}
			ctx := context.WithValue(r.Context(), tenantSubjectContextKey{}, subject)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// MintToken issues an HS256 token for subject valid for ttl. There is no login
// endpoint on this API (it's a shared-secret admin service), so this is the helper
// docs/integration tests/an operator CLI use to produce a token for testing or scripted
// access.
func MintToken(secret, subject string, ttl time.Duration) (string, error) {
	now := time.Now()
	claims := jwt.RegisteredClaims{
		Subject:   subject,
		IssuedAt:  jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString([]byte(secret))
}
