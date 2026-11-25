package service

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/jingyugao/cell-box/internal/guestapi"
)

func (s *Service) updateWorkload(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Revision string `json:"revision"`
	}
	if err := decodeOptionalLimit(w, r, &input, 256); err != nil {
		fail(w, err)
		return
	}
	// A running exec or an open conversation does not prevent a workload switch.
	b, release, err := s.beginIOWithPolicy(clientID(r), r.PathValue("id"), true)
	if err != nil {
		fail(w, err)
		return
	}
	defer release()
	if b.Profile.Guest.Env[guestapi.WorkloadReleasesEnv] == "" {
		fail(w, apiError("UNSUPPORTED_CAPABILITY", "Box profile does not configure mounted workload releases"))
		return
	}
	body, _ := json.Marshal(input)
	ctx, cancel := context.WithTimeout(r.Context(), 12*time.Second)
	defer cancel()
	res, err := s.guestRequest(ctx, b, "PUT", "/v1/workload", bytes.NewReader(body))
	if err != nil {
		fail(w, err)
		return
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusNotFound {
		fail(w, apiError("UNSUPPORTED_CAPABILITY", "This Guest does not support mounted workload upgrades"))
		return
	}
	if err := guestSuccess(res); err != nil {
		fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
