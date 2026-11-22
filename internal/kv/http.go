package kv

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/iotaaaaaaaaa/raftkv/internal/raft"
)

// Handler exposes the store over HTTP:
//
//	PUT    /kv/{key}   body is the value   -> 204
//	GET    /kv/{key}                       -> 200 with the value, or 404
//	DELETE /kv/{key}                       -> 204
//	GET    /status                         -> this node's Raft status as JSON
//
// A request sent to a follower gets a 307 redirect to the leader, so a client
// (or curl -L) can talk to any node.
func Handler(s *Store, rf *raft.Raft, httpAddrs map[string]string, timeout time.Duration) http.Handler {
	mux := http.NewServeMux()

	run := func(w http.ResponseWriter, r *http.Request, op Op) (Result, bool) {
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		res, err := s.Do(ctx, op)
		if err == nil {
			return res, true
		}
		var notLeader *NotLeaderError
		switch {
		case errors.As(err, &notLeader):
			if addr, ok := httpAddrs[notLeader.Leader]; ok {
				w.Header().Set("X-Raft-Leader", notLeader.Leader)
				// 307 keeps the method and body, unlike 302.
				http.Redirect(w, r, "http://"+addr+r.URL.Path, http.StatusTemporaryRedirect)
			} else {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
			}
		case errors.Is(err, ErrLeadershipLost):
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
		default:
			http.Error(w, err.Error(), http.StatusGatewayTimeout)
		}
		return Result{}, false
	}

	mux.HandleFunc("PUT /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if _, ok := run(w, r, Op{Kind: "put", Key: r.PathValue("key"), Value: string(body)}); ok {
			w.WriteHeader(http.StatusNoContent)
		}
	})

	mux.HandleFunc("GET /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		res, ok := run(w, r, Op{Kind: "get", Key: r.PathValue("key")})
		if !ok {
			return
		}
		if !res.Found {
			http.Error(w, "key not found", http.StatusNotFound)
			return
		}
		io.WriteString(w, res.Value)
	})

	mux.HandleFunc("DELETE /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		if _, ok := run(w, r, Op{Kind: "delete", Key: r.PathValue("key")}); ok {
			w.WriteHeader(http.StatusNoContent)
		}
	})

	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		st := rf.Status()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id": st.ID, "state": st.State.String(), "term": st.Term,
			"leader": st.LeaderID, "commit_index": st.CommitIndex, "last_index": st.LastIndex,
		})
	})
	return mux
}
