package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
)

/*
Sample requests:
/put
{
    "ts":"now",
    "values" :[{
        "value": 100
    }]
}


/query
{
    "begin":"-10m",
    "end":"now"
}
*/

type (
	// QueryRequest query
	QueryRequest struct {
		Columns        string `json:"columns,omitempty"`
		Begin          string `json:"begin,omitempty"`
		End            string `json:"end,omitempty"`
		IncludeInvalid bool   `json:"include_invalid,omitempty"`
	}

	// QueryResponse for query
	QueryResponse struct {
		Begin   int64       `json:"begin"`
		End     int64       `json:"end"`
		Columns []string    `json:"columns"`
		Data    [][]float32 `json:"data"`
	}

	// PutValue is one value to put with PutRequest
	PutValue struct {
		Column string  `json:"column,omitempty"`
		Value  float32 `json:"value"`
	}

	// PutRequest - data for put request
	PutRequest struct {
		TS     string     `json:"ts,omitempty"`
		Values []PutValue `json:"values"`
	}

	// Server handle rest request
	Server struct {
		Address string
		router  *mux.Router

		DbFilename string
		db         *RRD
	}
)

// Start server
func (s *Server) Start() {
	s.router = mux.NewRouter()
	s.router.HandleFunc("/query", s.queryHandler).Methods("POST")
	s.router.HandleFunc("/put", s.putHandler).Methods("POST")
	http.Handle("/", s.router)

	f, err := OpenRRD(s.DbFilename, false)
	if err != nil {
		fmt.Println("Open db error: " + err.Error())
		return
	}

	s.db = f

	server := &http.Server{
		Addr: s.Address,
	}
	fmt.Printf("Start listen on %s\n", s.Address)
	server.ListenAndServe()
}

func (s *Server) queryHandler(w http.ResponseWriter, r *http.Request) {
	Log("Server.queryHandler %s from %s", r.RequestURI, r.RemoteAddr)
	var req QueryRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, fmt.Sprintf("decode error %s\n", err.Error()), http.StatusBadRequest)
		return
	}

	LogDebug("Server.putHandler req: %+v", req)

	if req.Begin == "" {
		req.Begin = "0"
	}

	tsMin, ok := dateToTs(req.Begin)
	if !ok {
		http.Error(w, "bad begin date", http.StatusBadRequest)
		return
	}

	if req.End == "" {
		req.End = "now"
	}
	tsMax, ok := dateToTs(req.End)
	if !ok {
		http.Error(w, "bad end date", http.StatusBadRequest)
		return
	}

	var columns []int

	if len(req.Columns) > 0 {
		cols, err := parseStrIntList(req.Columns)
		if err != nil {
			http.Error(w, fmt.Sprintf("wrong columns: %s\n", err.Error()), http.StatusBadRequest)
			return
		}
		columns = cols
	}

	resp := QueryResponse{
		Begin: tsMin,
		End:   tsMax,
	}
	if rows, err := s.db.GetRange(tsMin, tsMax, columns, req.IncludeInvalid, true); err == nil {
		for idx, row := range rows {
			if idx == 0 {
				for _, col := range row.Values {
					resp.Columns = append(resp.Columns, s.db.ColumnName(col.Column))
				}
			}
			var rrow []float32
			for _, col := range row.Values {
				if col.Valid {
					rrow = append(rrow, col.Value)
				} else {
					rrow = append(rrow, 0)
				}
			}
			resp.Data = append(resp.Data, rrow)
		}
	}

	LogDebug("Server.queryHandler res: %+v", resp)

	j, err := json.Marshal(resp)
	if err != nil {
		fmt.Printf("encode error %s\n", err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(j)
}

func (s *Server) putHandler(w http.ResponseWriter, r *http.Request) {
	Log("Server.putHandler %s from %s", r.RequestURI, r.RemoteAddr)
	var req PutRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, fmt.Sprintf("decode error %s\n", err.Error()), http.StatusBadRequest)
		return
	}

	LogDebug("Server.putHandler req: %+v", req)

	if req.TS == "" {
		req.TS = "0"
	}

	ts, ok := dateToTs(req.TS)
	if !ok {
		http.Error(w, "bad ts date", http.StatusBadRequest)
		return
	}

	var values []Value
	for idx, v := range req.Values {
		value := Value{
			TS:     ts,
			Value:  v.Value,
			Column: idx,
			Valid:  true,
		}
		if v.Column != "" {
			c, err := strconv.Atoi(v.Column)
			if err != nil {
				http.Error(w, "column in value "+string(idx), http.StatusBadRequest)
				return
			}
			value.Column = c
		}
		values = append(values, value)
	}

	LogDebug("Server.putHandler res: %+v", values)
	err = s.db.PutValues(values...)
	if err != nil {
		http.Error(w, "put error "+err.Error(), http.StatusBadRequest)
		return
	}
	s.db.Flush()

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}
