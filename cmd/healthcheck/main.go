package main

import (
	"encoding/json"
	"net/http"
	"os"
	"time"
)

func main() {
	config := map[string]string{}
	raw, e := os.ReadFile("/run/service.json")
	if e != nil {
		os.Exit(1)
	}
	if json.Unmarshal(raw, &config) != nil {
		os.Exit(1)
	}
	token := config["SERVICE_TOKEN"]
	if v := os.Getenv("HEALTH_TOKEN_KEY"); v != "" {
		token = config[v]
	}
	url := os.Getenv("HEALTH_URL")
	if url == "" {
		os.Exit(1)
	}
	req, e := http.NewRequest("GET", url, nil)
	if e != nil {
		os.Exit(1)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	c := http.Client{Timeout: 3 * time.Second}
	resp, e := c.Do(req)
	if e != nil {
		os.Exit(1)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		os.Exit(1)
	}
}
