package main

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type Result struct {
	URL    string
	Status string
	Body   string
}

func worker(id int, jobs <-chan string, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()

	client := http.Client{
		Timeout: 5 * time.Second,
	}

	for url := range jobs {
		resp, err := client.Get(url)
		if err != nil {
			results <- Result{URL: url, Status: "ERROR", Body: err.Error()}
			continue
		}

		bodyBytes, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		// limit body to avoid huge terminal output
		body := string(bodyBytes)
		if len(body) > 200 {
			body = body[:200]
		}

		results <- Result{
			URL:    url,
			Status: resp.Status,
			Body:   body,
		}
	}
}

func main() {

	totalUrls := 3000
	workerCount := 50

	jobs := make(chan string, totalUrls)
	results := make(chan Result, totalUrls)

	var wg sync.WaitGroup

	start := time.Now()

	// Workers
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go worker(i, jobs, results, &wg)
	}

	// Job producer
	go func() {
		for i := 1; i <= totalUrls; i++ {
			url := "https://jsonplaceholder.typicode.com/posts/" + strconv.Itoa((i%100)+1)
			jobs <- url
		}
		close(jobs)
	}()

	// Close results after workers finish
	go func() {
		wg.Wait()
		close(results)
	}()

	// Print results
	count := 0
	for res := range results {
		fmt.Println("====================================")
		fmt.Println("URL   :", res.URL)
		fmt.Println("STATUS:", res.Status)
		fmt.Println("BODY  :", res.Body)
		count++
	}

	fmt.Println("====================================")
	fmt.Println("Total requests:", count)
	fmt.Println("Total time:", time.Since(start))
}
