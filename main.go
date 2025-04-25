/*
* This file contains the reproduction steps necessary for a bug in the devconsole proxy in which
* the proxy forwards many requests to a server which takes a long time to respond, and the clients
* continue to cancel the old requests and send them again in hopes the retry will complete.
*
* Because the proxy does not use its incoming-requests' Contexts when creating the outgoing server-request,
* it keeps the outgoing connection alive after the incoming connection is closed, allowing these long-lived
* TCP connections to build up, eventually leading to the proxy being unable to dial the server since the OS
* cannot assign any more socket addresses.
*
* Usage: go run .
 */
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/openshift/console/pkg/auth"
	"github.com/openshift/console/pkg/devconsole/proxy"
)

func runProxy() {
	log.Print("Starting proxy server")

	server := &http.Server{
		Addr: ":8000",
		Handler: http.HandlerFunc(func(responseWriter http.ResponseWriter, request *http.Request) {
			proxy.Handler(&auth.User{}, responseWriter, request)
		}),
	}
	log.Fatal(server.ListenAndServe())
}

// runWaitServer runs a server which holds onto requests for 60 seconds before responding,
// simulating server work
func runWaitServer() {
	log.Print("Starting greedy server")

	requestsHeld := 0
	requestCountLock := sync.Mutex{}

	go func() {
		for {
			time.Sleep(10 * time.Second)
			log.Printf("Holding %d requests", requestsHeld)
		}
	}()

	handleFunc := func(responseWriter http.ResponseWriter, request *http.Request) {
		requestCountLock.Lock()
		requestsHeld = requestsHeld + 1
		requestCountLock.Unlock()

		defer func() {
			requestCountLock.Lock()
			requestsHeld = requestsHeld - 1
			requestCountLock.Unlock()
		}()

		start := time.Now()

		// With timeout will wait for either the request's context to be cancelled or 60
		// seconds to expire, whichever is first
		wait, cancel := context.WithTimeout(request.Context(), 60*time.Second)
		// This is just to be safe
		defer cancel()

		// Block until the wait is complete
		<-wait.Done()

		responseWriter.Write([]byte(fmt.Sprintf("Request completed in %.2f seconds", time.Since(start).Seconds())))
	}

	server := &http.Server{
		Addr:    ":9000",
		Handler: http.HandlerFunc(handleFunc),
	}
	log.Fatal(server.ListenAndServe())
}

// runBrrrrClient starts up a client which makes many requests to the WaitServer via the Proxy in quick succession,
// closing the request connections before waiting on the server to respond
func runBrrrrClient() {
	log.Print("Starting abusive client")

	client := http.Client{
		Timeout: 1 * time.Millisecond,
	}

	for {
		requestBody, _ := json.Marshal(proxy.ProxyRequest{
			Method: "GET",
			Url:    "http://127.0.0.1:9000",
		})
		request, err := client.Post("http://127.0.0.1:8000/proxy/internet", "application/json", bytes.NewBuffer(requestBody))
		if err != nil && !strings.Contains(err.Error(), "context deadline exceeded") {
			log.Printf("error posting to proxy: %v", err)
		}
		if request != nil && request.Body != nil {
			body, err := io.ReadAll(request.Body)
			if err != nil {
				log.Printf("error reading response: %v", err)
			} else {
				log.Println(string(body))
			}
		}
	}
}

func main() {
	go runWaitServer()
	go runProxy()

	time.Sleep(2 * time.Second)

	for range 5 {
		go runBrrrrClient()
	}

	runBrrrrClient()
}
