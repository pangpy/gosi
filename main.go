package main

import (
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/gorilla/websocket"
	"log"
	"net/http"
	"sync"
)

// Kafka producer
var producer sarama.SyncProducer

// WebSocket upgrader
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// WebSocket connection map
var (
	connections = make(map[string]map[*websocket.Conn]string) // map of topics to WebSocket connections and their usernames
	mutex       sync.Mutex
)

type Message struct {
	Type     string `json:"type"`
	Topic    string `json:"topic,omitempty"`
	Username string `json:"username,omitempty"`
	Data     string `json:"data"`
}

func handleConnection(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Println("Error while upgrading connection:", err)
		return
	}
	defer conn.Close()

	var currentTopic string

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			fmt.Println("Error while reading message:", err)
			if currentTopic != "" {
				mutex.Lock()
				delete(connections[currentTopic], conn)
				mutex.Unlock()
			}
			break
		}

		var message Message
		err = json.Unmarshal(msg, &message)
		if err != nil {
			fmt.Println("Error while unmarshaling message:", err)
			continue
		}

		switch message.Type {
		case "setTopic":
			mutex.Lock()
			if len(connections[message.Topic]) >= 2 {
				conn.WriteJSON(Message{Type: "error", Data: "Topic is full"})
				conn.Close()
				mutex.Unlock()
				return
			}
			if connections[message.Topic] == nil {
				connections[message.Topic] = make(map[*websocket.Conn]string)
			}
			connections[message.Topic][conn] = ""
			currentTopic = message.Topic
			mutex.Unlock()
		case "setUsername":
			mutex.Lock()
			if _, exists := connections[message.Topic][conn]; exists {
				connections[message.Topic][conn] = message.Data
			} else {
				conn.WriteJSON(Message{Type: "error", Data: "Set a topic first"})
				conn.Close()
				mutex.Unlock()
				return
			}
			mutex.Unlock()
		case "message":
			if currentTopic == "" {
				conn.WriteJSON(Message{Type: "error", Data: "Set a topic first"})
				continue
			}
			// Publish message to Kafka
			_, _, err = producer.SendMessage(&sarama.ProducerMessage{
				Topic: currentTopic,
				Value: sarama.StringEncoder(fmt.Sprintf("%s: %s", connections[currentTopic][conn], message.Data)),
			})
			if err != nil {
				fmt.Println("Error while producing message:", err)
			}

			// Forward message to other WebSocket clients
			mutex.Lock()
			for client := range connections[currentTopic] {
				if err := client.WriteJSON(Message{Type: "message", Username: connections[currentTopic][conn], Data: message.Data}); err != nil {
					fmt.Println("Error while sending message to client:", err)
					delete(connections[currentTopic], client)
					client.Close()
				}
			}
			mutex.Unlock()
		}
	}
}

func main() {
	// Initialize Kafka producer
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Version = sarama.V2_3_0_0
	var err error
	producer, err = sarama.NewSyncProducer([]string{"192.168.1.13:9092"}, config)
	if err != nil {
		log.Fatal("Error creating Kafka producer:", err)
	}
	defer producer.Close()

	// Serve static files from the "static" directory
	fs := http.FileServer(http.Dir("./static"))
	http.Handle("/", fs)

	// Handle WebSocket connections
	http.HandleFunc("/ws", handleConnection)

	fmt.Println("Server started at :8080")
	http.ListenAndServe(":8080", nil)
}
