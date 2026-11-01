package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	_ "eventcollector.com/docs"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	amqp "github.com/rabbitmq/amqp091-go"
	httpSwagger "github.com/swaggo/http-swagger"
)

type EventRequest struct {
	UserID    int       `json:"user_id"`
	EventType string    `json:"event_type"`
	Timestamp time.Time `json:"timestamp"`
}
type Producer struct {
	Channel *amqp.Channel
}
type DBPool interface {
	Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error)
}

type Consumer struct {
	Channel *amqp.Channel
	Pool    DBPool
}
type ProducerInterface interface {
	Publish(event *EventRequest, ctx context.Context) error
}
type Handler struct {
	Producer ProducerInterface
}

func declareQueue(ch *amqp.Channel) error {
	_, err := ch.QueueDeclare(
		"events",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Printf("Failed to declare a queue. Error: %v\n", err)
		return err
	}
	return nil
}
func (p *Producer) declareQueue() error {
	return declareQueue(p.Channel)
}
func (c *Consumer) declareQueue() error {
	return declareQueue(c.Channel)
}
func NewProducer(conn *amqp.Connection) (*Producer, error) {
	channel, err := conn.Channel()
	if err != nil {
		log.Printf("Failed to create channel: %v\n", err)
		return nil, err
	}
	producer := &Producer{Channel: channel}
	err = producer.declareQueue()
	if err != nil {
		log.Printf("Failed to declare a queue. Error: %v\n", err)
		return nil, err
	}
	return producer, nil
}

func NewConsumer(conn *amqp.Connection, pool *pgxpool.Pool) (*Consumer, error) {
	channel, err := conn.Channel()
	if err != nil {
		log.Printf("Error creating an AMQP channel: %v\n", err)
		return nil, err
	}
	consumer := &Consumer{
		Channel: channel,
		Pool:    pool,
	}
	err = consumer.declareQueue()
	if err != nil {
		log.Printf("Failed to declare a queue. Error: %v\n", err)
		return nil, err
	}
	return consumer, nil
}
func closeChannel(ch *amqp.Channel) error {
	return ch.Close()
}
func (p *Producer) Close() error {
	return closeChannel(p.Channel)
}
func (c *Consumer) Close() error {
	return closeChannel(c.Channel)
}
func NewHandler(producer *Producer) *Handler {
	return &Handler{Producer: producer}
}
func (p *Producer) Publish(event *EventRequest, ctx context.Context) error {
	body, err := json.Marshal(event)
	if err != nil {
		log.Printf("Serialization error: %v\n", err)
		return err
	}
	publishing := amqp.Publishing{
		ContentType:  "application/json",
		Body:         body,
		DeliveryMode: 2,
	}
	c, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	err = p.Channel.PublishWithContext(c,
		"",
		"events",
		false, false,
		publishing,
	)
	if err != nil {
		log.Printf("Error publishing a message in RabbitMQ: %v\n", err)
		return err
	}
	return nil
}

func (c *Consumer) saveBatch(ctx context.Context, buffer []EventRequest) error {
	newContext, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for _, b := range buffer {
		_, err := c.Pool.Exec(newContext,
			`INSERT INTO events (user_id, event_type, created_at)
		    VALUES ($1, $2, $3)
			ON CONFLICT DO NOTHING`,
			b.UserID, b.EventType, b.Timestamp,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Consumer) saveToDatabase(ctx context.Context, buffer []EventRequest) error {
	const maxRetries = 3
	const retryDelay = 2 * time.Second
	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		err := c.saveBatch(ctx, buffer)
		if err == nil {
			return nil
		}
		lastErr = err
		log.Printf("Save failed (attempt %v/%v): %v", attempt, maxRetries, err)
		if attempt < maxRetries {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(retryDelay):
			}
		}
	}
	return fmt.Errorf("Failed after %v attempts: %v", maxRetries, lastErr)
}
func (c *Consumer) Start(ctx context.Context) error {
	events, err := c.Channel.Consume(
		"events",
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Printf("Failed to register a consumer. Error: %v\n", err)
		return err
	}
	var eventBuffer []EventRequest
	var wg sync.WaitGroup
	errChann := make(chan error, 10)
	go func() {
		for err := range errChann {
			log.Printf("Data saving error: %v\n", err)
		}
	}()
	go func() {
		wg.Wait()
		close(errChann)
	}()
	log.Printf(" [*] Waiting for events. To exit press CTRL+C")
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-events:
			if !ok {
				return nil
			}
			log.Printf("Received the event: %v\n", event.Body)
			var eventRequest EventRequest
			if err := json.Unmarshal(event.Body, &eventRequest); err != nil {
				log.Printf("Failed to parse event: %v\n", err)
				continue
			}
			eventBuffer = append(eventBuffer, eventRequest)
			if len(eventBuffer) >= 10 {
				batch := eventBuffer
				eventBuffer = nil
				wg.Add(1)
				go func(e []EventRequest) {
					defer wg.Done()
					if err := c.saveToDatabase(ctx, e); err != nil {
						errChann <- err
					}
				}(batch)
				log.Println("The set of events is recorded in the database")
			}
		}
	}
}

// @Summary Request handler
// @Description Request handler for receiving events in JSON format
// @Tags events
// @Accept json
// @Produce application/json
// @Param request body EventRequest true "Event"
// @Success 202 {string} string "Accepted"
// @Failure 405 {string} string "Method Not Allowed"
// @Failure 400 {string} string "Bad Request"
// @Failure 500 {string} string "Internal Server Error"
// @Router /reception [post]
func (h *Handler) ReceptionHandler(w http.ResponseWriter, httpReq *http.Request) {
	if httpReq.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer httpReq.Body.Close()
	var event EventRequest
	if err1 := json.NewDecoder(httpReq.Body).Decode(&event); err1 != nil {
		http.Error(w, "the server has a bad request", http.StatusBadRequest)
		log.Printf("Decoding error: %v\n", err1)
		return
	}
	if err2 := h.Producer.Publish(&event, httpReq.Context()); err2 != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		log.Printf("Producer error: %v\n", err2)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

// @title Event Collector
// @version 1.0
// @description event collection and processing
// @host localhost:8080
// @BasePath /
func main() {
	_ = godotenv.Load()
	httpHost := os.Getenv("HTTP_HOST")
	if httpHost == "" {
		httpHost = "0.0.0.0"
	}
	httpPort := os.Getenv("HTTP_PORT")
	if httpPort == "" {
		httpPort = "8080"
	}
	hostFlag := flag.String("httpHost", httpHost, "server host")
	portFlag := flag.String("httpPort", httpPort, "server port")
	flag.Parse()
	rabbitUser := os.Getenv("RABBIT_LOGIN")
	if rabbitUser == "" {
		log.Fatal("RABBIT_LOGIN is not set")
	}
	rabbitPassword := os.Getenv("RABBIT_PASSWORD")
	if rabbitPassword == "" {
		log.Fatal("RABBIT_PASSWORD is not set")
	}
	rabbitHost := os.Getenv("RABBIT_HOST")
	if rabbitHost == "" {
		rabbitHost = "localhost"
	}
	rabbitPort := os.Getenv("RABBIT_PORT")
	if rabbitPort == "" {
		rabbitPort = "5672"
	}
	rabbitConn := fmt.Sprintf("amqp://%v:%v@%v:%v/", rabbitUser, rabbitPassword, rabbitHost, rabbitPort)
	rabbit, err1 := amqp.Dial(rabbitConn)
	if err1 != nil {
		log.Fatalf("Unable to open connect to RabbitMQ server. Error: %v\n", err1)
	}
	defer func() {
		_ = rabbit.Close()
	}()
	dbUser := os.Getenv("DATABASE_LOGIN")
	if dbUser == "" {
		log.Fatal("DATABASE_LOGIN is not set")
	}
	dbPassword := os.Getenv("DATABASE_PASSWORD")
	if dbPassword == "" {
		log.Fatal("DATABASE_PASSWORD is not set")
	}
	dbHost := os.Getenv("DATABASE_HOST")
	if dbHost == "" {
		dbHost = "localhost"
	}
	dbPort := os.Getenv("DATABASE_PORT")
	if dbPort == "" {
		dbPort = "5432"
	}
	dbName := os.Getenv("DATABASE_NAME")
	if dbName == "" {
		log.Fatal("DATABASE_NAME is not set")
	}
	sslmode := os.Getenv("DATABASE_SSLMODE")
	if sslmode == "" {
		sslmode = "disable"
	}
	poolConn := fmt.Sprintf("postgres://%v:%v@%v:%v/%v?sslmode=%v", dbUser, dbPassword, dbHost, dbPort, dbName, sslmode)
	pool, err2 := pgxpool.New(context.Background(), poolConn)
	if err2 != nil {
		log.Fatalf("Failed to connect to PostgreSQL. Error: %v\n", err2)
	}
	defer pool.Close()
	producer, err3 := NewProducer(rabbit)
	if err3 != nil {
		log.Fatalf("Failed to create producer: %v\n", err3)
	}
	defer producer.Close()
	consumer, err4 := NewConsumer(rabbit, pool)
	if err4 != nil {
		log.Fatalf("Failed to create consumer: %v\n", err4)
	}
	defer consumer.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		if err := consumer.Start(ctx); err != nil {
			log.Printf("Consumer error: %v\n", err)
		}
	}()
	handler := NewHandler(producer)
	http.HandleFunc("/reception", handler.ReceptionHandler)
	http.HandleFunc("/swagger/", httpSwagger.WrapHandler)
	addr := fmt.Sprintf("%v:%v", *hostFlag, *portFlag)
	go func() {
		log.Printf("The server is running on: %v\n", addr)
		log.Fatal(http.ListenAndServe(addr, nil))
	}()
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	log.Println("Shutting down server...")
	cancel()
	time.Sleep(2 * time.Second)
}
