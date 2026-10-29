package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
)

type MockPool struct {
	execFunc func(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error)
}

func (m *MockPool) Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error) {
	return m.execFunc(ctx, sql, args...)
}

type MockProducer struct {
	publishFunc func(event *EventRequest, ctx context.Context) error
}

func (m *MockProducer) Publish(event *EventRequest, ctx context.Context) error {
	return m.publishFunc(event, ctx)
}
func TestSaveBatch_Success(t *testing.T) {
	mock := &MockPool{
		execFunc: func(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error) {
			return pgconn.CommandTag{}, nil
		},
	}
	consumer := &Consumer{Pool: mock}
	if err := consumer.saveBatch(context.Background(), []EventRequest{{UserID: 1}}); err != nil {
		t.Errorf("Expected no error, got %v\n", err)
	}
}
func TestSaveBatch_DBError(t *testing.T) {
	mock := &MockPool{
		execFunc: func(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error) {
			return pgconn.CommandTag{}, errors.New("db error")
		},
	}
	consumer := &Consumer{Pool: mock}
	if err := consumer.saveBatch(context.Background(), []EventRequest{{UserID: 1}}); err == nil {
		t.Error("Expected error, got nil\n")
	}
}
func TestSaveBatch_EmptyBuffer(t *testing.T) {
	execCalled := false
	mock := &MockPool{
		execFunc: func(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error) {
			execCalled = true
			return pgconn.CommandTag{}, nil
		},
	}
	consumer := &Consumer{Pool: mock}
	if err := consumer.saveBatch(context.Background(), []EventRequest{}); err != nil {
		t.Errorf("Expected no error, got %v\n", err)
	}
	if execCalled {
		t.Error("Exec should not be called for empty buffer\n")
	}
}
func TestSaveToDatabase_SuccessFirstAttempt(t *testing.T) {
	attempts := 0
	mock := &MockPool{
		execFunc: func(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error) {
			attempts++
			return pgconn.CommandTag{}, nil
		},
	}
	consumer := &Consumer{Pool: mock}
	if err := consumer.saveToDatabase(context.Background(), []EventRequest{{UserID: 1}}); err != nil {
		t.Errorf("Expected no error, got %v\n", err)
	}
	if attempts != 1 {
		t.Errorf("Expected 1 attempt, got %v\n", attempts)
	}
}
func TestSaveToDatabase_RetrySuccess(t *testing.T) {
	attempts := 0
	mock := &MockPool{
		execFunc: func(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error) {
			attempts++
			if attempts < 3 {
				return pgconn.CommandTag{}, errors.New("Temporary error\n")
			}
			return pgconn.CommandTag{}, nil
		},
	}
	consumer := &Consumer{Pool: mock}
	if err := consumer.saveToDatabase(context.Background(), []EventRequest{{UserID: 1}}); err != nil {
		t.Errorf("Expected no error, got %v\n", err)
	}
	if attempts != 3 {
		t.Errorf("Expected 3 attempt, got %v\n", attempts)
	}
}
func TestSaveToDatabase_Failure(t *testing.T) {
	attempts := 0
	mock := &MockPool{
		execFunc: func(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error) {
			attempts++
			return pgconn.CommandTag{}, errors.New("Permanent error\n")
		},
	}
	consumer := &Consumer{Pool: mock}
	if err := consumer.saveToDatabase(context.Background(), []EventRequest{{UserID: 1}}); err == nil {
		t.Error("Expected error, got nil\n")
	}
	if attempts != 3 {
		t.Errorf("Expected 3 attemtps, got %v\n", attempts)
	}
}
func TestSaveToDatabase_ContextCancel(t *testing.T) {
	mock := &MockPool{
		execFunc: func(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error) {
			return pgconn.CommandTag{}, errors.New("Temporary error\n")
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	consumer := &Consumer{Pool: mock}
	err := consumer.saveToDatabase(ctx, []EventRequest{{UserID: 1}})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Expected context.Canceled, got %v\n", err)
	}
}
func TestReceptionHandler_IncorrectMethod(t *testing.T) {
	handler := &Handler{}
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/reception", nil)
	handler.ReceptionHandler(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected error 405, got %v\n", w.Code)
	}
}
func TestReceptionHandler_InvalidJson(t *testing.T) {
	handler := &Handler{}
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/reception", strings.NewReader("not json"))
	handler.ReceptionHandler(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected error 400, got %v\n", w.Code)
	}
}
func TestReceptionHandler_Success(t *testing.T) {
	mock := &MockProducer{
		publishFunc: func(event *EventRequest, ctx context.Context) error {
			return nil
		},
	}
	handler := &Handler{Producer: mock}
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/reception", strings.NewReader(`{"user_id":1}`))
	handler.ReceptionHandler(w, req)
	if w.Code != http.StatusAccepted {
		t.Errorf("Expected error 202, got %v\n", w.Code)
	}
}
func TestReceptionHandler(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test\n")
	}
	conn, err1 := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err1 != nil {
		t.Fatalf("Unable to open connect to RabbitMQ server. Error: %v\n", err1)
	}
	defer conn.Close()
	producer, err2 := NewProducer(conn)
	if err2 != nil {
		t.Fatalf("Failed to create producer: %v\n", err2)
	}
	defer producer.Close()
	handler := NewHandler(producer)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/reception", strings.NewReader(`{"user_id":1,"event_type":"test","timestamp":"2026-10-14T23:04:00Z"}`))
	handler.ReceptionHandler(w, req)
	if w.Code != http.StatusAccepted {
		t.Fatalf("Expected error 202, got %v\n", w.Code)
	}
}
func TestPublish(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test\n")
	}
	conn, err1 := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err1 != nil {
		t.Fatalf("Unable to open connect to RabbitMQ server. Error: %v\n", err1)
	}
	defer conn.Close()
	producer, err2 := NewProducer(conn)
	if err2 != nil {
		t.Fatalf("Failed to create producer: %v\n", err2)
	}
	defer producer.Close()
	event := &EventRequest{
		UserID:    1,
		EventType: "test",
		Timestamp: time.Now(),
	}
	if err3 := producer.Publish(event, context.Background()); err3 != nil {
		t.Fatalf("Producer error: %v\n", err3)
	}
}
func TestStart(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test\n")
	}
	conn, err1 := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err1 != nil {
		t.Fatalf("Unable to open connect to RabbitMQ server. Error: %v\n", err1)
	}
	defer conn.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool, err2 := pgxpool.New(ctx, "postgres://postgres:postgres@localhost:5432/events?sslmode=disable")
	if err2 != nil {
		t.Fatalf("Unable to connect: %v\n", err2)
	}
	defer pool.Close()
	producer, _ := NewProducer(conn)
	defer producer.Close()
	producer.Publish(&EventRequest{
		UserID:    1,
		EventType: "test",
		Timestamp: time.Now(),
	}, ctx)
	consumer, err3 := NewConsumer(conn, pool)
	if err3 != nil {
		t.Fatalf("Failed to create consumer: %v\n", err3)
	}
	defer consumer.Close()
	go func() {
		if err4 := consumer.Start(ctx); err4 != nil {
			t.Errorf("Consumer error: %v\n", err4)
		}
	}()
	time.Sleep(2 * time.Second)
	cancel()
	var count int
	pool.QueryRow(ctx, "SELECT COUNT(*) FROM events").Scan(&count)
	if count == 0 {
		t.Error("Expected event in DB, got 0\n")
	}
}
