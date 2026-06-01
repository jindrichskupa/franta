//go:build integration

// Package decode_test provides external integration tests for the decode
// package. An external test package is required here because internal/kafka
// imports franta/internal/decode (for the Decoder interface), and an internal
// test would create an import cycle: decode → kafka → decode.
package decode_test

import (
	"context"
	"encoding/binary"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"franta/internal/config"
	"franta/internal/decode"
	kafkapkg "franta/internal/kafka"
)

// personSchema duplicates the constant from codec_test.go; that file uses
// package decode (internal) which is inaccessible here due to the import cycle
// between decode and kafka (kafka imports decode for the Decoder interface).
const personSchema = `{"type":"record","name":"Person","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}`

func TestConfluentRoundTripAvro(t *testing.T) {
	ctx := context.Background()
	rp, err := redpanda.Run(ctx, "redpandadata/redpanda:latest")
	if err != nil {
		t.Fatalf("start redpanda: %v", err)
	}
	defer func() { _ = rp.Terminate(ctx) }()

	broker, err := rp.KafkaSeedBroker(ctx)
	if err != nil {
		t.Fatalf("seed broker: %v", err)
	}
	srURL, err := rp.SchemaRegistryAddress(ctx)
	if err != nil {
		t.Fatalf("schema registry address: %v", err)
	}

	// Create the topic.
	const topic = "sr-it"
	admClient, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	adm := kadm.NewClient(admClient)
	if _, err := adm.CreateTopics(ctx, 1, 1, nil, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	admClient.Close()

	// Register an Avro schema for <topic>-value.
	srClient, err := sr.NewClient(sr.URLs(srURL))
	if err != nil {
		t.Fatalf("sr client: %v", err)
	}
	ss, err := srClient.CreateSchema(ctx, topic+"-value", sr.Schema{Schema: personSchema, Type: sr.TypeAvro})
	if err != nil {
		t.Fatalf("register schema: %v", err)
	}

	// Encode an Avro record and wrap it in the Confluent wire format.
	sch, _ := avro.Parse(personSchema)
	body, err := avro.Marshal(sch, map[string]any{"name": "alice", "age": 30})
	if err != nil {
		t.Fatalf("avro marshal: %v", err)
	}
	wire := make([]byte, 5+len(body))
	wire[0] = 0x00
	binary.BigEndian.PutUint32(wire[1:5], uint32(ss.ID))
	copy(wire[5:], body)

	// Produce the raw wire bytes directly via kgo (the franta Producer would
	// JSON-bias the path, but we want raw bytes).
	prod, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	if err := prod.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: wire}).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
	prod.Close()

	// Consume via the franta Consumer with a real Confluent decoder.
	cl := config.Cluster{Brokers: []string{broker}, Auth: config.Auth{Type: "plaintext"}}
	consClient, err := kafkapkg.NewClient(cl, topic)
	if err != nil {
		t.Fatalf("consumer client: %v", err)
	}
	defer consClient.Close()
	cons := kafkapkg.NewConsumer(consClient, topic)
	dec, err := decode.NewConfluent(srURL, "", "")
	if err != nil {
		t.Fatalf("decoder: %v", err)
	}
	cons.UseDecoder(dec)
	if err := cons.Seek(ctx, kafkapkg.StartSpec{Kind: kafkapkg.StartBeginning}); err != nil {
		t.Fatalf("seek: %v", err)
	}

	out := make(chan kafkapkg.Fetched, 4)
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = cons.Run(cctx, out, nil) }()

	select {
	case f := <-out:
		got := f.Record.ValueDisplay()
		if !strings.Contains(got, `"name": "alice"`) || !strings.Contains(got, `"age": 30`) {
			t.Fatalf("decoded = %q", got)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for a decoded record")
	}
}

func TestConfluentRoundTripProtobuf(t *testing.T) {
	ctx := context.Background()
	rp, err := redpanda.Run(ctx, "redpandadata/redpanda:latest")
	if err != nil {
		t.Fatalf("start redpanda: %v", err)
	}
	defer func() { _ = rp.Terminate(ctx) }()

	broker, err := rp.KafkaSeedBroker(ctx)
	if err != nil {
		t.Fatalf("seed broker: %v", err)
	}
	srURL, err := rp.SchemaRegistryAddress(ctx)
	if err != nil {
		t.Fatalf("schema registry address: %v", err)
	}

	const topic = "sr-proto-it"
	const personProto = `syntax = "proto3";
package fr.test;
message Person {
  string name = 1;
  int32 age = 2;
}`

	admClient, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	adm := kadm.NewClient(admClient)
	if _, err := adm.CreateTopics(ctx, 1, 1, nil, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	admClient.Close()

	srClient, err := sr.NewClient(sr.URLs(srURL))
	if err != nil {
		t.Fatalf("sr client: %v", err)
	}
	ss, err := srClient.CreateSchema(ctx, topic+"-value", sr.Schema{Schema: personProto, Type: sr.TypeProtobuf})
	if err != nil {
		t.Fatalf("register protobuf schema: %v", err)
	}

	// Build the wire-format payload: 0x00 + uint32(id) + 0x00 (single top-level
	// message index) + protobuf body.
	fd, err := decode.CompileProtoForTest(personProto)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	desc := fd.Messages().Get(0)
	msg := dynamicpb.NewMessage(desc)
	msg.Set(desc.Fields().ByName("name"), protoreflect.ValueOfString("alice"))
	msg.Set(desc.Fields().ByName("age"), protoreflect.ValueOfInt32(30))
	body, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("proto marshal: %v", err)
	}
	wire := make([]byte, 5+1+len(body))
	wire[0] = 0x00
	binary.BigEndian.PutUint32(wire[1:5], uint32(ss.ID))
	wire[5] = 0x00 // message-index short form
	copy(wire[6:], body)

	prod, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	if err := prod.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: wire}).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
	prod.Close()

	cl := config.Cluster{Brokers: []string{broker}, Auth: config.Auth{Type: "plaintext"}}
	consClient, err := kafkapkg.NewClient(cl, topic)
	if err != nil {
		t.Fatalf("consumer client: %v", err)
	}
	defer consClient.Close()
	cons := kafkapkg.NewConsumer(consClient, topic)
	dec, err := decode.NewConfluent(srURL, "", "")
	if err != nil {
		t.Fatalf("decoder: %v", err)
	}
	cons.UseDecoder(dec)
	if err := cons.Seek(ctx, kafkapkg.StartSpec{Kind: kafkapkg.StartBeginning}); err != nil {
		t.Fatalf("seek: %v", err)
	}

	out := make(chan kafkapkg.Fetched, 4)
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = cons.Run(cctx, out, nil) }()

	select {
	case f := <-out:
		got := f.Record.ValueDisplay()
		// protojson injects randomized whitespace after `:` — match with regex.
		if !regexp.MustCompile(`"name":\s*"alice"`).MatchString(got) ||
			!regexp.MustCompile(`"age":\s*30`).MatchString(got) {
			t.Fatalf("decoded = %q", got)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for decoded protobuf record")
	}
}
