// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package main

import (
	"context"
	"log"
    "time"

	"github.com/apache/pulsar-client-go/pulsar"
)

func main() {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

    consumer, err := client.Subscribe(pulsar.ConsumerOptions{
        Topic:            "topic-delay",
        SubscriptionName: "my-sub",
        Type:             pulsar.Pop,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer consumer.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: "topic-delay",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()


    // produce
	ctx := context.Background()
	if msgId, err := producer.Send(ctx, &pulsar.ProducerMessage{
		Payload: []byte("delay message"),
        DeliverAfter: 10 * time.Second,
	}); err != nil {
		log.Fatal(err)
	} else {
        log.Printf("Published message: %v, -- deliver after: 10 seconds", msgId)
	}

    // consume
    for {
        msgs, err := consumer.Pop(1, 15000)
        if err != nil {
            log.Println(err)
            continue
        }
        log.Printf("Received message msgId: %#v -- content: '%s'\n",
            msgs[0].ID(), string(msgs[0].Payload()))
        consumer.Ack(msgs[0])
        break
    }
}

