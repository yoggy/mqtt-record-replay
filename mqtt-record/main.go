//
// mqtt-record.go - tools for recording from and playing back to MQTT topics.
//
//
// License:
//   Copyright (c) 2018 yoggy <yoggy0@gmail.com>
//   Copyright (c) 2022 Jannik Beyerstedt <beyerstedt@consider-it.de>
//   Released under the MIT license
//   http://opensource.org/licenses/mit-license.php;
//
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	msgpack "github.com/vmihailenco/msgpack/v5"
)

const buildVersion string = "v2.0.0-alpha"

// global variables
var file *os.File
var wg = &sync.WaitGroup{}

// configuration values
var brokerURL string
var topic string
var filename string

func init() {
	autoFileName := "recording-" + time.Now().Format("2006-01-02T150405") + ".mqtt"

	flag.StringVar(&brokerURL, "b", "tcp://localhost:1883", "MQTT broker URL")
	flag.StringVar(&topic, "t", "#", "MQTT topic to subscribe")
	flag.StringVar(&filename, "o", autoFileName, "Output file name")
	flag.Parse()
}

func millis() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

type MqttMessage struct {
	Millis  int64
	Topic   string
	Payload []byte
}

var message_handler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	t := millis()
	topic := msg.Topic()
	payload := msg.Payload()

	buf_payload, err := msgpack.Marshal(&MqttMessage{Millis: t, Topic: topic, Payload: payload})
	if err != nil {
		panic(err)
	}

	size := int64(len(buf_payload))
	fmt.Printf("t=%d, topic=%s, size=%d\n", t, topic, size)
	//fmt.Printf("%+v\n", b)

	buf_size := make([]byte, binary.MaxVarintLen64)
	binary.PutVarint(buf_size, size)
	//s,_ := binary.Varint(buf_size)

	file.Write(buf_size)
	file.Write(buf_payload)
	file.Sync()
}

func mqtt_record(url, topic, filename string) {
	f, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	file = f
	defer f.Close()

	opts := mqtt.NewClientOptions()
	opts.AddBroker(url)
	opts.SetDefaultPublishHandler(message_handler)

	client := mqtt.NewClient(opts)
	defer client.Disconnect(100)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	fmt.Printf("mqtt_record() : subscribe start...topic=%s\n", topic)
	if token := client.Subscribe(topic, 0, nil); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	wg.Wait()
	fmt.Println("mqtt_record() : leave...")
}

func main() {
	fmt.Println("MQTT Recorder " + buildVersion)
	fmt.Println("- MQTT broker:     ", brokerURL)
	fmt.Println("- Subscribe topic: ", topic)
	fmt.Println("- Output filename: ", filename)
	fmt.Println("")

	wg.Add(1)
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)
	go func() {
		for sig := range c {
			fmt.Println("", sig)
			wg.Done()
		}
	}()

	mqtt_record(brokerURL, topic, filename)

	os.Exit(0)
}
