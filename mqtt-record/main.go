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
	"io/ioutil"
	"log"
	"os"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	msgpack "github.com/vmihailenco/msgpack/v5"
)

const buildVersion string = "v2.0.0-alpha"

// global variables
var file *os.File
var msgCnt uint

// configuration values
var verbosity int
var brokerURL string
var topic string
var filename string

const msgStatsTime int = 5 // report statistics every 5 seconds

func init() {
	autoFileName := "recording-" + time.Now().Format("2006-01-02T150405") + ".mqtt"

	flag.IntVar(&verbosity, "v", 1, "verbosity level: off (0), info (1), debug (2)")

	flag.StringVar(&brokerURL, "b", "tcp://localhost:1883", "MQTT broker URL")
	flag.StringVar(&topic, "t", "#", "MQTT topic to subscribe")
	flag.StringVar(&filename, "o", autoFileName, "Output file name")
	flag.Parse()
}

func nowMillis() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

type MqttMessage struct {
	Millis  int64
	Topic   string
	Payload []byte
}

var message_handler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	t := nowMillis()
	topic := msg.Topic()
	payload := msg.Payload()
	msgCnt++

	buf_payload, err := msgpack.Marshal(&MqttMessage{Millis: t, Topic: topic, Payload: payload})
	if err != nil {
		log.Fatalln("Error creating packet:", err)
	}

	size := int64(len(buf_payload))
	if verbosity > 1 {
		log.Printf("t=%d, %6d bytes, topic=%s\n", t, size, topic)
	}

	buf_size := make([]byte, binary.MaxVarintLen64)
	binary.PutVarint(buf_size, size)

	file.Write(buf_size)
	file.Write(buf_payload)
}

func main() {
	fmt.Println("MQTT Recorder " + buildVersion)
	fmt.Println("- MQTT broker:     ", brokerURL)
	fmt.Println("- Subscribe topic: ", topic)
	fmt.Println("- Output filename: ", filename)
	fmt.Println("")

	if verbosity < 1 {
		log.SetFlags(0)
		log.SetOutput(ioutil.Discard)
	}

	// try opening file for writing
	var err error
	file, err = os.Create(filename)
	if err != nil {
		log.Fatalln("Error opening file for writing:", err)
	}
	defer file.Close()

	// subscribe to MQTT and write recording
	opts := mqtt.NewClientOptions()
	opts.AddBroker(brokerURL)
	opts.SetDefaultPublishHandler(message_handler)

	client := mqtt.NewClient(opts)
	defer client.Disconnect(100)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalln("Error connecting to MQTT broker:", token.Error())
	}
	if verbosity > 1 {
		log.Println("Success connecting to MQTT broker")
	}

	if token := client.Subscribe(topic, 0, nil); token.Wait() && token.Error() != nil {
		log.Fatalln("Error subscribing to MQTT topic:", token.Error())
	}
	if verbosity > 1 {
		log.Println("Success subscribing to topic")
	}

	for {
		if verbosity == 1 {
			log.Printf("Recorded %4d messages in last %d sec.", msgCnt, msgStatsTime)
		}
		msgCnt = 0

		time.Sleep(time.Duration(msgStatsTime) * time.Second)
	}
}
