//
// mqtt-record.go - tools for recording from and playing back to MQTT topics.
//
// How to:
//   $ mkdir -p ~/work/
//   $ cd ~/work/
//   $ git clone https://github.com/yoggy/mqtt-record-replay.git
//   $ cd mqtt-record-replay
//   $ go get -u github.com/eclipse/paho.mqtt.golang
//   $ go get -u github.com/vmihailenco/msgpack
//   $ go build mqtt-record.go
//   $ go build mqtt-replay.go
//
//   $ ./mqtt-record
//   usage: mqtt-record.exe url subscribe_topic record_filename
//
//   example:
//
//       $ mqtt-record.exe tcp://iot.exlipse.org:1883 "test/record/topic/#" record.mqtt
//
//
//   $ ./mqtt-replay
//   usage: mqtt-replay.exe recording_filename url
//
//    example:
//
//        $ mqtt-replay.exe recording.mqtt tcp://iot.exlipse.org:1883
//
// License:
//   Copyright (c) 2018 yoggy <yoggy0@gmail.com>
//   Released under the MIT license
//   http://opensource.org/licenses/mit-license.php;
//
package main

import (
	"encoding/binary"
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	msgpack "github.com/vmihailenco/msgpack"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var file *os.File
var wg = &sync.WaitGroup{}

func usage() {
	fmt.Printf("usage: %s url subscribe_topic recording_filename \n", os.Args[0])
	fmt.Printf("\n")
	fmt.Printf("example:\n")
	fmt.Printf("\n")
	fmt.Printf("    $ %s tcp://iot.exlipse.org:1883 \"test/record/topic/#\" recording.mqtt\n", os.Args[0])
	fmt.Printf("\n")
	os.Exit(0)
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
	if len(os.Args) != 4 {
		usage()
	}

	wg.Add(1)
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)
	go func() {
		for sig := range c {
			fmt.Println("", sig)
			wg.Done()
		}
	}()

	mqtt_record(os.Args[1], os.Args[2], os.Args[3])

	os.Exit(0)
}
