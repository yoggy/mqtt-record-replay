//
// mqtt-replay.go - tools for recording from and playing back to MQTT topics.
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
//        $ mqtt-replay.exe recording.mqtt tcp://iot.eclipse.org:1883
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
	"os"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	msgpack "github.com/vmihailenco/msgpack"
)

func usage() {
	fmt.Printf("usage: %s recording_filename url \n", os.Args[0])
	fmt.Printf("\n")
	fmt.Printf("example:\n")
	fmt.Printf("\n")
	fmt.Printf("    $ %s recording.mqtt tcp://iot.exlipse.org:1883\n", os.Args[0])
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

func readPayloadSize(f *os.File) int64 {
	buf := make([]byte, binary.MaxVarintLen64)
	_, err := f.Read(buf)
	if err != nil {
		return -1
	}
	payload_size, _ := binary.Varint(buf)
	return payload_size
}

func readPayload(f *os.File, size int64) []byte {
	buf := make([]byte, size)
	_, err := f.Read(buf)
	if err != nil {
		return nil
	}
	return buf
}

func mqtt_replay(filename, url string) {
	f, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	opts := mqtt.NewClientOptions()
	opts.AddBroker(url)

	client := mqtt.NewClient(opts)
	defer client.Disconnect(100)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	fmt.Printf("mqtt_replay() : start replay...file=%s, url=%s\n", filename, url)

	t0 := int64(0)
	t1 := int64(0)

	for {
		payload_size := readPayloadSize(f)
		if payload_size == -1 {
			break
		}

		payload_buf := readPayload(f, payload_size)
		if payload_buf == nil {
			break
		}

		var msg MqttMessage
		err = msgpack.Unmarshal(payload_buf, &msg)
		if err != nil {
			panic(err)
		}

		if t0 > 0 {
			// spin lock
			for {
				if (millis() - t0) >= (msg.Millis - t1) {
					break
				}
			}
		}
		fmt.Printf("mqtt_replay() : t=%d topic=%s, payload_size=%d\n", msg.Millis, msg.Topic, payload_size)
		t0 = millis()
		t1 = msg.Millis

		token := client.Publish(msg.Topic, byte(0), false, msg.Payload)
		token.Wait()
	}

	fmt.Println("mqtt_record() : finish...")
}

func main() {
	if len(os.Args) != 3 {
		usage()
	}

	mqtt_replay(os.Args[1], os.Args[2])
}
