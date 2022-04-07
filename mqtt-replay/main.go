//
// mqtt-replay.go - tools for recording from and playing back to MQTT topics.
//
//
// License:
//   Copyright (c) 2018 yoggy <yoggy0@gmail.com>
//   Copyright (c) 2021 Bendix Buchheister <buchheister@consider-it.de>
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
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	msgpack "github.com/vmihailenco/msgpack"
)

const buildVersion string = "v2.0.0-alpha"

// configuration values
var brokerURL string
var filename string
var startTimeSec uint
var endTimeSec uint // end time of 0 seconds doesn't make sense, so use it for "full file"

func init() {
	flag.StringVar(&brokerURL, "b", "tcp://localhost:1883", "MQTT broker URL")
	flag.StringVar(&filename, "i", "", "Input file")
	flag.UintVar(&startTimeSec, "s", 0, "Starting time offset (seconds)")
	flag.UintVar(&endTimeSec, "e", 0, "End time (seconds, leave out for full file)")
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

func mqtt_replay(filename, url string, startTimeMillis int64, stopTimeMillis int64) {
	f, err := os.Open(filename)
	if err != nil {
		fmt.Println("Input file could not be opened!")
		panic(err)
	}
	defer f.Close()

	opts := mqtt.NewClientOptions()
	opts.AddBroker(url)

	client := mqtt.NewClient(opts)
	defer client.Disconnect(100)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		fmt.Println("Connection to MQTT broker failed!")
		panic(token.Error())
	}

	fmt.Printf("mqtt_replay() : start replay...file=%s, url=%s\n", filename, url)

	firstMsg := true
	startTime := millis()
	t0 := int64(0)
	t1 := startTimeMillis
	hasStopTime := stopTimeMillis > 0

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
			fmt.Println("Fatal error unpacking packet in recording file")
			panic(err)
		}

		if firstMsg {
			firstMsg = false
			stopTimeMillis = stopTimeMillis - startTimeMillis
			startTimeMillis += msg.Millis
		}

		if msg.Millis >= startTimeMillis {

			if (hasStopTime) && ((millis() - startTime) > (stopTimeMillis)) {
				fmt.Println("TODO: stopping file")
				break
			}

			if t0 > 0 {
				// spin lock
				for {
					if (millis() - t0) >= (msg.Millis - t1) {
						break
					}
					time.Sleep(200 * time.Microsecond)
				}
			}
			fmt.Printf("mqtt_replay() : t=%d topic=%s, payload_size=%d\n", msg.Millis, msg.Topic, payload_size)
			t0 = millis()
			t1 = msg.Millis

			token := client.Publish(msg.Topic, byte(0), false, msg.Payload)
			token.Wait()
		}
	}

	fmt.Println("mqtt_record() : finish...")
}

func main() {
	fmt.Println("MQTT Recording Replay " + buildVersion)
	fmt.Println("- MQTT broker:     ", brokerURL)
	fmt.Println("- Input filename:  ", filename)
	if endTimeSec > 0 {
		fmt.Println("- Interval:        ", startTimeSec, "-", endTimeSec, "sec.")
	} else if startTimeSec > 0 {
		fmt.Println("- Start time:      ", startTimeSec, "sec.")
	}
	fmt.Println("")

	var startMillis = int64(startTimeSec) * 1000
	var stopMillis = int64(endTimeSec) * 1000

	mqtt_replay(filename, brokerURL, startMillis, stopMillis)
}
