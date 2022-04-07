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
	"io/ioutil"
	"log"
	"os"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	msgpack "github.com/vmihailenco/msgpack/v5"
)

const buildVersion string = "v2.0.0-alpha"

// configuration values
var verbosity int
var brokerURL string
var filename string
var startTimeSec uint
var endTimeSec uint // end time of 0 seconds doesn't make sense, so use it for "full file"

func init() {
	flag.IntVar(&verbosity, "v", 1, "verbosity level: off (0), info (1), debug (2)")

	flag.StringVar(&brokerURL, "b", "tcp://localhost:1883", "MQTT broker URL")
	flag.StringVar(&filename, "i", "", "Input file")
	flag.UintVar(&startTimeSec, "s", 0, "Starting time offset (seconds)")
	flag.UintVar(&endTimeSec, "e", 0, "End time (seconds, leave out for full file)")
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

func readEntry(file *os.File) (MqttMessage, int64) {
	// read payload size entry
	buf := make([]byte, binary.MaxVarintLen64)
	_, err := file.Read(buf)
	if err != nil {
		return MqttMessage{}, -1 // EOF reached
	}
	payload_size, _ := binary.Varint(buf)

	// read payload buffer
	payload_buf := make([]byte, payload_size)
	_, err = file.Read(payload_buf)
	if err != nil {
		return MqttMessage{}, -1 // EOF reached
	}

	// unpack message
	var msg MqttMessage
	err = msgpack.Unmarshal(payload_buf, &msg)
	if err != nil {
		log.Fatalln("Fatal error unpacking packet in recording file")
	}

	return msg, payload_size
}

func publish(client mqtt.Client, msg MqttMessage) {
	token := client.Publish(msg.Topic, byte(0), false, msg.Payload)
	token.Wait()
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

	if verbosity < 1 {
		log.SetFlags(0)
		log.SetOutput(ioutil.Discard)
	}

	// try opening file for reading
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalln("Error opening file for reading:", err)
	}
	defer file.Close()

	// try connecting to MQTT broker
	opts := mqtt.NewClientOptions()
	opts.AddBroker(brokerURL)

	client := mqtt.NewClient(opts)
	defer client.Disconnect(100)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Panicln("Error connecting to MQTT broker:", token.Error())
	}
	if verbosity > 1 {
		log.Println("Success connecting to MQTT broker")
	}

	//
	// process recording file
	//
	endTimeAvailable := endTimeSec > 0

	// get first entry in recording file
	msg, len := readEntry(file)
	recordingStartTime := msg.Millis // timestamp of first entry in file

	var firstMsgMilis int64
	var firstMsgWallclock int64

	// fast forward to message at requested start time
	for {
		if msg.Millis-recordingStartTime >= int64(startTimeSec*1000) {
			log.Printf("t=%6.2f s, %6d bytes, topic=%s\n", float32(msg.Millis-recordingStartTime)/1000.0, len, msg.Topic)
			publish(client, msg)

			firstMsgMilis = msg.Millis
			firstMsgWallclock = nowMillis()

			break
		}

		msg, len = readEntry(file) // skip to next message
	}

	// play messages until requested end time
	for {
		msg, len = readEntry(file)
		if len < 0 {
			log.Println("End of recording reached")
			break
		}

		// check requested end time
		if endTimeAvailable && msg.Millis-recordingStartTime > int64(endTimeSec*1000) {
			log.Println("Requested end time reached")
			break
		}

		// wait for target time to be reached
		targetWallclock := firstMsgWallclock + (msg.Millis - firstMsgMilis)
		for {
			if nowMillis() >= targetWallclock {
				log.Printf("t=%6.2f s, %6d bytes, topic=%s\n", float32(msg.Millis-recordingStartTime)/1000.0, len, msg.Topic)
				publish(client, msg)
				break
			}

			time.Sleep(200 * time.Microsecond)
		}
	}

	log.Println("Replay finished")
}
