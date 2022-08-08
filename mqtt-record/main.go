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
	"os/signal"
	"sort"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	msgpack "github.com/vmihailenco/msgpack/v5"
)

const buildVersion string = "v2.0.0"

// global variables
var file *os.File
var msgCnt uint

// configuration values
var verbosity int
var brokerURL string
var topic string
var filename string
var statsOutput bool

const msgStatsTime int = 5 // report statistics every 5 seconds

// message statistics store
type StatValues struct {
	initialized bool
	min         uint64
	avg         float64
	max         uint64
}

func (stats *StatValues) updateTimeDiffStats(currentValue uint64, numMsgs uint64) {
	if !stats.initialized { // initial condition
		stats.min = currentValue
		stats.max = currentValue
		stats.initialized = true
	}

	stats.avg = float64(stats.avg)*float64(numMsgs-1)/float64(numMsgs) + float64(currentValue)/float64(numMsgs)

	if currentValue < stats.min {
		stats.min = currentValue
	}

	if currentValue > stats.max {
		stats.max = currentValue
	}
}

type MsgStats struct {
	LastMsgMillis  uint64
	NumMsgs        uint64
	TimeDiffMillis StatValues
	MsgSizeByte    StatValues
}

var msgStats map[string]MsgStats

func init() {
	flag.IntVar(&verbosity, "v", 1, "verbosity level: off (0), info (1), debug (2)")

	flag.StringVar(&brokerURL, "b", "tcp://localhost:1883", "MQTT broker URL")
	flag.StringVar(&topic, "t", "#", "MQTT topic to subscribe")
	flag.StringVar(&filename, "o", "recording-$topic-$time.mqtt", "Output file name")
	flag.BoolVar(&statsOutput, "s", false, "Print regular message statistics per topic")
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

	// calculate message statistics (omitting first message)
	stats, exists := msgStats[topic]
	if !exists {
		var initStats = MsgStats{}
		initStats.NumMsgs = 0
		initStats.MsgSizeByte = StatValues{false, 0, 0, 0}
		initStats.TimeDiffMillis = StatValues{false, 0, 0, 0}
		initStats.LastMsgMillis = uint64(t)

		msgStats[topic] = initStats
	} else {
		stats.NumMsgs++

		if size < 0 {
			fmt.Println(topic, "low size")
		}

		var timeDiff = uint64(t) - stats.LastMsgMillis
		stats.TimeDiffMillis.updateTimeDiffStats(timeDiff, stats.NumMsgs)
		stats.MsgSizeByte.updateTimeDiffStats(uint64(size), stats.NumMsgs)

		stats.LastMsgMillis = uint64(t)
		msgStats[topic] = stats
	}
}

func main() {
	var filenameSafeTopic = strings.ReplaceAll(topic, "/", "_")
	var filenameTimestamp = time.Now().Format("2006-01-02T150405")

	filename = strings.Replace(filename, "$topic", filenameSafeTopic, 1)
	filename = strings.Replace(filename, "$time", filenameTimestamp, 1)

	fmt.Println("MQTT Recorder " + buildVersion)
	fmt.Println("- MQTT broker:     ", brokerURL)
	fmt.Println("- Subscribe topic: ", topic)
	fmt.Println("- Output filename: ", filename)
	fmt.Println("")

	if verbosity < 1 {
		log.SetFlags(0)
		log.SetOutput(ioutil.Discard)
	}

	msgStats = make(map[string]MsgStats)

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

	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, os.Interrupt)
	go func() {
		for range signalChannel {
			fmt.Println("Message Statistics by Topic:")
			printMsgStats(msgStats)

			os.Exit(0)
		}
	}()

	for {
		time.Sleep(time.Duration(msgStatsTime) * time.Second)

		if verbosity == 1 {
			log.Printf("Recorded %4d messages in last %d sec.", msgCnt, msgStatsTime)

			if statsOutput {
				printMsgStats(msgStats)
			}
		}
		msgCnt = 0
	}
}

func printMsgStats(stats map[string]MsgStats) {
	// sort alphabetically by key
	keys := make([]string, 0, len(stats))
	for k := range stats {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, topic := range keys {
		var stat = stats[topic]
		fmt.Printf("%-25s: %5d msg, %6d/%6.0f/%6d byte, %4d/%4.0f/%4d ms delta (min/avg/max)\n", topic, stat.NumMsgs, stat.MsgSizeByte.min, stat.MsgSizeByte.avg, stat.MsgSizeByte.max, stat.TimeDiffMillis.min, stat.TimeDiffMillis.avg, stat.TimeDiffMillis.max)
	}
}
