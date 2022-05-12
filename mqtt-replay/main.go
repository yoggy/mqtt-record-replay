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
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	msgpack "github.com/vmihailenco/msgpack/v5"
	"golang.org/x/term"
)

const buildVersion string = "v2.0.0"

// configuration values
const skipSeconds int = 5

var verbosity int
var brokerURL string
var filename string
var startTimeSec uint
var endTimeSec uint // end time of 0 seconds doesn't make sense, so use it for "full file"

// internal state
var shouldHalt bool
var shouldExit bool

func init() {
	flag.IntVar(&verbosity, "v", 1, "verbosity level: off (0), info (1), debug (2)")

	flag.StringVar(&brokerURL, "b", "tcp://localhost:1883", "MQTT broker URL")
	flag.StringVar(&filename, "i", "", "Input file (REQUIRED)")
	flag.UintVar(&startTimeSec, "s", 0, "Starting time offset (seconds)")
	flag.UintVar(&endTimeSec, "e", 0, "End time (seconds, leave out for full file)")
	flag.Parse()

	if filename == "" {
		println("ERROR: Input file name not set!")
		println("Usage:")
		flag.PrintDefaults()
		os.Exit(1)
	}

	shouldHalt = false
	shouldExit = false
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

type Playback struct {
	File   *os.File
	Client mqtt.Client

	// internal playback state
	endTimeAvailable   bool
	endTimeMillis      int64
	recordingStartTime int64 // timestamp of first entry in file

	firstMsgMillis    int64
	firstMsgWallclock int64
	msgMillisRelative int64 // current playback position
	haltOffsetMillis  int64

	haltStartWallclock int64
}

func (p *Playback) Init(endTimeSec uint) {
	p.endTimeAvailable = endTimeSec > 0
	p.endTimeMillis = int64(endTimeSec) * 1000
}

func (p *Playback) PlayFrom(startTimeMillis uint) {
	// reset to file start when skipping backwards
	if int64(startTimeMillis) < p.msgMillisRelative {
		_, err := p.File.Seek(0, 0)
		if err != nil {
			log.Fatalln("Error selecting file start")
		}
	}

	// search for (new) start message when playback position has changed
	if startTimeMillis == 0 || int64(startTimeMillis) != p.msgMillisRelative {
		p.haltOffsetMillis = 0

		// get first entry in recording file
		msg, len := readEntry(p.File)
		if len < 0 {
			log.Println("End of recording reached")
			return
		}
		if p.recordingStartTime == 0 { // only set for very first call
			p.recordingStartTime = msg.Millis // timestamp of first entry in file
		}

		// fast forward to message at requested start time
		for {
			p.msgMillisRelative = msg.Millis - p.recordingStartTime
			if p.msgMillisRelative >= int64(startTimeMillis) {
				log.Printf("t=%6.2f s, %6d bytes, topic=%s\n", float32(p.msgMillisRelative)/1000.0, len, msg.Topic)
				publish(p.Client, msg)

				p.firstMsgMillis = msg.Millis
				p.firstMsgWallclock = nowMillis()

				break
			}

			msg, len = readEntry(p.File) // not at start time yet, skip to next message
			if len < 0 {
				log.Println("End of recording reached")
				return
			}
		}

	} else {
		// just re-start playing otherwise
		p.haltOffsetMillis = nowMillis() - p.haltStartWallclock
	}
}

func (p *Playback) SkipAndPlay(relativePlayPositionSec int) {
	currentPositionMillis := p.msgMillisRelative
	targetPositionMillis := currentPositionMillis + int64(relativePlayPositionSec*1000)
	if targetPositionMillis < 0 {
		targetPositionMillis = 0
	}

	p.PlayFrom(uint(targetPositionMillis * 1000))
}

func (p *Playback) PlayNextMessage() bool {
	msg, len := readEntry(p.File)
	if len < 0 {
		log.Println("End of recording reached")
		return false
	}

	p.msgMillisRelative = msg.Millis - p.recordingStartTime

	// check requested end time
	if p.endTimeAvailable && p.msgMillisRelative > p.endTimeMillis {
		log.Println("Requested end time reached")
		return false
	}

	// wait for target time to be reached
	targetWallclock := p.firstMsgWallclock + (msg.Millis - p.firstMsgMillis) + p.haltOffsetMillis
	for {
		if nowMillis() >= targetWallclock {
			log.Printf("t=%6.2f s, %6d bytes, topic=%s\n", float32(p.msgMillisRelative)/1000.0, len, msg.Topic)
			publish(p.Client, msg)
			break
		}

		time.Sleep(200 * time.Microsecond)
	}

	return true // still messages left
}

func (p *Playback) Pause() {
	p.haltStartWallclock = nowMillis()
}

const KEY_SIGINT string = "SIGINT"
const KEY_UP string = "UP"
const KEY_DOWN string = "DOWN"
const KEY_RIGHT string = "RIGHT"
const KEY_LEFT string = "LEFT"

func readKeypress() (string, error) {
	const ETX = '\x03' // ^C

	// switch stdin into "raw" mode to get key presses without need for a newline
	oldState, err := term.MakeRaw(0)
	if err != nil {
		return "", err
	}
	defer term.Restore(0, oldState)

	// read text from terminal
	readBuf := make([]byte, 3)
	numRead, err := os.Stdin.Read(readBuf) // this is blocking
	if err != nil {
		return "", err
	}

	if readBuf[0] == ETX {
		return KEY_SIGINT, nil
	}

	// Three-character control sequence, beginning with "ESC-[".
	if numRead == 3 && readBuf[0] == 27 && readBuf[1] == 91 {
		if readBuf[2] == 65 {
			return KEY_UP, nil
		} else if readBuf[2] == 66 {
			return KEY_DOWN, nil
		} else if readBuf[2] == 67 {
			return KEY_RIGHT, nil
		} else if readBuf[2] == 68 {
			return KEY_LEFT, nil
		}
	} else if numRead == 1 {
		return string(readBuf[0]), nil
	}
	return "", errors.New("unknown key sequence")
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

	// capture some signals
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for sig := range c {
			if sig == os.Interrupt {
				if shouldHalt { // second SIGINT -> exit
					shouldExit = true
				} else { // first SIGINT -> just halt
					shouldHalt = true
				}
			}
		}
	}()

	//
	// process recording file
	//
	var playControl Playback
	playControl.File = file
	playControl.Client = client

	playControl.Init(endTimeSec)
	playControl.PlayFrom(startTimeSec * 1000)

	messagesLeft := true
	for messagesLeft && !shouldExit {
		for shouldHalt {
			playControl.Pause()

			key, err := readKeypress() // blocking
			if err != nil {
				log.Fatalln("Error reading key: ", err)
				break
			}
			if key == KEY_SIGINT {
				log.Println("Exit requested")
				os.Exit(0)
			}

			if key == KEY_RIGHT {
				playControl.SkipAndPlay(skipSeconds)
				shouldHalt = false
				break

			} else if key == KEY_LEFT {
				playControl.SkipAndPlay(-skipSeconds)
				shouldHalt = false
				break

			} else if key == KEY_UP {
				playControl.PlayFrom(startTimeSec * 1000)
				shouldHalt = false
				break

			} else if key == " " {
				playControl.SkipAndPlay(0)
				shouldHalt = false
				break

			} else {
				fmt.Println("Unknown key, use:")
				fmt.Println("  <space> to play again")
				fmt.Println("  <right arrow> to skip forwards")
				fmt.Println("  <left arrow>  to skip backwards")
				fmt.Println("  <up arrow>    to start from beginning")
			}
		}

		messagesLeft = playControl.PlayNextMessage()
	}

	log.Println("Replay finished")
}
