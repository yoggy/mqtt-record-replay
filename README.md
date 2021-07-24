mqtt-record-replay
====

tools for recording from and playing back to MQTT topics.

![img01.png](img01.png)

How to
====
Setup your `GOPATH` and/or `GOBIN` beforehand to be listed in your `PATH` environment variable.
Then clone this repository and build and install the executables to your system.

    $ mkdir -p ~/work/
    $ cd ~/work/
    $ git clone https://github.com/yoggy/mqtt-record-replay.git
    $ cd mqtt-record-replay
    $ go install ./...
 
    $ mqtt-record
    usage: mqtt-record.exe url subscribe_topic record_filename
 
    example:
 
        $ mqtt-record.exe tcp://iot.eclipse.org:1883 "test/record/topic/#" record.mqtt
 
 
    $ mqtt-replay
    usage: mqtt-replay.exe recording_filename url
 
     example:
 
         $ mqtt-replay.exe recording.mqtt tcp://iot.eclipse.org:1883

Copyright and license
----
Copyright (c) 2018 yoggy

Released under the [MIT license](LICENSE.txt)

