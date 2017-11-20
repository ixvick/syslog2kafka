package main

import (
	//"fmt"
	"log"
	"time"
	"flag"
	"os"
	"strings"
        "github.com/Shopify/sarama"
	"gopkg.in/mcuadros/go-syslog.v2"
)

var (
        brokers = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The Kafka brokers to connect to, as a comma separated list")
	flush = flag.Int("flush", 500, "Flush messages to kafka every X ms")
	topic = flag.String("topic", "syslog_queue", "Kafka topic name")
	listen = flag.String("udp", "0.0.0.0:514", "Syslog udp listen")
	threads = flag.Int("threads", 4, "Main threads count")
	buffer = flag.Int("buffer", 32, "Syslog messages buffer size")
)

func main() {
        flag.Parse()
        brokerList := strings.Split(*brokers, ",")
        // Init Syslog
	channel := make(syslog.LogPartsChannel, *buffer)
	handler := syslog.NewChannelHandler(channel)

	server := syslog.NewServer()
	//server.SetFormat(syslog.RFC5424)
	server.SetFormat(syslog.Automatic)
	server.SetHandler(handler)
	server.ListenUDP(*listen)

	server.Boot()

        // Init Kafka
        config := sarama.NewConfig()
        //config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
        //config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
        //config.Producer.Return.Successes = true

        //producer, err := sarama.NewSyncProducer(brokerList, config)
        //if err != nil {
        //        log.Fatalln("Failed to start Sarama producer:", err)
        //}

        config.Producer.RequiredAcks = sarama.WaitForLocal       // Only wait for the leader to ack
        config.Producer.Compression = sarama.CompressionSnappy   // Compress messages
        config.Producer.Flush.Frequency = time.Duration(*flush) * time.Millisecond

        producer, err := sarama.NewAsyncProducer(brokerList, config)
        if err != nil {
              log.Fatalln("Failed to start Sarama producer:", err)
        }

        go func() {
              for err := range producer.Errors() {
                  log.Println("Failed to write access log entry:", err)
              }
        }()

        // Syslog reader
	processor := func(channel syslog.LogPartsChannel) {
		for logParts := range channel {
		        line := logParts["content"].(string)
                        producer.Input() <- &sarama.ProducerMessage{
			      Topic: *topic,
			      Value: sarama.StringEncoder(line),
			}
			//fmt.Println(line)
		}
	}

	for i := 1; i <= *threads; i++ {
	    go processor(channel)
	}

	server.Wait()
}
