package main

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math/rand"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	"github.com/riferrei/srclient"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

const schemaFile string = "SensorReading.proto"

func main() {

	props := LoadProperties()
	CreateTopic(props)
	topic := TopicName

	schemaRegistryClient := srclient.CreateSchemaRegistryClient(props["schema.registry.url"])
	schemaRegistryClient.CodecCreationEnabled(false)
	srBasicAuthUserInfo := props["schema.registry.basic.auth.user.info"]
	credentials := strings.Split(srBasicAuthUserInfo, ":")
	schemaRegistryClient.SetCredentials(credentials[0], credentials[1])

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": props["bootstrap.servers"],
		"sasl.mechanisms":   "PLAIN",
		"security.protocol": "SASL_SSL",
		"sasl.username":     props["sasl.username"],
		"sasl.password":     props["sasl.password"]})
	if err != nil {
		panic(fmt.Sprintf("Failed to create producer %s", err))
	}
	defer producer.Close()

	go func() {
		for event := range producer.Events() {
			switch ev := event.(type) {
			case *kafka.Message:
				message := ev
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Error delivering the order '%s'\n", message.Key)
				} else {
					fmt.Printf("Reading sent to the partition %d with offset %d. \n",
						message.TopicPartition.Partition, message.TopicPartition.Offset)
				}
			}
		}
	}()

	schema, err := schemaRegistryClient.GetLatestSchema(topic, false)
	if schema == nil {
		schemaBytes, _ := ioutil.ReadFile(schemaFile)
		schema, err = schemaRegistryClient.CreateSchema(topic, string(schemaBytes), false)
		if err != nil {
			panic(fmt.Sprintf("Error creating the schema %s", err))
		}
	}

	devices := []*SensorReading_Device{}
	d1 := new(SensorReading_Device)
	deviceID, _ := uuid.NewUUID()
	d1.DeviceID = deviceID.String()
	d1.Enabled = true
	devices = append(devices, d1)

	d2 := new(SensorReading_Device)
	deviceID, _ = uuid.NewUUID()
	d2.DeviceID = deviceID.String()
	d2.Enabled = true
	devices = append(devices, d2)

	d3 := new(SensorReading_Device)
	deviceID, _ = uuid.NewUUID()
	d3.DeviceID = deviceID.String()
	d3.Enabled = true
	devices = append(devices, d3)

	d4 := new(SensorReading_Device)
	deviceID, _ = uuid.NewUUID()
	d4.DeviceID = deviceID.String()
	d4.Enabled = true
	devices = append(devices, d4)

	d5 := new(SensorReading_Device)
	deviceID, _ = uuid.NewUUID()
	d5.DeviceID = deviceID.String()
	d5.Enabled = true
	devices = append(devices, d5)

	for {

		deviceSelected := devices[len(devices)-1]

		// Create key and value
		key := deviceSelected.DeviceID
		sensorReading := SensorReading{
			Device:   deviceSelected,
			DateTime: time.Now().UnixNano(),
			Reading:  rand.Float64(),
		}
		valueBytes, _ := proto.Marshal(&sensorReading)

		// Serialize the record value
		schemaIDBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(schemaIDBytes, uint32(schema.ID()))
		var recordValue []byte
		recordValue = append(recordValue, byte(0))
		recordValue = append(recordValue, schemaIDBytes...)
		recordValue = append(recordValue, valueBytes...)

		// Produce the record to the topic
		producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic: &topic, Partition: kafka.PartitionAny},
			Key: []byte(key), Value: recordValue}, nil)

		// Sleep for one second...
		time.Sleep(100 * time.Millisecond)

	}

}
