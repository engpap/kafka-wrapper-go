# Kafka Wrapper Go
## Description
This package is a wrapper for the Kafka library in Go [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go). It provides APIs to interact with Kafka topics, producers, and consumers. It also provides a fault tolerance mechanism to handle Kafka connection issues.

## Installation
```bash
go get github.com/engpap/kafka-wrapper-go@v<x.y.z>
```
Substitute `<x.y.z>` with the version you want to install. See the releases page for the available versions.

## Usage
First of all import the package:
```go
import (
   kafkaWrapper "github.com/engpap/kafka-wrapper-go/pkg"
)
```

### Producer
To create a producer, use the `CreateProducer` function. This function returns a `kafka.Producer` object and an error. If the producer is created successfully, the error will be `nil`. Otherwise, the error will contain the error message.
```go
producer, err := kafkaWrapper.CreateProducer()
if err != nil {
    panic(err)
}
kafkaWrapper.SetupCloseProducerHandler(producer)
```
To produce a message, use the `ProduceMessage` function. This function takes the producer object, the topic name, the message key, and the message value. It returns an error. If the message is produced successfully, the error will be `nil`. Otherwise, the error will contain the error message.
```go
err := kafkaWrapper.ProduceMessage(producer, "<action_type>", "<topic_name>", data)
if err != nil {
    ...
    return
}
// wait for all messages to be acknowledged
producer.Flush(15 * 1000)

```
Where `data` can be any data structure that can be marshalled into a JSON string. For example, an instance of the following struct can be used as `data`:
```go
type Project struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	CourseID string `json:"course_id"`
}
```

### Consumer
To create a consumer, use the `CreateConsumer` function. This function takes the topic name, the callback function, and the microservice name.
```go
go kafkaWrapper.CreateConsumer("<topic-name>", callbackFunction, "<microservice-name>")
```
The microservice name is used to configure the `group.id` of the consumer. Each microservice should have a unique `group.id`. This will allow to have multiple consumers listening to the same topic without losing messages.

### Callback Function
It's a function of the following type:
```go
type MessageHandler func(action_type string, message interface{})
```
The callback function is called whenever a message is received from the Kafka topic. The function takes two arguments:
- `action_type`
- `message`

### Message
Message is an interface that can be any data structure. It is the data that is received from the Kafka topic. The data can be unmarshalled into a struct or a map based on the data structure.

For example, a callback function that saves a course in memory can be defined as follows:
```go
func (c *Controller) SaveCourseInMemory(data interface{}) {
	if projectMap, ok := data.(map[string]interface{}); ok {
		project := models.Project{
			ID:       fmt.Sprint(projectMap["id"]),
			Name:     fmt.Sprint(projectMap["name"]),
			CourseID: fmt.Sprint(projectMap["course_id"]),
		}
		c.Projects = append(c.Projects, project)
		fmt.Println("In-Memory Projects: ", c.Projects)
	} else {
		fmt.Printf("Error: data cannot be converted to Project\n")
	}
}
```     

### Action Type
The action type is a string that is used to identify the type of message. This is useful when you have multiple consumers listening to the same topic. The action type can be used to filter messages based on the type of action that needs to be taken.
It can be one of the following:
- `add`
- `delete`



# Instructions for developers
## Initialize version of the package
```bash
git tag v1.0.0
git push origin v1.0.0
```

## Update version of the package
```bash
git tag -a v1.0.1 -m "Refactor package structure"
git push origin v1.0.1
```