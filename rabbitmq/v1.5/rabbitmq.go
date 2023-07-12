package v1_5

import (
	"fmt"
	"github.com/streadway/amqp"
)

func InitRabbitMQConnection(url string) (error, *amqp.Connection) {
	fmt.Println("Connection RabbitMQ")
	connection, errorConnection := amqp.Dial(url)
	if errorConnection != nil {
		return errorConnection, nil
	}
	return nil, connection
}
