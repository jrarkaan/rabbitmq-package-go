package v1_5

import "github.com/streadway/amqp"

func InitRabbitMQConnection(url string) (error, *amqp.Connection) {
	connection, errorConnection := amqp.Dial(url)
	if errorConnection != nil {
		return errorConnection, nil
	}
	return nil, connection
}
