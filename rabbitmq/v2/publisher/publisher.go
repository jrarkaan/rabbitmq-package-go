package publisher

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

type RabbitMQPublisher struct {
	amqpChan *amqp.Channel
}

func InitRabbitMQConsumer(
	amqpConn *amqp.Connection,
) (error, *RabbitMQPublisher) {
	amqpChan, errChannel := amqpConn.Channel()
	if errChannel != nil {
		return errors.Wrap(errChannel, "p.amqpConn.Channel"), nil
	}
	return nil, &RabbitMQPublisher{
		amqpChan: amqpChan,
	}
}

func (publisher *RabbitMQPublisher) CloseChan() {
	if err := publisher.amqpChan.Close(); err != nil {
		fmt.Println(
			fmt.Sprintf("EmailsPublisher CloseChan: %v", err),
		)
	}
}

func (publisher *RabbitMQPublisher) Publish(
	exchangeName string,
	bindingKey string,
	mandatory bool,
	immediate bool,
	body []byte,
	contentType string,
) error {
	fmt.Println(
		fmt.Sprintf("Publishing message Exchange: %s, RoutingKey: %s", exchangeName, bindingKey),
	)
	errPublish := publisher.amqpChan.Publish(
		exchangeName,
		bindingKey,
		mandatory,
		immediate,
		amqp.Publishing{
			ContentType: contentType,
			Body:        body,
		},
	)
	if errPublish != nil {
		return errors.Wrap(errPublish, "ch.Publish")
	}
	return nil
}
