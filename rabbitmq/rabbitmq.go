package rabbitmq

import (
	"fmt"
	"github.com/pkg/errors"
	"log"

	"github.com/streadway/amqp"
)

type RabbitMQ struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	queue   amqp.Queue
}

func NewRabbitMQ(url string) (*RabbitMQ, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		log.Println("Failed to connect to RabbitMQ")
		return nil, err
	}

	channel, errChannel := conn.Channel()
	if errChannel != nil {
		log.Println("Failed to open a channel")
		return nil, errChannel
	}

	return &RabbitMQ{
		conn:    conn,
		channel: channel,
	}, nil
}

func (r *RabbitMQ) CreateChannel(
	bindingKey string,
	consumerTag string,
	exchangeName string,
	kindExchangeName string,
	durableExchange bool,
	autoDeleteExchange bool,
	internalExchange bool,
	noWaitExchange bool,
	argsExchange amqp.Table,
	queueName string,
	durableQueue bool,
	autoDeleteQueue bool,
	exclusiveQueue bool,
	noWaitQueue bool,
	argQueue amqp.Table,
	prefetchCount int,
	prefetchSize int,
	prefetchGlobal bool,
) (error, *amqp.Channel) {
	ch, err := r.conn.Channel()
	if err != nil {
		return errors.Wrap(err, "Error Consumer amqpConn.Channel"), nil
	}
	fmt.Println(fmt.Sprintf("Declaring exchange: %s", exchangeName))
	errExchangeDeclare := ch.ExchangeDeclare(
		exchangeName,
		kindExchangeName,
		durableExchange,
		autoDeleteExchange,
		internalExchange,
		noWaitExchange,
		argsExchange,
	)
	if errExchangeDeclare != nil {
		return errors.Wrap(errExchangeDeclare, "Error ch.ExchangeDeclare"), nil
	}
	queue, errQueue := ch.QueueDeclare(
		queueName,
		durableQueue,
		autoDeleteQueue,
		exclusiveQueue,
		noWaitQueue,
		argQueue,
	)
	if errQueue != nil {
		return errors.Wrap(errQueue, "Error Consumer ch.QueueDeclare"), nil
	}
	fmt.Println(
		fmt.Sprintf("Declared queue, binding it to exchange: Queue: %v, messagesCount: %v, "+
			"consumerCount: %v, exchange: %v, bindingKey: %v",
			queue.Name,
			queue.Messages,
			queue.Consumers,
			exchangeName,
			bindingKey),
	)
	err = ch.QueueBind(
		queue.Name,
		bindingKey,
		exchangeName,
		noWaitQueue,
		argQueue,
	)
	if err != nil {
		return errors.Wrap(err, "Error Consumer ch.QueueBind"), nil
	}
	fmt.Println(
		fmt.Sprintf("Queue bound to exchange, starting to consume from queue, consumerTag: %v", consumerTag),
	)
	err = ch.Qos(
		prefetchCount,  // prefetch count
		prefetchSize,   // prefetch size
		prefetchGlobal, // global
	)
	if err != nil {
		return errors.Wrap(err, "Error ch.Qos"), nil
	}
	return nil, ch
}

func (r *RabbitMQ) StartConsumer(
	bindingKey string,
	consumerTag string,
	exchangeName string,
	kindExchangeName string,
	durableExchange bool,
	autoDeleteExchange bool,
	internalExchange bool,
	noWaitExchange bool,
	argsExchange amqp.Table,
	queueName string,
	durableQueue bool,
	autoDeleteQueue bool,
	exclusiveQueue bool,
	noWaitQueue bool,
	argQueue amqp.Table,
	prefetchCount int,
	prefetchSize int,
	prefetchGlobal bool,
	consumeAutoAck bool,
	consumeExclusive bool,
	consumeNoLocal bool,
	consumeNoWait bool,
	consumeArg amqp.Table,
) (error, <-chan amqp.Delivery) {
	err, ch := r.CreateChannel(
		bindingKey,
		consumerTag,
		exchangeName,
		kindExchangeName,
		durableExchange,
		autoDeleteExchange,
		internalExchange,
		noWaitExchange,
		argsExchange,
		queueName,
		durableQueue,
		autoDeleteQueue,
		exclusiveQueue,
		noWaitQueue,
		argQueue,
		prefetchCount,
		prefetchSize,
		prefetchGlobal,
	)
	if err != nil {
		return errors.Wrap(err, "CreateChannel"), nil
	}

	deliveries, errConsume := ch.Consume(
		queueName,
		consumerTag,
		consumeAutoAck,
		consumeExclusive,
		consumeNoLocal,
		consumeNoWait,
		consumeArg,
	)
	if errConsume != nil {
		return errors.Wrap(err, "Consume"), nil
	}

	return nil, deliveries
}

func (r *RabbitMQ) Publish(
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
	errPublish := r.channel.Publish(
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

func (r *RabbitMQ) Close() {
	r.channel.Close()
	r.conn.Close()
}
