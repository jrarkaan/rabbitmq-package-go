package consumer

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

type RabbitMQConsumer struct {
	amqpConn *amqp.Connection
}

func InitRabbitMQConsumer(
	amqpConn *amqp.Connection,
) *RabbitMQConsumer {
	return &RabbitMQConsumer{
		amqpConn: amqpConn,
	}
}

func (consumer *RabbitMQConsumer) CreateChannel(
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
	ch, err := consumer.amqpConn.Channel()
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
		return errors.Wrap(err, "Error  ch.Qos"), nil
	}
	return nil, ch
}

func (consumer *RabbitMQConsumer) StartConsumer(
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

	err, ch := consumer.CreateChannel(
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
	defer ch.Close()

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
