package main

import (
    "github.com/Shopify/sarama"
    "github.com/bsm/sarama-cluster"
    "log"
    "fmt"
    "os"
    "os/signal"
    "sync"


)
var Address = []string{"10.0.3.153:9092"}


func main()  {
    topic := []string{"LogReciver41"}
    var wg = &sync.WaitGroup{}
    wg.Add(2)
    //广播式消费：消费者1
    go clusterConsumer(wg, Address, topic, "test-consumer-group")
    //广播式消费：消费者2
    go clusterConsumer(wg, Address, topic, "test-consumer-group")
	
	 //广播式消费：消费者3
    go clusterConsumer(wg, Address, topic, "test-consumer-group")
	
	 //广播式消费：消费者4
    go clusterConsumer(wg, Address, topic, "test-consumer-group")
 
 
    wg.Wait()
}
 
// 支持brokers cluster的消费者
func clusterConsumer(wg *sync.WaitGroup,brokers, topics []string, groupId string)  {
    defer wg.Done()
    config := cluster.NewConfig()
    config.Consumer.Return.Errors = true
    config.Group.Return.Notifications = true
    config.Consumer.Offsets.Initial = sarama.OffsetNewest
 
    // init consumer
    consumer, err := cluster.NewConsumer(brokers, groupId, topics, config)
    if err != nil {
        log.Printf("%s: sarama.NewSyncProducer err, message=%s \n", groupId, err)
        return
    }
    defer consumer.Close()
 
    // trap SIGINT to trigger a shutdown
    signals := make(chan os.Signal, 1)
    signal.Notify(signals, os.Interrupt)
 
    // consume errors
    go func() {
        for err := range consumer.Errors() {
            log.Printf("%s:Error: %s\n", groupId, err.Error())
        }
    }()
 
    // consume notifications
    go func() {
        for ntf := range consumer.Notifications() {
            log.Printf("%s:Rebalanced: %+v \n", groupId, ntf)
        }
    }()
 
    // consume messages, watch signals
    var successes int
    Loop:
    for {
        select {
        case msg, ok := <-consumer.Messages():
            if ok {
                //fmt.Fprintf(os.Stdout, "%s:%s/%d/%d\t%s\t%s\n", groupId, msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
                consumer.MarkOffset(msg, "")  // mark message as processed
                successes++
                fmt.Fprintf(os.Stdout,"current count = %d \n",successes)
            }
        case <-signals:
            break Loop
        }
    }
    fmt.Fprintf(os.Stdout, "%s consume %d messages \n", groupId, successes)
}
