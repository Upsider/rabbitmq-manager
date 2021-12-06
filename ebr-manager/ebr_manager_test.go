package ebr_manager

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	rabbitmq_manager "github.com/Upsider/rabbitmq-manager"
)

func TestReconnection(t *testing.T) {
	mgr, err := NewManager(rabbitmq_manager.Config{
		Host:         "localhost",
		Port:         5672,
		Username:     "guest",
		Password:     "guest",
		NamingPrefix: "",
		NamingSuffix: "",
	})
	if err != nil {
		panic(err)
	}

	err = mgr.Setup()
	if err != nil {
		panic(err)
	}

	mgr.RabbitClient.Close()

	fmt.Println(mgr.RabbitClient.IsClosed())

	time.Sleep(4 * time.Second)

	fmt.Println(mgr.RabbitClient.IsClosed())

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, os.Kill, syscall.SIGTERM)
	<-signalChan

}
