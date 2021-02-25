package amqp

import (
	"context"
	"testing"
	"time"
)

func makeLink(mode ReceiverSettleMode) *link {
	return &link{
		close:              make(chan struct{}),
		done:               make(chan struct{}),
		receiverReady:      make(chan struct{}, 1),
		messages:           make(chan Message, 1),
		receiverSettleMode: &mode,
		unsettledMessages:  map[string]struct{}{},
	}
}

func doNothing(msg *Message) error {
	return nil
}

func accept(msg *Message) error {
	return msg.Accept(context.TODO())
}

func makeMessage() Message {
	return Message{
		deliveryID:  uint32(1),
		DeliveryTag: []byte("one"),
		doneSignal:  make(chan struct{}),
	}
}

func TestReceiver_HandleMessageModeFirst_AutoAccept(t *testing.T) {
	r := &Receiver{
		link:         makeLink(ModeFirst),
		batching:     true, // allows to  avoid making the outgoing call on dispostion
		dispositions: make(chan messageDisposition, 2),
	}
	msg := makeMessage()
	r.link.messages <- msg
	r.link.addUnsettled(&msg)
	if err := r.HandleMessage(context.TODO(), doNothing); err != nil {
		t.Errorf("HandleMessage() error = %v", err)
	}

	if len(r.dispositions) == 0 {
		t.Errorf("the message should have triggered a disposition")
	}

	// handle the race because the mao is purged in a background goroutine
	check := true
	success := true
	for check {
		select {
		case <-time.After(10 * time.Millisecond):
			success = false
			break
		default:
			if r.link.countUnsettled() == 0 {
				check = false
			}
		}
	}
	if !success {
		t.Errorf("the message was not removed from the unsettled map")
	}
}

func TestReceiver_HandleMessageModeSecond_DontDispose(t *testing.T) {
	r := &Receiver{
		link:         makeLink(ModeSecond),
		batching:     true, // allows to  avoid making the outgoing call on dispostion
		dispositions: make(chan messageDisposition, 2),
	}
	msg := makeMessage()
	r.link.messages <- msg
	r.link.addUnsettled(&msg)
	if err := r.HandleMessage(context.TODO(), doNothing); err != nil {
		t.Errorf("HandleMessage() error = %v", err)
	}
	if len(r.dispositions) != 0 {
		t.Errorf("it is up to the message handler to settle messages")
	}
	if r.link.countUnsettled() == 0 {
		t.Errorf("the message should still be tracked until settled")
	}
}

func TestReceiver_HandleMessageModeSecond_removeFromUnsettledMapOnDisposition(t *testing.T) {
	r := &Receiver{
		link:         makeLink(ModeSecond),
		batching:     true, // allows to  avoid making the outgoing call on dispostion
		dispositions: make(chan messageDisposition, 1),
	}
	msg := makeMessage()
	r.link.messages <- msg
	r.link.addUnsettled(&msg)
	// unblock the accept waiting on inflight disposition for modeSecond
	loop := true
	// call handle with the accept handler in a goroutine because it will block on inflight disposition.
	go func() {
		if err := r.HandleMessage(context.TODO(), accept); err != nil {
			t.Errorf("HandleMessage() error = %v", err)
		}
	}()

	// simulate batch disposition.
	// when the inflight has an entry, we know the Accept has been called
	for loop {
		r.inFlight.mu.Lock()
		inflightCount := len(r.inFlight.m)
		r.inFlight.mu.Unlock()
		if inflightCount > 0 {
			r.inFlight.remove(msg.deliveryID, nil, nil)
			loop = false
		}
		time.Sleep(1 * time.Millisecond)
	}

	if len(r.dispositions) == 0 {
		t.Errorf("the message should have triggered a disposition")
	}
	if r.link.countUnsettled() != 0 {
		t.Errorf("the message should be removed from unsettled map")
	}
	loop = false
}
