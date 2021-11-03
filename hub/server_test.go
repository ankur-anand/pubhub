package hub_test

import (
	"context"
	"flag"
	"log"
	"net"
	"testing"
	"time"

	"github.com/ankur-anand/pubhub/hub"
	pbhub "github.com/ankur-anand/pubhub/proto/gen/v1/hub"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

var timeoutSelect = flag.Duration("timeout-select", 10*time.Second, "timeout duration on select operation")

const bufSize = 1024 * 1024

func bufDialer(lis *bufconn.Listener) func(context.Context, string) (net.Conn, error) {
	return func(ctx context.Context, s string) (net.Conn, error) {
		return lis.Dial()
	}
}

func grpcDail(ctx context.Context, lis *bufconn.Listener) *grpc.ClientConn {
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(bufDialer(lis)), grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	return conn
}

type mockedHook struct {
	funcCalled func()
}

func (m mockedHook) Duration(metadata hub.HookMetadata, d time.Duration) {
}

func (m mockedHook) PubHook(msg *pbhub.KV) {
	m.funcCalled()
}

func (m mockedHook) SubHook(ops hub.SubscriberOPS) {
	m.funcCalled()
}

func TestClientServer_Subscribe(t *testing.T) {
	gSrv := grpc.NewServer()

	rcvCounter := make(chan int, 10)
	hookFunc := func() {
		rcvCounter <- 1
	}

	server, err := hub.NewServer(gSrv, zaptest.NewLogger(t), mockedHook{funcCalled: hookFunc})
	if err != nil {
		t.Errorf("error creating new server %v", err)
	}
	defer server.Close()

	l1 := bufconn.Listen(bufSize)
	go func() {
		if err := gSrv.Serve(l1); err != nil {
			log.Fatalf("Service exited with error: %v", err)
		}
	}()
	defer gSrv.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clients := make([]*hub.Client, 0)
	newRecv := func(namespace []string) chan *pbhub.KV {
		subscriberConn := grpcDail(ctx, l1)
		client := hub.NewClient(namespace, nil, subscriberConn)
		clients = append(clients, client)

		clientRcv := make(chan *pbhub.KV, 10)
		go func() {
			client.Subscribe(ctx, clientRcv)
		}()
		return clientRcv
	}

	clientRcv1 := newRecv([]string{"test1", "test2"})
	clientRcv2 := newRecv([]string{"*"})
	clientRcv3 := newRecv([]string{"test1", "test2", "test3"})

	count := 0
countLoop:
	for {
		select {
		case <-rcvCounter:
			count++
			if count == 3 {
				break countLoop
			}
		case <-time.After(*timeoutSelect):
			break countLoop
		}
	}

	if count != 3 {
		t.Errorf("error receving the total subscriber")
	}

	tcs := []struct {
		namespace string
		key       string
	}{
		{
			namespace: "test1",
			key:       "hi",
		},
		{
			namespace: "test2",
			key:       "bye",
		},
		{
			namespace: "test3",
			key:       "hi there!"},
	}

	// local server publish.
	for _, tc := range tcs {
		err := server.Publish(&pbhub.KV{
			Namespace: tc.namespace,
			Key:       tc.key,
		})
		if err != nil {
			t.Errorf("error: publishing the msg %v", err)
		}
	}

	dbMake := func(rcv chan *pbhub.KV) map[string]*pbhub.KV {
		db := make(map[string]*pbhub.KV)
		for {
			select {
			case msg := <-rcv:
				db[msg.Key] = msg
			case <-time.After(200 * time.Millisecond):
				return db
			}
		}
	}

	type NK struct {
		key       string
		namespace string
		present   bool
	}
	valTCS := []struct {
		name    string
		nk      []NK
		present []bool
		db      map[string]*pbhub.KV
		client  *hub.Client
	}{
		{
			name: "testcase1",
			nk: []NK{{key: "hi", namespace: "test1", present: true},
				{key: "bye", namespace: "test2", present: true},
				{key: "hi there!", namespace: "test3", present: false}},
			db:     dbMake(clientRcv1),
			client: clients[0],
		},
		{
			name: "test case wildcard",
			nk: []NK{{key: "hi", namespace: "test1", present: true},
				{key: "bye", namespace: "test2", present: true},
				{key: "hi there!", namespace: "test3", present: true}},
			db:     dbMake(clientRcv2),
			client: clients[1],
		},
		{
			name: "testcase3",
			nk: []NK{{key: "hi", namespace: "test1", present: true},
				{key: "bye", namespace: "test2", present: true},
				{key: "hi there!", namespace: "test3", present: true}},
			db:     dbMake(clientRcv3),
			client: clients[2],
		},
	}

	for _, tc := range valTCS {
		t.Run(tc.name, func(t *testing.T) {
			for _, v := range tc.nk {
				val, ok := tc.db[v.key]
				if ok != v.present {
					t.Errorf("expected present didn't matched with returned ok")
				}
				if v.present && val == nil {
					t.Errorf("value for present cannot be nil")
				}

				if val != nil && val.Namespace != v.namespace && val.Key != v.key {
					if val.Id == 0 {
						t.Errorf("id of Value cannot be zero")
					}
					t.Errorf("expected namespace and key mismatch")
				}

				tc.client.UnSubscribe()
			}
		})
	}

}

func TestClientServer_Publish(t *testing.T) {
	gSrv := grpc.NewServer()

	rcvCounter := make(chan int, 10)
	hookFunc := func() {
		rcvCounter <- 1
	}

	server, err := hub.NewServer(gSrv, zaptest.NewLogger(t), mockedHook{funcCalled: hookFunc})
	if err != nil {
		t.Errorf("error creating new server %v", err)
	}
	defer server.Close()

	l1 := bufconn.Listen(bufSize)
	go func() {
		if err := gSrv.Serve(l1); err != nil {
			log.Fatalf("Service exited with error: %v", err)
		}
	}()
	defer gSrv.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clients := make([]*hub.Client, 0)
	newRecv := func(namespace []string) chan *pbhub.KV {
		subscriberConn := grpcDail(ctx, l1)
		client := hub.NewClient(namespace, nil, subscriberConn)
		clients = append(clients, client)

		clientRcv := make(chan *pbhub.KV, 10)
		go func() {
			client.Subscribe(ctx, clientRcv)
		}()
		return clientRcv
	}

	clientRcv1 := newRecv([]string{"test1", "test2"})
	clientRcv2 := newRecv([]string{"*"})
	clientRcv3 := newRecv([]string{"test1", "test2", "test3"})

	count := 0
countLoop:
	for {
		select {
		case <-rcvCounter:
			count++
			if count == 3 {
				break countLoop
			}
		case <-time.After(*timeoutSelect):
			break countLoop
		}
	}

	if count != 3 {
		t.Errorf("error receving the total subscriber")
	}

	tcs := []struct {
		namespace string
		key       string
	}{
		{
			namespace: "test1",
			key:       "hi",
		},
		{
			namespace: "test2",
			key:       "bye",
		},
		{
			namespace: "test3",
			key:       "hi there!"},
	}

	subscriberConn := grpcDail(ctx, l1)
	publisher := hub.NewClient([]string{"*"}, nil, subscriberConn)
	// local server publish.
	for _, tc := range tcs {
		err := publisher.Publish(ctx, &pbhub.KV{
			Namespace: tc.namespace,
			Key:       tc.key,
		})
		if err != nil {
			t.Errorf("error: publishing the msg %v", err)
		}
	}

	dbMake := func(rcv chan *pbhub.KV) map[string]*pbhub.KV {
		db := make(map[string]*pbhub.KV)
		for {
			select {
			case msg := <-rcv:
				db[msg.Key] = msg
			case <-time.After(200 * time.Millisecond):
				return db
			}
		}
	}

	type NK struct {
		key       string
		namespace string
		present   bool
	}
	valTCS := []struct {
		name    string
		nk      []NK
		present []bool
		db      map[string]*pbhub.KV
		client  *hub.Client
	}{
		{
			name: "testcase1",
			nk: []NK{{key: "hi", namespace: "test1", present: true},
				{key: "bye", namespace: "test2", present: true},
				{key: "hi there!", namespace: "test3", present: false}},
			db:     dbMake(clientRcv1),
			client: clients[0],
		},
		{
			name: "test case wildcard",
			nk: []NK{{key: "hi", namespace: "test1", present: true},
				{key: "bye", namespace: "test2", present: true},
				{key: "hi there!", namespace: "test3", present: true}},
			db:     dbMake(clientRcv2),
			client: clients[1],
		},
		{
			name: "testcase3",
			nk: []NK{{key: "hi", namespace: "test1", present: true},
				{key: "bye", namespace: "test2", present: true},
				{key: "hi there!", namespace: "test3", present: true}},
			db:     dbMake(clientRcv3),
			client: clients[2],
		},
	}

	for _, tc := range valTCS {
		t.Run(tc.name, func(t *testing.T) {
			for _, v := range tc.nk {
				val, ok := tc.db[v.key]
				if ok != v.present {
					t.Errorf("expected present didn't matched with returned ok")
				}
				if v.present && val == nil {
					t.Errorf("value for present cannot be nil")
				}

				if val != nil && val.Namespace != v.namespace && val.Key != v.key {
					if val.Id == 0 {
						t.Errorf("id of Value cannot be zero")
					}
					t.Errorf("expected namespace and key mismatch")
				}

				tc.client.UnSubscribe()
			}
		})
	}

}

func TestClientServer_PublishList(t *testing.T) {
	gSrv := grpc.NewServer()

	rcvCounter := make(chan int, 10)
	hookFunc := func() {
		rcvCounter <- 1
	}

	server, err := hub.NewServer(gSrv, zaptest.NewLogger(t), mockedHook{funcCalled: hookFunc})
	if err != nil {
		t.Errorf("error creating new server %v", err)
	}
	defer server.Close()

	l1 := bufconn.Listen(bufSize)
	go func() {
		if err := gSrv.Serve(l1); err != nil {
			log.Fatalf("Service exited with error: %v", err)
		}
	}()
	defer gSrv.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clients := make([]*hub.Client, 0)
	newRecv := func(namespace []string) chan *pbhub.KV {
		subscriberConn := grpcDail(ctx, l1)
		client := hub.NewClient(namespace, nil, subscriberConn)
		clients = append(clients, client)

		clientRcv := make(chan *pbhub.KV, 10)
		go func() {
			client.Subscribe(ctx, clientRcv)
		}()
		return clientRcv
	}

	clientRcv1 := newRecv([]string{"test1", "test2"})
	clientRcv2 := newRecv([]string{"*"})
	clientRcv3 := newRecv([]string{"test1", "test2", "test3"})

	count := 0
countLoop:
	for {
		select {
		case <-rcvCounter:
			count++
			if count == 3 {
				break countLoop
			}
		case <-time.After(*timeoutSelect):
			break countLoop
		}
	}

	if count != 3 {
		t.Errorf("error receving the total subscriber")
	}

	tcs := []struct {
		namespace string
		key       string
	}{
		{
			namespace: "test1",
			key:       "hi",
		},
		{
			namespace: "test2",
			key:       "bye",
		},
		{
			namespace: "test3",
			key:       "hi there!"},
	}

	subscriberConn := grpcDail(ctx, l1)
	publisher := hub.NewClient([]string{"*"}, nil, subscriberConn)
	sendChannel := make(chan *pbhub.KV, 5)
	go publisher.PublishList(ctx, sendChannel)
	// local server publish.
	for _, tc := range tcs {
		sendChannel <- &pbhub.KV{
			Namespace: tc.namespace,
			Key:       tc.key,
		}
	}

	close(sendChannel)
	dbMake := func(rcv chan *pbhub.KV) map[string]*pbhub.KV {
		db := make(map[string]*pbhub.KV)
		for {
			select {
			case msg := <-rcv:
				db[msg.Key] = msg
			case <-time.After(200 * time.Millisecond):
				return db
			}
		}
	}

	type NK struct {
		key       string
		namespace string
		present   bool
	}
	valTCS := []struct {
		name    string
		nk      []NK
		present []bool
		db      map[string]*pbhub.KV
		client  *hub.Client
	}{
		{
			name: "testcase1",
			nk: []NK{{key: "hi", namespace: "test1", present: true},
				{key: "bye", namespace: "test2", present: true},
				{key: "hi there!", namespace: "test3", present: false}},
			db:     dbMake(clientRcv1),
			client: clients[0],
		},
		{
			name: "test case wildcard",
			nk: []NK{{key: "hi", namespace: "test1", present: true},
				{key: "bye", namespace: "test2", present: true},
				{key: "hi there!", namespace: "test3", present: true}},
			db:     dbMake(clientRcv2),
			client: clients[1],
		},
		{
			name: "testcase3",
			nk: []NK{{key: "hi", namespace: "test1", present: true},
				{key: "bye", namespace: "test2", present: true},
				{key: "hi there!", namespace: "test3", present: true}},
			db:     dbMake(clientRcv3),
			client: clients[2],
		},
	}

	for _, tc := range valTCS {
		t.Run(tc.name, func(t *testing.T) {
			for _, v := range tc.nk {
				val, ok := tc.db[v.key]
				if ok != v.present {
					t.Errorf("expected present didn't matched with returned ok")
				}
				if v.present && val == nil {
					t.Errorf("value for present cannot be nil")
				}

				if val != nil && val.Namespace != v.namespace && val.Key != v.key {
					if val.Id == 0 {
						t.Errorf("id of Value cannot be zero")
					}
					t.Errorf("expected namespace and key mismatch")
				}

				tc.client.UnSubscribe()
			}
		})
	}

}

func TestClientServer_Subscribe_Conditional(t *testing.T) {
	gSrv := grpc.NewServer()

	rcvCounter := make(chan int, 10)
	hookFunc := func() {
		rcvCounter <- 1
	}

	server, err := hub.NewServer(gSrv, zaptest.NewLogger(t), mockedHook{funcCalled: hookFunc})
	if err != nil {
		t.Errorf("error creating new server %v", err)
	}
	defer server.Close()

	l1 := bufconn.Listen(bufSize)
	go func() {
		if err := gSrv.Serve(l1); err != nil {
			log.Fatalf("Service exited with error: %v", err)
		}
	}()
	defer gSrv.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clients := make([]*hub.Client, 0)
	newRecv := func(namespace []string, condition []byte) chan *pbhub.KV {
		subscriberConn := grpcDail(ctx, l1)
		client := hub.NewClient(namespace, condition, subscriberConn)
		clients = append(clients, client)

		clientRcv := make(chan *pbhub.KV, 10)
		go func() {
			client.Subscribe(ctx, clientRcv)
		}()
		return clientRcv
	}

	clientRcv1 := newRecv([]string{"test1", "test2"}, []byte(keyTestEqualCondition))
	clientRcv2 := newRecv([]string{"*"}, []byte(metadataTestEqualCondition))
	clientRcv3 := newRecv([]string{"test1", "test2", "test3"}, []byte(multiTestConditions))

	count := 0
countLoop:
	for {
		select {
		case <-rcvCounter:
			count++
			if count == 3 {
				break countLoop
			}
		case <-time.After(*timeoutSelect):
			break countLoop
		}
	}

	if count != 3 {
		t.Errorf("error receving the total subscriber")
	}

	tcs := []struct {
		namespace string
		key       string
	}{
		{
			namespace: "test1",
			key:       "hi",
		},
		{
			namespace: "test2",
			key:       "bye",
		},
		{
			namespace: "test3",
			key:       "hi there!"},
	}

	// local server publish.
	for _, tc := range tcs {
		err := server.Publish(&pbhub.KV{
			Namespace: tc.namespace,
			Key:       tc.key,
			Metadata:  map[string]string{"meta_key": "meta value"},
		})
		if err != nil {
			t.Errorf("error: publishing the msg %v", err)
		}
	}

	dbMake := func(rcv chan *pbhub.KV) map[string]*pbhub.KV {
		db := make(map[string]*pbhub.KV)
		for {
			select {
			case msg := <-rcv:
				db[msg.Key] = msg
			case <-time.After(200 * time.Millisecond):
				return db
			}
		}
	}

	type NK struct {
		key       string
		namespace string
		present   bool
	}
	valTCS := []struct {
		name    string
		nk      []NK
		present []bool
		db      map[string]*pbhub.KV
		client  *hub.Client
	}{
		{
			name: "testcase1",
			nk: []NK{{key: "hi", namespace: "test1", present: true},
				{key: "bye", namespace: "test2", present: false},
				{key: "hi there!", namespace: "test3", present: false}},
			db:     dbMake(clientRcv1),
			client: clients[0],
		},
		{
			name: "test case wildcard",
			nk: []NK{{key: "hi", namespace: "test1", present: true},
				{key: "bye", namespace: "test2", present: true},
				{key: "hi there!", namespace: "test3", present: true}},
			db:     dbMake(clientRcv2),
			client: clients[1],
		},
		{
			name: "testcase3",
			nk: []NK{{key: "hi", namespace: "test1", present: false},
				{key: "bye", namespace: "test2", present: true},
				{key: "hi there!", namespace: "test3", present: false}},
			db:     dbMake(clientRcv3),
			client: clients[2],
		},
	}

	for _, tc := range valTCS {
		t.Run(tc.name, func(t *testing.T) {
			for _, v := range tc.nk {
				val, ok := tc.db[v.key]
				if ok != v.present {
					t.Errorf("expected present didn't matched with returned ok")
				}
				if v.present && val == nil {
					t.Errorf("value for present cannot be nil")
				}

				if val != nil && val.Namespace != v.namespace && val.Key != v.key {
					if val.Id == 0 {
						t.Errorf("id of Value cannot be zero")
					}
					t.Errorf("expected namespace and key mismatch")
				}

				tc.client.UnSubscribe()
			}
		})
	}

}

var keyTestEqualCondition = `{
        "condition-1": {
            "type": "KeyEqualCondition",
            "options": {
                "equals": "hi"
            }
        }
}`

var metadataTestEqualCondition = `{
        "condition-1": {
            "type": "MetadataKeyEqualCondition",
            "options": {
                "equals": "meta value",
				"key_name": "meta_key"
            }
        }
}`

var multiTestConditions = `{
		"condition-2": {
            "type": "KeyEqualCondition",
            "options": {
                "equals": "bye"
            }
        },
        "condition-3": {
            "type": "MetadataKeyEqualCondition",
            "options": {
                "equals": "meta value",
				"key_name": "meta_key"
            }
        }
}`
