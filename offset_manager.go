package sarama

import (
	"sync"
	"time"
)

// Offset Manager

// OffsetManager uses Kafka to store and fetch consumed partition offsets.
type OffsetManager interface {
	// ManagePartition creates a PartitionOffsetManager on the given topic/partition. It will
	// return an error if this OffsetManager is already managing the given topic/partition.
	ManagePartition(topic string, partition int32) (PartitionOffsetManager, error)

	// Close stops the OffsetManager from managing offsets. It is required to call this function
	// before an OffsetManager object passes out of scope, as it will otherwise
	// leak memory. You must call this after all the PartitionOffsetManagers are closed.
	Close() error
}

type offsetManager struct {
	client Client
	conf   *Config
	group  string

	lock      sync.Mutex
	poms      map[topicPartition]*partitionOffsetManager
	boms      map[*Broker]*brokerOffsetManager
	signal    chan offsetMgrSignal
	batchChan chan offsetUpdate
	timer     *time.Ticker
	bom       *brokerOffsetManager
}

// NewOffsetManagerFromClient creates a new OffsetManager from the given client.
// It is still necessary to call Close() on the underlying client when finished with the partition manager.
func NewOffsetManagerFromClient(group string, client Client) (OffsetManager, error) {
	// Check that we are not dealing with a closed Client before processing any other arguments
	if client.Closed() {
		return nil, ErrClosedClient
	}
	conf := client.Config()

	om := &offsetManager{
		client:    client,
		conf:      conf,
		group:     group,
		poms:      make(map[topicPartition]*partitionOffsetManager),
		boms:      make(map[*Broker]*brokerOffsetManager),
		signal:    make(chan offsetMgrSignal, conf.ChannelBufferSize),
		batchChan: make(chan offsetUpdate, conf.ChannelBufferSize),
		timer:     time.NewTicker(conf.Consumer.Offsets.CommitInterval),
	}

	go withRecover(om.batcher)
	go withRecover(om.hub)

	return om, nil
}

func (om *offsetManager) ManagePartition(topic string, partition int32) (PartitionOffsetManager, error) {
	pom, err := om.newPartitionOffsetManager(topic, partition)
	if err != nil {
		return nil, err
	}

	om.lock.Lock()
	defer om.lock.Unlock()
	if om.poms[pom.topicPartition] != nil {
		return nil, ConfigurationError("That topic/partition is already being managed")
	}
	om.poms[pom.topicPartition] = pom
	return pom, nil
}

func (om *offsetManager) Close() error {
	return nil
}

func (om *offsetManager) refBrokerOffsetManager(broker *Broker) *brokerOffsetManager {
	om.lock.Lock()
	defer om.lock.Unlock()

	bom := om.boms[broker]
	if bom == nil {
		bom = om.newBrokerOffsetManager(broker)
		om.boms[broker] = bom
	}

	bom.refs++

	return bom
}

func (om *offsetManager) unrefBrokerOffsetManager(bom *brokerOffsetManager) {
	om.lock.Lock()
	defer om.lock.Unlock()

	bom.refs--

	if bom.refs == 0 {
		if om.boms[bom.broker] == bom {
			delete(om.boms, bom.broker)
		}
	}
}

func (om *offsetManager) abandonBroker(bom *brokerOffsetManager) {
	om.lock.Lock()
	defer om.lock.Unlock()
	delete(om.boms, bom.broker)
	if om.bom == bom {
		om.bom = nil
	}
}

func (om *offsetManager) abandonPartitionOffsetManager(pom *partitionOffsetManager) bool {
	om.lock.Lock()
	defer om.lock.Unlock()
	_, ok := om.poms[pom.topicPartition]
	if ok {
		delete(om.poms, pom.topicPartition)
	}
	return ok
}

func (om *offsetManager) markOffset(pom *partitionOffsetManager, offset int64, metadata string) {
	om.batchChan <- offsetUpdate{pom, offset, metadata}
}

func (om *offsetManager) getBom() (*brokerOffsetManager, error) {
	om.lock.Lock()
	bom := om.bom
	om.lock.Unlock()

	if bom == nil {
		var broker *Broker
		var err error
		if err = om.client.RefreshCoordinator(om.group); err != nil {
			return nil, err
		}

		if broker, err = om.client.Coordinator(om.group); err != nil {
			return nil, err
		}

		bom = om.refBrokerOffsetManager(broker)
	}
	return bom, nil
}

// Collect all the updates we get into batches
func (om *offsetManager) batcher() {
	var batch = make(offsetUpdates)
	fire := func() {
		if len(batch) > 0 {
			om.signal <- offsetMgrSignal{
				commit: batch,
			}
			batch = make(offsetUpdates, len(batch))
		}
	}

	for {
		select {
		case update := <-om.batchChan:
			batch[update.pom.topicPartition] = update
		case <-om.timer.C:
			fire()
		}
	}
	fire()
}

// Dispatch batches and other events like registrations
func (om *offsetManager) hub() {
	var signal offsetMgrSignal
	for {
		select {
		case signal = <-om.signal:
			// do nothing, fall through
		}

		if signal.commit != nil {
			om.flushOffsetCommit(signal.commit)
		}
	}
}

func (om *offsetManager) flushOffsetCommit(batch offsetUpdates) {
	bom, err := om.getBom()
	if err != nil {
		// TODO
		return
	}
	bom.flushToBroker(batch)
}

// Partition Offset Manager

// PartitionOffsetManager uses Kafka to store and fetch consumed partition offsets. You MUST call Close()
// on a partition offset manager to avoid leaks, it will not be garbage-collected automatically when it passes
// out of scope.
type PartitionOffsetManager interface {
	// NextOffset returns the next offset that should be consumed for the managed partition, accompanied
	// by metadata which can be used to reconstruct the state of the partition consumer when it resumes.
	// NextOffset() will return `config.Consumer.Offsets.Initial` and an empty metadata string if no
	// offset was committed for this partition yet.
	NextOffset() (int64, string)

	// MarkOffset marks the provided offset as processed, alongside a metadata string that represents
	// the state of the partition consumer at that point in time. The metadata string can be used by
	// another consumer to restore that state, so it can resume consumption.
	//
	// Note: calling MarkOffset does not necessarily commit the offset to the backend store immediately
	// for efficiency reasons, and it may never be committed if your application crashes. This means that
	// you may end up processing the same message twice, and your processing should ideally be idempotent.
	MarkOffset(offset int64, metadata string)

	// Errors returns a read channel of errors that occur during offset management, if enabled. By default,
	// errors are logged and not returned over this channel. If you want to implement any custom error
	// handling, set your config's Consumer.Return.Errors setting to true, and read from this channel.
	Errors() <-chan *ConsumerError

	// AsyncClose initiates a shutdown of the PartitionOffsetManager. This method will return immediately,
	// after which you should wait until the 'errors' channel has been drained and closed.
	// It is required to call this function, or Close before a consumer object passes out of scope,
	// as it will otherwise leak memory.  You must call this before calling Close on the underlying
	// client.
	AsyncClose()

	// Close stops the PartitionOffsetManager from managing offsets. It is required to call this function
	// (or AsyncClose) before a PartitionOffsetManager object passes out of scope, as it will otherwise
	// leak memory. You must call this before calling Close on the underlying client.
	Close() error
}

type partitionOffsetManager struct {
	parent *offsetManager
	topicPartition

	lock     sync.Mutex
	offset   int64
	metadata string

	errors chan *ConsumerError
}

func (om *offsetManager) newPartitionOffsetManager(topic string, partition int32) (*partitionOffsetManager, error) {
	pom := &partitionOffsetManager{
		parent: om,
		topicPartition: topicPartition{
			topic:     topic,
			partition: partition,
		},
		errors: make(chan *ConsumerError, om.conf.ChannelBufferSize),
	}

	if err := pom.fetchInitialOffset(om.conf.Metadata.Retry.Max); err != nil {
		return nil, err
	}
	return pom, nil
}

func (pom *partitionOffsetManager) fetchInitialOffset(retries int) error {
	request := new(OffsetFetchRequest)
	request.Version = 1
	request.ConsumerGroup = pom.parent.group
	request.AddPartition(pom.topic, pom.partition)

	bom, err := pom.parent.getBom()
	if err != nil {
		return err
	}
	response, err := bom.broker.FetchOffset(request)
	if err != nil {
		return err
	}

	block := response.GetBlock(pom.topic, pom.partition)
	if block == nil {
		return ErrIncompleteResponse
	}

	switch block.Err {
	case ErrNoError:
		pom.offset = block.Offset
		pom.metadata = block.Metadata
		return nil
	case ErrNotCoordinatorForConsumer:
		if retries <= 0 {
			return block.Err
		}
		pom.parent.abandonBroker(bom)
		if _, err := pom.parent.getBom(); err != nil {
			return err
		}
		return pom.fetchInitialOffset(retries - 1)
	case ErrOffsetsLoadInProgress:
		if retries <= 0 {
			return block.Err
		}
		time.Sleep(pom.parent.conf.Metadata.Retry.Backoff)
		return pom.fetchInitialOffset(retries - 1)
	default:
		return block.Err
	}
}

func (pom *partitionOffsetManager) handleError(err error) {
	cErr := &ConsumerError{
		Topic:     pom.topic,
		Partition: pom.partition,
		Err:       err,
	}

	if pom.parent.conf.Consumer.Return.Errors {
		pom.errors <- cErr
	} else {
		Logger.Println(cErr)
	}
}

func (pom *partitionOffsetManager) Errors() <-chan *ConsumerError {
	return pom.errors
}

func (pom *partitionOffsetManager) MarkOffset(offset int64, metadata string) {
	pom.parent.markOffset(pom, offset, metadata)
}

func (pom *partitionOffsetManager) updateCommitted(offset int64, metadata string) {
	pom.lock.Lock()
	defer pom.lock.Unlock()
	pom.offset = offset
	pom.metadata = metadata
}

func (pom *partitionOffsetManager) NextOffset() (int64, string) {
	pom.lock.Lock()
	defer pom.lock.Unlock()

	if pom.offset >= 0 {
		return pom.offset + 1, pom.metadata
	} else {
		return pom.parent.conf.Consumer.Offsets.Initial, ""
	}
}

func (pom *partitionOffsetManager) AsyncClose() {
	go func() {
		if pom.parent.abandonPartitionOffsetManager(pom) {
			close(pom.errors)
		}
	}()
}

func (pom *partitionOffsetManager) Close() error {
	pom.AsyncClose()

	var errors ConsumerErrors
	for err := range pom.errors {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		return errors
	}
	return nil
}

// Broker Offset Manager

type brokerOffsetManager struct {
	parent *offsetManager
	broker *Broker
	refs   int
}

func (om *offsetManager) newBrokerOffsetManager(broker *Broker) *brokerOffsetManager {
	bom := &brokerOffsetManager{
		parent: om,
		broker: broker,
	}

	return bom
}

func (bom *brokerOffsetManager) flushToBroker(updates offsetUpdates) {
	request := bom.constructRequest(updates)
	response, err := bom.broker.CommitOffset(request)

	if err != nil {
		bom.abort(err)
		return
	}

	for _, update := range updates {
		s := update.pom
		if request.blocks[s.topic] == nil || request.blocks[s.topic][s.partition] == nil {
			continue
		}

		var err KError
		var ok bool

		if response.Errors[s.topic] == nil {
			s.handleError(ErrIncompleteResponse)
			continue
		}
		if err, ok = response.Errors[s.topic][s.partition]; !ok {
			s.handleError(ErrIncompleteResponse)
			continue
		}

		switch err {
		case ErrNoError:
			block := request.blocks[s.topic][s.partition]
			s.updateCommitted(block.offset, block.metadata)
		case ErrUnknownTopicOrPartition, ErrNotLeaderForPartition, ErrLeaderNotAvailable:
			bom.abort(err)
			s.handleError(err)
		default:
			s.handleError(err)
		}
	}
}

func (bom *brokerOffsetManager) constructRequest(updates offsetUpdates) *OffsetCommitRequest {
	r := &OffsetCommitRequest{
		Version:       1,
		ConsumerGroup: bom.parent.group,
	}

	for _, s := range updates {
		r.AddBlock(s.pom.topic, s.pom.partition, s.offset, ReceiveTime, s.metadata)
	}
	return r
}

func (bom *brokerOffsetManager) abort(err error) {
	_ = bom.broker.Close() // we don't care about the error this might return, we already have one
	bom.parent.abandonBroker(bom)
}

type offsetMgrSignal struct {
	commit offsetUpdates
}

type offsetUpdate struct {
	pom      *partitionOffsetManager
	offset   int64
	metadata string
}

type topicPartition struct {
	topic     string
	partition int32
}

type offsetUpdates map[topicPartition]offsetUpdate
