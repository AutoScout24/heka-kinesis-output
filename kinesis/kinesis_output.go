package kinesis

import (
    "fmt"
    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/credentials"
    kin "github.com/aws/aws-sdk-go/service/kinesis"
    "github.com/mozilla-services/heka/message"
    "github.com/mozilla-services/heka/pipeline"
    "net/http"
    "time"
    "sync"
    "sync/atomic"
    "math/rand"
    "math"

)

type KinesisOutput struct {
    batchesSent                     int64
    batchesFailed                   int64
    processMessageCount             int64
    dropMessageCount                int64
    recordCount                     int64
    retryCount                      int64
    reportLock                      sync.Mutex
    flushLock                       sync.Mutex
    config                          *KinesisOutputConfig
    Client                          *kin.Kinesis
    awsConf                         *aws.Config
    batchedData                     []byte
    batchedEntries                  []*kin.PutRecordsRequestEntry
    backoffIncrement                time.Duration
    KINESIS_SHARDS                  int
    KINESIS_RECORD_SIZE             int
    KINESIS_SHARD_CAPACITY          int
    KINESIS_PUT_RECORDS_SIZE_LIMIT  int
    KINESIS_PUT_RECORDS_BATCH_SIZE  int
    hasTriedToSend                  bool
}

type KinesisOutputConfig struct {
    Region            string `toml:"region"`
    Stream            string `toml:"stream"`
    AccessKeyID       string `toml:"access_key_id"`
    SecretAccessKey   string `toml:"secret_access_key"`
    Token             string `toml:"token"`
    PayloadOnly       bool   `toml:"payload_only"`
    BackoffIncrement  string `toml:"backoff_increment"`
    MaxRetries        int    `toml:"max_retries"`
    KinesisShardCount int    `toml:"kinesis_shard_count"`
    KinesisRecordSize int    `toml:"kinesis_record_size"`
}

func (k *KinesisOutput) ConfigStruct() interface{} {
    return &KinesisOutputConfig{
        Region:          "eu-west-1",
        Stream:          "",
        AccessKeyID:     "",
        SecretAccessKey: "",
        Token:           "",
    }
}

func init() {
    pipeline.RegisterPlugin("KinesisOutput", func() interface{} { return new(KinesisOutput) })
}

func (k *KinesisOutput) InitAWS() *aws.Config {
    var creds *credentials.Credentials

    if k.config.AccessKeyID != "" && k.config.SecretAccessKey != "" {
        creds = credentials.NewStaticCredentials(k.config.AccessKeyID, k.config.SecretAccessKey, "")
    } else {
        creds = credentials.NewEC2RoleCredentials(&http.Client{Timeout: 10 * time.Second}, "", 0)
    }
    return &aws.Config{
        Region:      k.config.Region,
        Credentials: creds,
    }
}

func (k *KinesisOutput) Init(config interface{}) error {
    k.config = config.(*KinesisOutputConfig)

    if (k.config.BackoffIncrement == "") {
        k.config.BackoffIncrement = "250ms"
    }
    duration, err := time.ParseDuration(k.config.BackoffIncrement)
    if (err != nil) {
        return fmt.Errorf("Unable to parse backoff_increment as a valid duration. See https://golang.org/pkg/time/#ParseDuration. Err: %s", err)
    }
    k.backoffIncrement = duration

    if (k.config.MaxRetries == 0) {
        k.config.MaxRetries = 30
    }

    if (k.config.KinesisShardCount == 0) {
        return fmt.Errorf("Please supply the value kinesis_shard_count")
    }

    if (k.config.KinesisRecordSize > (1000 * 1024)) {
        return fmt.Errorf("Your kinesis_record_size must be less than 1M. See https://docs.aws.amazon.com/sdk-for-go/api/service/kinesis/Kinesis.html#PutRecords-instance_method")
    } else if (k.config.KinesisRecordSize == 0) {
        k.config.KinesisRecordSize = (100 * 1024) // 100 KB
    }

    k.KINESIS_SHARDS = k.config.KinesisShardCount
    k.KINESIS_RECORD_SIZE = k.config.KinesisRecordSize

    k.KINESIS_SHARD_CAPACITY = k.KINESIS_SHARDS * 1024 * 1024
    k.KINESIS_PUT_RECORDS_SIZE_LIMIT = int(math.Min(float64(k.KINESIS_SHARD_CAPACITY), 5 * 1024 * 1024)) // 5 MB;
    k.KINESIS_PUT_RECORDS_BATCH_SIZE = int(math.Max(1, math.Floor(float64(k.KINESIS_PUT_RECORDS_SIZE_LIMIT / k.KINESIS_RECORD_SIZE)) - 1))

    k.batchedData = []byte {}
    k.batchedEntries = []*kin.PutRecordsRequestEntry {}

    k.awsConf = k.InitAWS()
    
    k.Client = kin.New(k.awsConf)

    return nil
}

func (k *KinesisOutput) SendEntries(or pipeline.OutputRunner, entries []*kin.PutRecordsRequestEntry, backoff time.Duration, retries int) error {

    k.hasTriedToSend = true

    multParams := &kin.PutRecordsInput{
        Records:      entries,
        StreamName:   aws.String(k.config.Stream),
    }

    data, err := k.Client.PutRecords(multParams)
    
    // Update statistics & handle errors
    if err != nil {
        if (or != nil) {
            or.LogError(fmt.Errorf("Batch: Error pushing message to Kinesis: %s", err))
        }
        atomic.AddInt64(&k.batchesFailed, 1)

        if (retries <= k.config.MaxRetries || k.config.MaxRetries == -1) {
            atomic.AddInt64(&k.retryCount, 1)

            // filter down to only the failed records:
            retryEntries := []*kin.PutRecordsRequestEntry {}
            for i, entry := range entries {
                response := data.Records[i]
                if (response.ErrorCode != nil) {
                    retryEntries = append(retryEntries, entry)
                }
            }

            time.Sleep(backoff + k.backoffIncrement)
            k.SendEntries(or, retryEntries, backoff + k.backoffIncrement, retries + 1)
        } else {
            atomic.AddInt64(&k.dropMessageCount, int64(len(entries)))
            if (or != nil) {
                or.LogError(fmt.Errorf("Batch: Hit max retries when attempting to send data"))
            }
        }
    }

    atomic.AddInt64(&k.batchesSent, 1)

    return nil
}

func (k *KinesisOutput) PrepareSend(or pipeline.OutputRunner, entries []*kin.PutRecordsRequestEntry) {
    // clone the entries so the output can happen
    clonedEntries := make([]*kin.PutRecordsRequestEntry, len(entries))
    copy(clonedEntries, entries)

    // Run the put async
    go k.SendEntries(or, clonedEntries, 0, 0)
}

func (k *KinesisOutput) BundleMessage(msg []byte) *kin.PutRecordsRequestEntry {
    // define a Partition Key
    pk := fmt.Sprintf("%X", rand.Int63())

    // Add things to the current batch.
    return &kin.PutRecordsRequestEntry {
        Data:            msg,
        PartitionKey:    aws.String(pk),
    }
}

func (k *KinesisOutput) AddToRecordBatch(or pipeline.OutputRunner, msg []byte) {
    entry := k.BundleMessage(msg)

    tmp := append(k.batchedEntries, entry)

    // if we have hit the batch limit, send.
    if (len(tmp) > k.KINESIS_PUT_RECORDS_BATCH_SIZE) {
        k.PrepareSend(or, k.batchedEntries)
        k.batchedEntries = []*kin.PutRecordsRequestEntry { entry }
    } else {
        k.batchedEntries = tmp
    }

    // do Reporting
    atomic.AddInt64(&k.recordCount, 1)
}

func (k *KinesisOutput) HandlePackage(or pipeline.OutputRunner, pack *pipeline.PipelinePack) error {
    k.flushLock.Lock()
    defer k.flushLock.Unlock()

    // encode the packages.
    msg, err := or.Encode(pack)
    if err != nil {
        errOut := fmt.Errorf("Error encoding message: %s", err)
        or.LogError(errOut)
        pack.Recycle(nil)
        return errOut
    }

    // If we only care about the Payload...
    if k.config.PayloadOnly {
        msg = []byte(pack.Message.GetPayload())
    }

    var tmp []byte
    // if we already have data then we should append.
    if (len(k.batchedData) > 0) {
        tmp = append(append(k.batchedData, []byte(",")...), msg...)
    } else {
        tmp = msg
    }

    // if we can't fit the data in this record
    if (len(tmp) > k.KINESIS_RECORD_SIZE) {
        // add the existing data to the output batch
        array := append(append([]byte("["), k.batchedData...), []byte("]")...)
        k.AddToRecordBatch(or, array)

        // update the batched data to only contain the current message.
        k.batchedData = msg
    } else {
        // otherwise we add the existing data to a batch
        k.batchedData = tmp
    }

    // do reporting and tidy up
    atomic.AddInt64(&k.processMessageCount, 1)
    pack.Recycle(nil)

    return nil
}

func (k *KinesisOutput) Run(or pipeline.OutputRunner, helper pipeline.PluginHelper) error {
    var pack *pipeline.PipelinePack

    if or.Encoder() == nil {
        return fmt.Errorf("Encoder required.")
    }

    // handle packages
    for pack = range or.InChan() {
        k.HandlePackage(or, pack)
    }

    return nil
}

func (k *KinesisOutput) ReportMsg(msg *message.Message) error {
    k.reportLock.Lock()
    defer k.reportLock.Unlock()

    message.NewInt64Field(msg, "ProcessMessageCount", atomic.LoadInt64(&k.processMessageCount), "count")
    message.NewInt64Field(msg, "DropMessageCount", atomic.LoadInt64(&k.dropMessageCount), "count")
    
    message.NewInt64Field(msg, "BatchesSent", atomic.LoadInt64(&k.batchesSent), "count")
    message.NewInt64Field(msg, "BatchesFailed", atomic.LoadInt64(&k.batchesFailed), "count")

    message.NewInt64Field(msg, "RecordCount", atomic.LoadInt64(&k.recordCount), "count")

    message.NewInt64Field(msg, "RetryCount", atomic.LoadInt64(&k.retryCount), "count")
    
    return nil
}

func (k *KinesisOutput) TimerEvent() error {
    if(!hasTriedToSend) {
        k.FlushData()
    }
    hasTriedToSend = false
}

func (k *KinesisOutput) FlushData() {
    k.flushLock.Lock()
    defer k.flushLock.Unlock()

    array := append(append([]byte("["), k.batchedData...), []byte("]")...)
    entry := k.BundleMessage(array)

    k.PrepareSend(nil, append(k.batchedEntries, entry))
    k.batchedEntries = []*kin.PutRecordsRequestEntry { }
}

func (k *KinesisOutput) CleanupForRestart() {

    // force flush all messages in memory.
    k.FlushData()

    k.batchedData = []byte {}
    k.batchedEntries = []*kin.PutRecordsRequestEntry {}

    k.reportLock.Lock()
    defer k.reportLock.Unlock()

    atomic.StoreInt64(&k.processMessageCount, 0)
    atomic.StoreInt64(&k.dropMessageCount, 0)
    atomic.StoreInt64(&k.batchesSent, 0)
    atomic.StoreInt64(&k.batchesFailed, 0)
    atomic.StoreInt64(&k.recordCount, 0)
    atomic.StoreInt64(&k.retryCount, 0)
}
