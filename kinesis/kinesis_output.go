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
)

type KinesisOutput struct {
    batchesSent         int64
    batchesFailed       int64
    processMessageCount int64
    dropMessageCount    int64
    recordCount         int64
    retryCount          int64
    reportLock          sync.Mutex
    config              *KinesisOutputConfig
    Client              *kin.Kinesis
    awsConf             *aws.Config
    batchedData         []byte
    batchedEntries      []*kin.PutRecordsRequestEntry
}

type KinesisOutputConfig struct {
    Region          string `toml:"region"`
    Stream          string `toml:"stream"`
    AccessKeyID     string `toml:"access_key_id"`
    SecretAccessKey string `toml:"secret_access_key"`
    Token           string `toml:"token"`
    PayloadOnly     bool   `toml:"payload_only"`
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

const BACKOFF_INCREMENT = 250 // millis
const MAX_RETRIES = 30

const KINESIS_SHARDS = 32
const KINESIS_RECORD_SIZE = (100 * 1024) // 100 KB
const KINESIS_SHARD_CAPACITY = KINESIS_SHARDS * 1024 * 1024
const KINESIS_PUT_RECORDS_SIZE_LIMIT = math.min(KINESIS_SHARD_CAPACITY, 5 * 1024 * 1024) // 5 MB;
const KINESIS_PUT_RECORDS_BATCH_SIZE = math.max(1, math.floor(KINESIS_PUT_RECORDS_SIZE_LIMIT / KINESIS_RECORD_SIZE) - 1)
// const KINESIS_PARALLEL_PUT_LIMIT = Math.floor(KINESIS_SHARD_CAPACITY / KINESIS_PUT_RECORDS_SIZE_LIMIT);

func (k *KinesisOutput) InitAWS() aws.Config {

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
    var creds *credentials.Credentials

    k.config = config.(*KinesisOutputConfig)
    k.batchedData = []byte {}
    k.batchedEntries = []*kin.PutRecordsRequestEntry {}

    k.awsConf = k.InitAWS()
    
    k.Client = kin.New(k.awsConf)

    return nil
}

func (k *KinesisOutput) SendEntries(or pipeline.OutputRunner, entries []*kin.PutRecordsRequestEntry, backoff int, retries int) error {
    multParams := &kin.PutRecordsInput{
        Records:      entries,
        StreamName:   aws.String(k.config.Stream),
    }

    _, err := k.Client.PutRecords(multParams)
    
    // Update statistics & handle errors
    if err != nil {
        if (or != nil) {
            or.LogError(fmt.Errorf("Batch: Error pushing message to Kinesis: %s", err))
        }
        atomic.AddInt64(&k.batchesFailed, 1)
        atomic.AddInt64(&k.dropMessageCount, int64(len(entries)))

        if (retries <= MAX_RETRIES) {
            atomic.AddInt64(&k.retryCount, 1)
            time.Sleep(time.Millisecond * backoff)
            SendEntries(or, entries, backoff + BACKOFF_INCREMENT, retries + 1)    
        } else {
            or.LogError(fmt.Errorf("Batch: Hit max retries when attempting to send data"))
        }
    }

    atomic.AddInt64(&k.batchesSent, 1)

    return nil
}

func (k *KinesisOutput) BundleMessage(msg []byte) kin.PutRecordsRequestEntry {
    // define a Partition Key
    pk := fmt.Sprintf("%X", rand.Int())

    // Add things to the current batch.
    return &kin.PutRecordsRequestEntry {
        Data:            msg,
        PartitionKey:    aws.String(pk),
    }
}

func (k *KinesisOutput) AddToRecordBatch(msg []byte) {
    entry := BundleMessage(msg)

    tmp := append(k.batchedEntries, entry)

    // if we have hit the batch limit, send.
    if (len(tmp) > KINESIS_PUT_RECORDS_BATCH_SIZE) {
        // clone the entries so the output can happen
        clonedEntries := make([]*kin.PutRecordsRequestEntry, len(k.batchedEntries))
        copy(clonedEntries, k.batchedEntries)

        // Run the put async
        go k.SendEntries(or, clonedEntries, 0, 0)

        k.batchedEntries = []*kin.PutRecordsRequestEntry { entry }
    } else {
        k.batchedEntries = tmp
    }

    // do Reporting
    atomic.AddInt64(&k.recordCount, 1)
}

func (k *KinesisOutput) HandlePackage(or pipeline.OutputRunner, pack *pipeline.PipelinePack) error {

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
        tmp = append(k.batchedData, []byte(","), msg)
    } else {
        tmp = msg
    }

    // if we can't fit the data in this record
    if (len(tmp) > KINESIS_RECORD_SIZE) {
        // add the existing data to the output batch
        array := append([]byte("["), k.batchedData, []byte("]"))
        AddToRecordBatch(array)

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

func init() {
    pipeline.RegisterPlugin("KinesisOutput", func() interface{} { return new(KinesisOutput) })
}

// func (k *KinesisOutput) FlushData() {
//     BundleMessage(k.batchedData)
// }

func (k *KinesisOutput) CleanupForRestart() {

    // force flush all messages in memory.
    //SendEntries(nil, k.batchedEntries)

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