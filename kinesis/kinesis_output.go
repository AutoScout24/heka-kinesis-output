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
)

type KinesisOutput struct {
    batchesSent         int64
    batchesFailed       int64
    processMessageCount int64
    dropMessageCount    int64
    reportLock          sync.Mutex
    entries             []*kin.PutRecordsRequestEntry {}
    config              *KinesisOutputConfig
    Client              *kin.Kinesis
}

type KinesisOutputConfig struct {
    Region          string `toml:"region"`
    Stream          string `toml:"stream"`
    AccessKeyID     string `toml:"access_key_id"`
    SecretAccessKey string `toml:"secret_access_key"`
    Token           string `toml:"token"`
    PayloadOnly     bool   `toml:"payload_only"`
    BatchNum        int    `toml:"batch_num"`
}

func (k *KinesisOutput) ConfigStruct() interface{} {
    return &KinesisOutputConfig{
        Region:          "us-east-1",
        Stream:          "",
        AccessKeyID:     "",
        SecretAccessKey: "",
        Token:           "",
    }
}


var awsConf aws.Config

func (k *KinesisOutput) Init(config interface{}) error {
    var creds *credentials.Credentials

    k.config = config.(*KinesisOutputConfig)
    k.entries = []*kin.PutRecordsRequestEntry {}

    if k.config.AccessKeyID != "" && k.config.SecretAccessKey != "" {
        creds = credentials.NewStaticCredentials(k.config.AccessKeyID, k.config.SecretAccessKey, "")
    } else {
        creds = credentials.NewEC2RoleCredentials(&http.Client{Timeout: 10 * time.Second}, "", 0)
    }
    awsConf = &aws.Config{
        Region:      k.config.Region,
        Credentials: creds,
    }
    k.Client = kin.New(awsConf)

    return nil
}

func (k *KinesisOutput) SendPayload(entries []*kin.PutRecordsRequestEntry, or pipeline.OutputRunner) error {
    multParams := &kin.PutRecordsInput{
        Records:      entries,
        StreamName:   aws.String(k.config.Stream),
    }

    req, _ := k.Client.PutRecordsRequest(multParams)
    err := req.Send()
    
    if err != nil {
        or.LogError(fmt.Errorf("Batch: Error pushing message to Kinesis: %s", err))
    }
    return nil
}

func (k *KinesisOutput) SendEntries(or pipeline.OutputRunner, entries []*kin.PutRecordsRequestEntry) error {
    multParams := &kin.PutRecordsInput{
        Records:      entries,
        StreamName:   aws.String(k.config.Stream),
    }

    req, _ := k.Client.PutRecordsRequest(multParams)
    atomic.AddInt64(&k.batchesSent, 1)
    err := req.Send()
    
    // Update statistics & handle errors
    if err != nil {
        or.LogError(fmt.Errorf("Batch: Error pushing message to Kinesis: %s", err))
        atomic.AddInt64(&k.batchesFailed, 1)
        atomic.AddInt64(&t.dropMessageCount, len(entries))
    }
}

func (k *KinesisOutput) Run(or pipeline.OutputRunner, helper pipeline.PluginHelper) error {
    var (
        pack       *pipeline.PipelinePack
        msg        []byte
        pk         string
        err        error
        entries    []*kin.PutRecordsRequestEntry
    )

    if or.Encoder() == nil {
        return fmt.Errorf("Encoder required.")
    }

    // configure defaults
    if (k.config.BatchNum <= 0 || k.config.BatchNum > 500) {
        return fmt.Errorf("`batch_num` should be greater than 0 and no greater than 500. See: https://docs.aws.amazon.com/sdk-for-go/api/service/kinesis/Kinesis.html#PutRecords-instance_method")
    }

    for pack = range or.InChan() {
        // Run the body of the loop async.
        msg, err = or.Encode(pack)
        if err != nil {
            or.LogError(fmt.Errorf("Error encoding message: %s", err))
            pack.Recycle(nil)
            continue
        }

        pk = fmt.Sprintf("%d-%s", pack.Message.Timestamp, pack.Message.Hostname)
        if k.config.PayloadOnly {
            msg = []byte(pack.Message.GetPayload())
        }

        // Add things to the current batch.
        entry := &kin.PutRecordsRequestEntry{
            Data:            msg,
            PartitionKey:    aws.String(pk),
        }

        entries = append(entries, entry)

        // if we have hit the batch limit send.
        if (len(entries) >= k.config.BatchNum) {
            clonedEntries := make([]*kin.PutRecordsRequestEntry, len(k.entries))
            copy(clonedEntries, k.entries)
            k.entries = []*kin.PutRecordsRequestEntry {}

            // Run the put async
            go k.SendEntries(or, clonedEntries)
        }

        atomic.AddInt64(&k.processMessageCount, 1)
        pack.Recycle(nil)
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
    
    return nil
}

func init() {
    pipeline.RegisterPlugin("KinesisOutput", func() interface{} { return new(KinesisOutput) })
}
