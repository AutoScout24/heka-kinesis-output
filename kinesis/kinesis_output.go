package kinesis

import (
    "fmt"
    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/credentials"
    kin "github.com/aws/aws-sdk-go/service/kinesis"
    "github.com/mozilla-services/heka/pipeline"
    "net/http"
    "time"
)

type KinesisOutput struct {
    config *KinesisOutputConfig
    Client *kin.Kinesis
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

func (k *KinesisOutput) Init(config interface{}) error {
    var creds *credentials.Credentials

    k.config = config.(*KinesisOutputConfig)

    if k.config.AccessKeyID != "" && k.config.SecretAccessKey != "" {
        creds = credentials.NewStaticCredentials(k.config.AccessKeyID, k.config.SecretAccessKey, "")
    } else {
        creds = credentials.NewEC2RoleCredentials(&http.Client{Timeout: 10 * time.Second}, "", 0)
    }
    conf := &aws.Config{
        Region:      k.config.Region,
        Credentials: creds,
    }
    k.Client = kin.New(conf)

    return nil
}

func (k *KinesisOutput) SendPayload(entries []*kin.PutRecordsRequestEntry) {
    multParams = &kin.PutRecordsInput{
        Records:      entries,
        StreamName:   aws.String(k.config.Stream),
    }

    req, _ := k.Client.PutRecordsRequest(multParams)
    err := req.Send()
    
    if err != nil {
        or.LogError(fmt.Errorf("Batch: Error pushing message to Kinesis: %s", err))
    }
}

func (k *KinesisOutput) Run(or pipeline.OutputRunner, helper pipeline.PluginHelper) error {
    var (
        pack       *pipeline.PipelinePack
        msg        []byte
        pk         string
        err        error
        params     *kin.PutRecordInput
        multParams *kin.PutRecordsInput
        entries    []*kin.PutRecordsRequestEntry
    )

    if or.Encoder() == nil {
        return fmt.Errorf("Encoder required.")
    }

    // configure defaults
    if (k.config.BatchNum <= 0 || k.config.BatchNum > 500) {
        return fmt.Errorf("`batch_num` should be greater than 0 and no greater than 500. See: https://docs.aws.amazon.com/sdk-for-go/api/service/kinesis/Kinesis.html#PutRecords-instance_method")
    }

    entries = []*kin.PutRecordsRequestEntry {}

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
            clonedEntries := append([]*kin.PutRecordsRequestEntry(nil), entries)
            entries = []*kin.PutRecordsRequestEntry {}
            go SendPayload(clonedEntries)
        }   

        pack.Recycle(nil)
    }

    return nil
}

func init() {
    pipeline.RegisterPlugin("KinesisOutput", func() interface{} { return new(KinesisOutput) })
}
