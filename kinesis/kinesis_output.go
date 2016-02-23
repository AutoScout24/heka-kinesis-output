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
    Batch           bool   `toml:"batch"`
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

func (k *KinesisOutput) Run(or pipeline.OutputRunner, helper pipeline.PluginHelper) error {
    var (
        pack       *pipeline.PipelinePack
        msg        []byte
        pk         string
        err        error
        params     *kin.PutRecordInput
        multParams *kin.PutRecordsInput
        entries    []*kin.PutRecordsRequestEntry
        iterCount  int
    )

    if or.Encoder() == nil {
        return fmt.Errorf("Encoder required.")
    }

    // configure defaults
    if (k.config.Batch) {

        if (k.config.BatchNum == 0) {
            return fmt.Errorf("`batch_num` should be greater than 0.")
        }

        if (k.config.BatchNum > 500) {
            return fmt.Errorf("`batch_num` should be greater no greater than 500. See: https://docs.aws.amazon.com/sdk-for-go/api/service/kinesis/Kinesis.html#PutRecords-instance_method")
        }

        entries = make([]*kin.PutRecordsRequestEntry, k.config.BatchNum)
    }

    for pack = range or.InChan() {
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

        if (k.config.Batch) {
            // Add things to the current batch.
            entries[iterCount] = &kin.PutRecordsRequestEntry{
                Data:            msg,
                PartitionKey:    aws.String(pk),
            }
            iterCount = iterCount + 1

            // if we have hit the batch limit send.
            if (iterCount > k.config.BatchNum) {
                multParams = &kin.PutRecordsInput{
                    Records:      entries,
                    StreamName:   aws.String(k.config.Stream),
                }

                req, _ := k.Client.PutRecordsRequest(multParams)
                err := req.Send()

                // reset variants
                iterCount = 0
                entries = make([]*kin.PutRecordsRequestEntry, k.config.BatchNum)
                
                if err != nil {
                    or.LogError(fmt.Errorf("Batch: Error pushing message to Kinesis: %s", err))
                    pack.Recycle(nil)
                    continue
                }
            }
        } else {
            // handle sequential input
            params = &kin.PutRecordInput{
                Data:         msg,
                PartitionKey: aws.String(pk),
                StreamName:   aws.String(k.config.Stream),
            }
            _, err = k.Client.PutRecord(params)
            if err != nil {
                or.LogError(fmt.Errorf("Sequence: Error pushing message to Kinesis: %s", err))
                pack.Recycle(nil)
                continue
            }
        }
        
        pack.Recycle(nil)
    }

    return nil
}

func init() {
    pipeline.RegisterPlugin("KinesisOutput", func() interface{} { return new(KinesisOutput) })
}
