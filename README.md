# heka-kinesis-output

## Installation
Follow the [guide from the heka documentation to build with external plugins][3].

You also need to include the official aws-sdk-go by adding the following below the `goamz` clone in `cmake/externals.cmake`:
```bash
git_clone(https://github.com/vaughan0/go-ini a98ad7ee00ec53921f08832bc06ecf7fd600e6a1)
git_clone(https://github.com/aws/aws-sdk-go 90a21481e4509c85ee68b908c72fe4b024311447)
add_dependencies(aws-sdk-go go-ini)
```
You can then specify the Kinesis output plugin:
```bash
add_external_plugin(git https://github.com/MattLTW/heka-kinesis-output master)
```

## Detail
This output will put your [heka][1] messages and put them into a [Kinesis][2] stream. It will do this in batches. It is designed to batch the content in to the largest possible single put records request. This allows for the highest possible throughput with the least amount of push overhead.

This plugin batches in two stages. First it batches multiple messages in a single Kinesis record entry. Each Message will be placed into a JSON array inside a single record. 

> **Note:** This plugin expects that your message encoder, encodes as JSON.

The plugin will build log entries up until the record size exceeds `kinesis_record_size` (100KB). 

It will then build the record and place it in a buffer to be sent out. Once we have at most 5MB of data (less depending upon your number of kinesis shards) we send the data through to kinesis asynchronously. Each batch can be [at most 5M and individual messages can be at most 1M][4]. 

> **Note:** Batching will be sent once 5MB (or your max for your shard count) has been reached.

You can set the `ticker_interval` parameter allows you to flush more regularly if your data takes a while to reach 5MB.

If you choose to leave your access key and secret access key out then it will inherit the EC2 role credentials.

## Parameters

\* - Required parameters

`region*` - The AWS region your kinesis stream is in. 

`stream*` - The name of your kinesis stream.

`kinesis_shard_count*` - The number of Kinesis shards your stream is using. This is used to work out the maximum number of records your stream can handle.

`access_key_id` - Your AWS Key ID.

`secret_access_key` - Your AWS Secret Key.

`payload_only` - Should the entire encoded message be sent or only the payload? Default `false`.

`max_retries` - On failure how many times should this retry until? -1 means retry forever. Default `30`.

`backoff_increment` - In between each retry we will increase the timeout duration by. Default `250ms`

`kinesis_record_size` - The desired size of individual kinesis records. Default `102400` (100KB)

`ticker_interval` - The time (in seconds) that the plugin will attempt to flush the buffers. Useful on low frequency streams that need to be kept up to date. If the plugin has tried to attempt to send data within that timer then it will skip the flush. If left blank this flush functionality is disabled.


## Example configuration:

```ini
[KinesisOut]
type = "KinesisOutput"
message_matcher = "TRUE"

region = "us-east-1"
stream = "foobar"
kinesis_shard_count = 32

access_key_id = "AKIAJ89854WHHJDF8HJF"
secret_access_key = "JKLjkldfjklsdfjkls+d8u8954hjkdfkfdfgfj"
payload_only = false
max_retries = -1
backoff_increment = "500ms"
kinesis_record_size = 256000 # 256Kb
ticker_interval = 5
```

  [1]: https://hekad.readthedocs.org/en/latest/index.html
  [2]: https://aws.amazon.com/kinesis/
  [3]: http://hekad.readthedocs.org/en/latest/installing.html#building-hekad-with-external-plugins
  [4]: https://docs.aws.amazon.com/sdk-for-go/api/service/kinesis/Kinesis.html#PutRecords-instance_method
