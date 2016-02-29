# heka-kinesis-output

## Installation
Follow the [guide from the heka documentation to build with external plugins][3].

You also need to include the official aws-sdk-go by adding the following below the `goamz` clone in `cmake/externals.cmake`:
```bash
git_clone(https://github.com/vaughan0/go-ini a98ad7ee00ec53921f08832bc06ecf7fd600e6a1)
git_clone(https://github.com/aws/aws-sdk-go 90a21481e4509c85ee68b908c72fe4b024311447)
add_dependencies(aws-sdk-go go-ini)
```

If you do not need all of the plugins from this repository, you can specify specific ones:
```bash
add_external_plugin(git https://github.com/MattLTW/heka-plugins master)
```

## Kinesis
This output will put your [heka][1] messages and put them into a [Kinesis][2] stream. It will do this in batches.

This plugin batches in two stages. First it batches multiple messages in a single Kinesis record entry. Each Message will be placed into a JSON array inside a single record. The size of the array depends upon the `kinesis_record_size` parameter. Next it will batch multiple Record Entries in to a single Kinesis.PutRecords command.

> **Note:** This plugin expects that your message encoder, encodes as JSON.

The plugin will build record entries up until the record size exceeds 100KB. It will then buffer an entry just smaller than 100KB.

Once we have at most 5MB of data (less depending upon your number of kinesis shards) we send the data through to kinesis asynchronously.

Each batch can be [at most 5M and individual messages can be at most 1M][4]. 

> **Note** Batching will only be sent once 5MB (or your max for your shard count) has been reached. Therefore it is only really applicable to high frequency messages. **This plugin will introduce latency in to your logs.**

If you choose to leave your access key and secret access key out then it will inherit the EC2 role credentials.

## Parameters

\* - Required parameters

`region`* - The AWS region your kinesis stream is in. 

`stream`* - The name of your kinesis stream.

`kinesis_shard_count`* - The number of Kinesis shards your stream is using. This is used to work out the maximum number of records your stream can handle.

`access_key_id` - Your AWS Key ID.

`secret_access_key` - Your AWS Secret Key.

`payload_only` - Should the entire encoded message be sent or only the payload? Default `false`.

`max_retries` - On failure how many times should this retry until? -1 means retry forever. Default `30`.

`backoff_increment` - In between each retry we will increase the timeout duration by. Default `250ms`

`kinesis_record_size` - The desired size of individual kinesis records. Default `102400` (100KB)


Example configuration

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
```

  [1]: https://hekad.readthedocs.org/en/latest/index.html
  [2]: https://aws.amazon.com/kinesis/
  [3]: http://hekad.readthedocs.org/en/latest/installing.html#building-hekad-with-external-plugins
  [4]: https://docs.aws.amazon.com/sdk-for-go/api/service/kinesis/Kinesis.html#PutRecords-instance_method
