# Kinesis Stream

A wrapper around KCL 2.x which exposes an Akka Streams source to consume messages off of a Kinesis Data Stream.


## Required Policies

The following IAM permissions are required to use KCL 2.x

```
// Resource: Stream
kinesis:RegisterStreamConsumer
kinesis:DescribeStreamSummary
kinesis:ListShards
kinesis:DescribeStream
kinesis:GetShardIterator
kinesis:GetRecords
kinesis:PutRecord
kinesis:PutRecords

// Resource: "*"
kinesis:ListStreams

// Resource: Stream Consumer
kinesis:SubscribeToShard
kinesis:DescribeStreamConsumer

```
