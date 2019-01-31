# Kinesis Stream

A wrapper around KCL 2.x which exposes an Akka Streams source to consume messages off of a Kinesis Data Stream.


## Required Policies

The following IAM permissions are required to use KCL 2.x

- `STREAM_ARN` - the ARN of the Kinesis stream being consumed from
- `DYNAMO_ARN` - the ARN of the DynamoDB table that the KCL will use for checkpointing
    - The name of the table will correspond to the `application name` set for the consumer
    
```json
{
    "PolicyName": "Kinesis-Consumer",
    "PolicyDocument": {
      "Version": "2012-10-17",
      "Statement": [
        {
          "Effect": "Allow",
          "Action": [
            "kinesis:RegisterStreamConsumer",
            "kinesis:DescribeStreamSummary",
            "kinesis:ListShards",
            "kinesis:DescribeStream",
            "kinesis:GetShardIterator",
            "kinesis:GetRecords",
            "kinesis:PutRecord",
            "kinesis:PutRecords"
          ],
          "Resource": [
            "{STREAM_ARN}"
          ]
        },
        {
          "Effect": "Allow",
          "Action": [
            "kinesis:SubscribeToShard",
            "kinesis:ListStreams",
            "kinesis:DescribeStreamConsumer"
          ],
          "Resource": [
            "*"
          ]
        },
        {
          "Effect": "Allow",
          "Action": [
            "dynamodb:*"
          ],
          "Resource": [
            "{DYNAMO_ARN}"
          ]
        },
        {
          "Effect": "Allow",
          "Action": [
            "cloudwatch:PutMetricData"
          ],
          "Resource": [
            "*"
          ]
        }
      ]
    }
  }
```
