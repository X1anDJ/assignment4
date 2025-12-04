import * as cdk from 'aws-cdk-lib';
import { RemovalPolicy } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as subs from 'aws-cdk-lib/aws-sns-subscriptions';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as s3n from 'aws-cdk-lib/aws-s3-notifications';

export class StorageStack extends cdk.Stack {
  public readonly bucket: s3.Bucket;
  public readonly table: dynamodb.Table;
  public readonly sizeTrackingQueue: sqs.Queue;
  public readonly loggingQueue: sqs.Queue;

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // S3 bucket
    this.bucket = new s3.Bucket(this, 'TestBucket', {
      removalPolicy: RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      eventBridgeEnabled: false,
    });

    // DynamoDB table
    this.table = new dynamodb.Table(this, 'SizeHistoryTable', {
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      partitionKey: { name: 'bucket', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'timestamp', type: dynamodb.AttributeType.NUMBER },
      removalPolicy: RemovalPolicy.DESTROY,
    });

    this.table.addGlobalSecondaryIndex({
      indexName: 'BucketSizeIndex',
      partitionKey: { name: 'bucket', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'size_bytes', type: dynamodb.AttributeType.NUMBER },
      projectionType: dynamodb.ProjectionType.ALL,
    });

    // SNS topic for S3 events (fan-out)
    const s3EventsTopic = new sns.Topic(this, 'S3EventsTopic');

    // SQS queues for consumers
    this.sizeTrackingQueue = new sqs.Queue(this, 'SizeTrackingQueue', {
      visibilityTimeout: cdk.Duration.seconds(30),
    });

    this.loggingQueue = new sqs.Queue(this, 'LoggingQueue', {
      visibilityTimeout: cdk.Duration.seconds(30),
    });

    // Subscriptions: SNS -> SQS
    s3EventsTopic.addSubscription(
      new subs.SqsSubscription(this.sizeTrackingQueue),
    );
    s3EventsTopic.addSubscription(
      new subs.SqsSubscription(this.loggingQueue),
    );

    // S3 notifications -> SNS
    this.bucket.addEventNotification(
      s3.EventType.OBJECT_CREATED_PUT,
      new s3n.SnsDestination(s3EventsTopic),
    );
    this.bucket.addEventNotification(
      s3.EventType.OBJECT_REMOVED_DELETE,
      new s3n.SnsDestination(s3EventsTopic),
    );
  }
}
