import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as apigw from 'aws-cdk-lib/aws-apigateway';
import * as path from 'path';
import { Bucket } from 'aws-cdk-lib/aws-s3';
import { Table } from 'aws-cdk-lib/aws-dynamodb';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as lambdaEventSources from 'aws-cdk-lib/aws-lambda-event-sources';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as cloudwatch from 'aws-cdk-lib/aws-cloudwatch';
import * as cw_actions from 'aws-cdk-lib/aws-cloudwatch-actions';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as subs from 'aws-cdk-lib/aws-sns-subscriptions';
import * as iam from 'aws-cdk-lib/aws-iam';

const lambdaCodePath = path.resolve(__dirname, '..', 'lambda-code');

export interface AppStackProps extends cdk.StackProps {
  bucket: Bucket;
  table: Table;
  sizeTrackingQueue: sqs.Queue;
  loggingQueue: sqs.Queue;
}

export class AppStack extends cdk.Stack {
  public readonly plottingApiUrl: string;

  constructor(scope: Construct, id: string, props: AppStackProps) {
    super(scope, id, props);

    const { bucket, table, sizeTrackingQueue, loggingQueue } = props;

    //
    // 1) Size-tracking Lambda, now consuming from SQS
    //
    const sizeTrackingFn = new lambda.Function(this, 'SizeTrackingFn', {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'size_tracking_lambda.lambda_handler',
      code: lambda.Code.fromAsset(lambdaCodePath),
      environment: {
        BUCKET_NAME: bucket.bucketName,
        TABLE_NAME: table.tableName,
      },
      timeout: cdk.Duration.seconds(30),
    });

    // Permissions
    bucket.grantRead(sizeTrackingFn);  
    table.grantWriteData(sizeTrackingFn);

    sizeTrackingFn.addEventSource(
      new lambdaEventSources.SqsEventSource(sizeTrackingQueue, {
        batchSize: 10,
      }),
    );

    //
    // 2) Plotting Lambda using API Gateway  
    //
    const plottingFn = new lambda.Function(this, 'PlottingFn', {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'plotting_lambda.lambda_handler',
      code: lambda.Code.fromAsset(lambdaCodePath),
      environment: {
        BUCKET_NAME: bucket.bucketName,
        TABLE_NAME: table.tableName,
        WINDOW_SECONDS: '30',  
      },
      timeout: cdk.Duration.seconds(30),
      layers: [
        lambda.LayerVersion.fromLayerVersionArn(
          this,
          'MatplotlibLayer',
          'arn:aws:lambda:us-east-1:256047881985:layer:matplotlib-py312:4',
        ),
      ],
    });

    bucket.grantReadWrite(plottingFn);
    table.grantReadData(plottingFn);

    const api = new apigw.LambdaRestApi(this, 'PlottingApi', {
      handler: plottingFn,
      proxy: true,
    });

    this.plottingApiUrl = api.url;

    new cdk.CfnOutput(this, 'PlottingApiUrl', {
      value: this.plottingApiUrl,
    });

    //
    // 3) Logging Lambda: consumes SQS and writes JSON logs with size_delta
    //
    const loggingFn = new lambda.Function(this, 'LoggingFn', {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'logging_lambda.lambda_handler',
      code: lambda.Code.fromAsset(lambdaCodePath),
      timeout: cdk.Duration.seconds(30),
       
    });

    loggingFn.addEventSource(
      new lambdaEventSources.SqsEventSource(loggingQueue, {
        batchSize: 10,
      }),
    );

    // Allow loggingFn to query its own logs via filter_log_events
    loggingFn.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ['logs:FilterLogEvents'],
        resources: ['*'],
      }),
    );

    //
    // 4) CloudWatch Metric Filter over logging lambda's log group
    //
    const metricFilter = new logs.MetricFilter(this, 'SizeDeltaMetricFilter', {
      logGroup: loggingFn.logGroup,
      // match any JSON log line that has a size_delta field
      filterPattern: logs.FilterPattern.exists('$.size_delta'),
      metricNamespace: 'Assignment4App',
      metricName: 'TotalObjectSize',
      metricValue: '$.size_delta',
    });

    const totalSizeMetric = metricFilter.metric({
      statistic: 'sum',
      period: cdk.Duration.seconds(30), 
    });

    //
    // 5) Alarm + Cleaner lambda, alarm action -> SNS -> Cleaner
    //
    const cleanerFn = new lambda.Function(this, 'CleanerFn', {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'cleaner_lambda.lambda_handler',
      code: lambda.Code.fromAsset(lambdaCodePath),
      environment: {
        BUCKET_NAME: bucket.bucketName,
      },
      timeout: cdk.Duration.seconds(30),
    });

    // Cleaner needs read/write on bucket to find & delete largest object
    bucket.grantReadWrite(cleanerFn);

    const totalSizeAlarm = new cloudwatch.Alarm(
      this,
      'TotalObjectSizeAlarm',
      {
        metric: totalSizeMetric,
        threshold: 20,
        evaluationPeriods: 1,
        comparisonOperator:
          cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
      },
    );

    // Alarm action via SNS topic subscribed by Cleaner lambda
    const cleanerAlarmTopic = new sns.Topic(this, 'CleanerAlarmTopic');
    totalSizeAlarm.addAlarmAction(
      new cw_actions.SnsAction(cleanerAlarmTopic),
    );
    cleanerAlarmTopic.addSubscription(
      new subs.LambdaSubscription(cleanerFn),
    );
  }
}
