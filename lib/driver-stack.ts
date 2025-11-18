import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as path from 'path';
import { Bucket } from 'aws-cdk-lib/aws-s3';

export interface DriverStackProps extends cdk.StackProps {
  bucket: Bucket;
  plottingApiUrl: string;
}

export class DriverStack extends cdk.Stack {
  public readonly driverFn: lambda.Function;

  constructor(scope: Construct, id: string, props: DriverStackProps) {
    super(scope, id, props);

    const { bucket, plottingApiUrl } = props;

    this.driverFn = new lambda.Function(this, 'DriverFn', {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'driver_lambda.lambda_handler',
      code: lambda.Code.fromAsset(path.join(__dirname, '../lambda-code')),
      environment: {
        BUCKET_NAME: bucket.bucketName,
        PLOTTING_API_URL: plottingApiUrl,  
      },
      timeout: cdk.Duration.seconds(30),
    });

    // allow driver lambda to PUT/DELETE objects
    bucket.grantReadWrite(this.driverFn);
  }
}
