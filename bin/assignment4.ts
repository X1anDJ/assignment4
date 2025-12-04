#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { StorageStack } from '../lib/storage-stack';
import { AppStack } from '../lib/app-stack';
import { DriverStack } from '../lib/driver-stack';

const app = new cdk.App();

// 1. storage (bucket + table + SNS + SQS)
const storage = new StorageStack(app, 'StorageStack', {
  env: { region: 'us-east-1' },
});

// 2. app logic (size-tracking, plotting, logging, cleaner)
const appLogic = new AppStack(app, 'AppStack', {
  env: { region: 'us-east-1' },
  bucket: storage.bucket,
  table: storage.table,
  sizeTrackingQueue: storage.sizeTrackingQueue,
  loggingQueue: storage.loggingQueue,
});

// 3. driver  
new DriverStack(app, 'DriverStack', {
  env: { region: 'us-east-1' },
  bucket: storage.bucket,
  plottingApiUrl: appLogic.plottingApiUrl,
});
