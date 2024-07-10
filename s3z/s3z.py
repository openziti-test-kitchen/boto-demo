#!/usr/bin/env python

import argparse
import boto3
import openziti
import os

def configure_openziti(ziti_identity_file, bucket_endpoint):
    print(f"Configuring openziti with service name: {ziti_identity_file}."
          f"Ensure Dial Service Policy grants intercept '{bucket_endpoint}'.")
    return openziti.load(ziti_identity_file)


def push_logs_to_s3(bucket_name, bucket_endpoint, push_log_dir, object_prefix):
    # Create a boto3 S3 client
    s3_client = boto3.client('s3', endpoint_url=bucket_endpoint)

    # Iterate over the files in the directory
    for file_name in os.listdir(push_log_dir):
        if file_name.endswith(".log"):
            file_path = os.path.join(push_log_dir, file_name)
            # Upload the file to the S3 bucket
            with openziti.monkeypatch():
                if object_prefix:
                    s3_client.upload_file(file_path, bucket_name, f"{object_prefix}/{file_name}")
                else:
                    s3_client.upload_file(file_path, bucket_name, file_name)


if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--ziti-identity-file', required=True, help='Ziti identity file')
    parser.add_argument('--region', required=True, help='AWS region')
    parser.add_argument('--bucket-name', required=True, help='bucket name')
    parser.add_argument('--bucket-endpoint', required=True, help='private bucket endpoint')
    parser.add_argument('--object-prefix', required=False, default='', help='object key prefix in bucket for pushed files')
    parser.add_argument('--push-log-dir', required=False, default='.', help='directory with *.log files to push to bucket')
    args = parser.parse_args()

    # Configure openziti
    configure_openziti(args.ziti_identity_file, args.bucket_endpoint)

    # Push logs to bucket
    push_logs_to_s3(args.bucket_name, args.bucket_endpoint, args.push_log_dir, args.object_prefix)
