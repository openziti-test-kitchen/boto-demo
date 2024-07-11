#!/usr/bin/env python

import argparse
import os

import boto3
import openziti


def configure_openziti(ziti_identity_file, bucket_endpoint):
    print(f"Configuring openziti with identity file: {ziti_identity_file}.",
          f"Ensure service configs match regional bucket endpoint'.")
    return openziti.load(ziti_identity_file)


def push_logs_to_s3(bucket_name, bucket_endpoint, push_log_dir, object_prefix):
    if bucket_endpoint:
        s3 = boto3.client('s3', endpoint_url=bucket_endpoint)
    else:
        s3 = boto3.client('s3')

    for file_name in os.listdir(push_log_dir):
        if file_name.endswith(".log"):
            file_path = os.path.join(push_log_dir, file_name)
            with openziti.monkeypatch():
                if object_prefix:
                    s3.upload_file(file_path, bucket_name,
                                   f"{object_prefix}/{file_name}")
                    print(f"Uploaded {file_path} to"
                          f"{bucket_name}/{object_prefix}.")
                else:
                    s3.upload_file(file_path, bucket_name, file_name)
                    print(f"Uploaded {file_path} to {bucket_name}.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--ziti-identity-file', required=True,
                        help='Ziti identity file')
    parser.add_argument('--bucket-name', required=True,
                        help='bucket name')
    parser.add_argument('--bucket-endpoint', required=False, default='',
                        help='private bucket endpoint')
    parser.add_argument('--object-prefix', required=False, default='',
                        help='object key prefix in bucket for pushed files')
    parser.add_argument('--push-log-dir', required=False, default='.',
                        help='directory with *.log files to push to bucket')
    args = parser.parse_args()

    sts = boto3.client('sts')
    caller = sts.get_caller_identity()
    print(f"\nAuthenticated to AWS as:",
          f"UserId: {caller.get('UserId')}",
          f"Account: {caller.get('Account')}",
          f"Arn: {caller.get('Arn')}\n", sep="\n\t")

    configure_openziti(
        args.ziti_identity_file,
        args.bucket_endpoint
    )

    push_logs_to_s3(
        args.bucket_name,
        args.bucket_endpoint,
        args.push_log_dir,
        args.object_prefix
    )
