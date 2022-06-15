import logging                                  #for documentation
import os                                       #for handling directories and secrets
import boto3                                    #for handling aws resources
from botocore.exceptions import ClientError     #for logging errors


# Code adapted from https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-example-creating-buckets.html
def create_s3_bucket(session, bucket_name, region=None):
    """Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param bucket_name: Bucket to create
    :param region: String region to create bucket in, e.g., 'us-west-2'
    :return: True if bucket created, else False
    """

    # Create bucket
    try:
        if region is None:
            s3_client = session.client('s3')
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client = session.client('s3', region_name=region)
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration=location)
        logging.info('Bucket created')
    except ClientError as e:
        logging.error(e)
        
        return False

    return True

def create_folders(session, bucket_name, folder_names):
    """Create folders in designated s3 bucket
    """

    s3_client = session.client('s3')
    try:
        for folder_name in folder_names:
            s3_client.put_object(Bucket=bucket_name,Key=folder_name+'/')
            logging.info('Created folders: '+folder_name)
    except ClientError as e:
        logging.error(e)

path = os.path.dirname(os.path.realpath(__file__))

os.chdir(path)


with open('create_s3_buckets.log', 'w') as f:
    pass

logging.basicConfig(filename='create_s3_buckets.log', level=logging.INFO)

AK = os.getenv('AK')
SK = os.getenv('SK')

REGION = 'us-west-2'
BUCKET_NAME = 'rsimpao-aws-demonstration'
FOLDER_NAMES = ['input/','output/','code']


def main():

    session = boto3.Session(
        aws_access_key_id=AK,
        aws_secret_access_key=SK
    )
    
    create_s3_bucket(session,bucket_name=BUCKET_NAME,region=REGION)
    create_folders(session, bucket_name=BUCKET_NAME,folder_names=FOLDER_NAMES)

if __name__ == "__main__":
    main()