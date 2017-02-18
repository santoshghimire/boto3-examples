import os
from boto3 import client


class S3(object):
    """Example class demonstrating operations on S3"""

    bucket_name = "defaultbucket"

    def __init__(self, *args, **kwargs):
        region = self.kwargs.get('region_name', 'us-east-1')
        self.bucket_name = self.kwargs.get('bucket_name', self.bucket_name)
        self.conn = client('s3', region_name=region)

    def upload_to_s3(self, file_name, path=None):
        """
        Upload given file to s3.
        """
        s3_client = self.conn
        if path:
            full_path = os.path.join(path, file_name)
        else:
            full_path = file_name
        s3_client.upload_file(
            file_name, self.bucket_name, full_path.split('tmp/')[-1])

    def upload_object(self, body, s3_key):
        """
        Upload object to s3 key
        """
        s3_client = self.conn
        return s3_client.put_object(Body=body, Key=s3_key)

    def download_file(self, file_name):
        """
        Download a file given s3 prefix
        inside /tmp directory.
        Returns the download file path.
        """
        s3_client = self.conn
        file_name = file_name.split('tmp/')[-1]

        file_name_only = file_name.split('/')[-1]
        file_name_only_len = len(file_name_only)
        file_name_len = len(file_name)
        file_dir = '/tmp/' + file_name[0:file_name_len - file_name_only_len]
        if not os.path.exists(file_dir):
            os.makedirs(file_dir)
        try:
            s3_client.download_file(
                self.bucket_name, file_name, '/tmp/' + file_name)
            return file_name
        except:
            print('Cannot download file', file_name)
            return

    def get_s3_results(self, dir_name):
        """
        Return all contents of a given dir in s3.
        Goes through the pagination to obtain all file names.
        """
        dir_name = dir_name.split('tmp/')[-1]
        paginator = self.conn.get_paginator('list_objects')
        s3_results = paginator.paginate(
            Bucket=self.bucket_name,
            Prefix=dir_name,
            PaginationConfig={'PageSize': 1000}
        )
        bucket_object_list = []
        for page in s3_results:
            if "Contents" in page:
                for key in page["Contents"]:
                    s3_file_name = key['Key'].split('/')[-1]
                    bucket_object_list.append(s3_file_name)
        return bucket_object_list
