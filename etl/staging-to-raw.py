import boto3


def lambda_handler(event, context):
    s3_resource = boto3.resource('s3',
                                 aws_access_key_id='',
                                 aws_secret_access_key='',
                                 region_name='us-east-2')
    s3_client = boto3.client('s3',
                             aws_access_key_id='',
                             aws_secret_access_key='',
                             region_name='us-east-2')

    new_bucket_name = "dcb-raw-zone"
    bucket_to_copy = "dcb-staging-zone"

    for key in s3_client.list_objects(Bucket=bucket_to_copy)['Contents']:
        files = key['Key']
        copy_source = {'Bucket': bucket_to_copy, 'Key': files}
        s3_resource.meta.client.copy(copy_source, new_bucket_name, files)
        print(files)

    response = s3_client.list_objects_v2(Bucket=bucket_to_copy)
    files_in_folder = response["Contents"]
    files_to_delete = []

    for f in files_in_folder:
        files_to_delete.append({"Key": f["Key"]})

    response = s3_client.delete_objects(
        Bucket=bucket_to_copy, Delete={"Objects": files_to_delete}
    )

    print(response)


if __name__ == '__main__':
    lambda_handler('', '')
