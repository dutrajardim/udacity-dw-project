
def sparkifydwh_role_delete (session):
    iam_client = session.client('iam')

    role_name = 'sparkifydwh_role'
    s3_read_only_arn = 'arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'

    iam_client.detach_role_policy(RoleName=role_name, PolicyArn=s3_read_only_arn)
    iam_client.delete_role(RoleName=role_name)
