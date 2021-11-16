
def sparkifydwh_role_create (session):
    iam_client = session.client('iam')

    role_name = 'sparkifydwh_role'
    s3_read_only_arn = 'arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
    sparkifydwh_role = None

    try:
        sparkifydwh_role = iam_client.get_role(RoleName=role_name)
        print('Role already exists.')
    except iam_client.exceptions.NoSuchEntityException as error:
        sparkifydwh_role = iam_client.create_role(
            RoleName=role_name,
            Description='Define the aws accesses for the sparkify data warehouse',
            AssumeRolePolicyDocument=json.dumps({
                "Version": "2012-10-17",
                "Statement": {
                    "Effect": "Allow",
                    "Action": "sts:AssumeRole",
                    "Principal": {
                        "Service": "redshift.amazonaws.com"
                    }
                }
            })
        )

        print('Role created.')
    except Exception as error:
        print(error)
    
    iam_client.attach_role_policy(RoleName=role_name, PolicyArn=s3_read_only_arn)
    
    return sparkifydwh_role['Role']['Arn'], role_name 
