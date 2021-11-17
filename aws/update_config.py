
import os
import configparser

def update_config (session):
    iam_client = session.client('iam')
    config = configparser.ConfigParser()

    role_name = 'sparkifydwh_role'
    filepath = os.path.join('..', 'dwh.cfg')     

    config.read_file(open(filepath))

    try:
        resp = iam_client.get_role(RoleName=role_name)

        config['IAM_ROLE']['ARN'] = resp['Role']['Arn']
    except iam_client.exceptions.NoSuchEntityException as error:
        config['IAM_ROLE']['ARN'] = ''

    config.write(open(filepath, 'w'))
