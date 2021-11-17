
import configparser
import os

def redshift_cluster_create(session):
    redshift_client = session.client('redshift')
    config = configparser.ConfigParser()

    filepath = os.path.join('..', 'dwh.cfg')
    config.read_file(open(filepath))

    resp = None

    NUM_NODES = int(config.get('DWH', 'NUM_NODES'))
    params = {
        'NodeType': config.get('DWH', 'NODE_TYPE'),
        'ClusterIdentifier': config.get('DWH', 'CLUSTER_IDENTIFIER'),
    
        'DBName': config.get('DB', 'NAME'),
        'Port': int(config.get('DB', 'PORT')),
        'MasterUsername': config.get('DB', 'USERNAME'),
        'MasterUserPassword': config.get('DB', 'PASSWORD'),
        'IamRoles': [config.get('IAM_ROLE', 'ARN')]
    }

    if NUM_NODES > 1:
        params['NumberOfNodes'] = NUM_NODES
        params['ClusterType'] = 'multi-node'
    else:
        params['ClusterType'] = 'single-node'

    try:
        resp = redshift_client.create_cluster(**params)
    except Exception as error:
        print(error)

    return resp
