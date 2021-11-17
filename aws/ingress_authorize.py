
import configparser
import os

def ingress_authorize (session):
    ec2_client = session.client('ec2')
    redshift_client = session.client('redshift')
    config = configparser.ConfigParser()

    filepath = os.path.join('..', 'dwh.cfg')
    config.read_file(open(filepath))
    
    PORT = int(config.get('DB', 'PORT'))

    try:
        resp = redshift_client.describe_clusters(ClusterIdentifier='dwhcluster')
        cluster_props = resp['Clusters'][0]

        vpc_filter = [
            { 'Name': 'vpc-id', 'Values': [cluster_props['VpcId']] }
        ]
        
        resp = ec2_cliente.describe_security_groups(Filters=vpc_filter)
        default_sg = resp['SecurityGroups'][0]

        ec2_client.authorize_security_group_ingress(
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=PORT,
            ToPort=PORT,
            GroupId=default_sg['GroupId']
        )
        
    except Exception as error:
        print(error)
