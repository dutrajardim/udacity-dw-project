import configparser
import os
import boto3
import json
from time import sleep


class MyCluster:
    def __init__(self, filepath):
        print("AWS MyCluster: Creating session.")

        self.filepath = filepath

        config = configparser.ConfigParser()
        config.read_file(open(filepath))

        self.config = config

        KEY = config.get("AWS", "KEY")
        SECRET = config.get("AWS", "SECRET")

        session = boto3.session.Session(
            aws_access_key_id=KEY, aws_secret_access_key=SECRET, region_name="us-west-2"
        )

        self.redshift_client = session.client("redshift")
        self.iam_client = session.client("iam")
        self.ec2_client = session.client("ec2")

    def authorize_ingress(self, cluster_props):
        print("AWS MyCluster: Authorizing ingress.")

        PORT = int(self.config.get("DB", "PORT"))

        try:
            vpc_filter = [{"Name": "vpc-id", "Values": [cluster_props["VpcId"]]}]

            resp = self.ec2_cliente.describe_security_groups(Filters=vpc_filter)
            default_sg = resp["SecurityGroups"][0]

            self.ec2_client.authorize_security_group_ingress(
                CidrIp="0.0.0.0/0",
                IpProtocol="TCP",
                FromPort=PORT,
                ToPort=PORT,
                GroupId=default_sg["GroupId"],
            )

            return True
        except Exception as error:
            return False

    def redshift_cluster_create(self):
        print("AWS MyCluster: Creating cluster.")
        config = self.config
        resp = None

        NUM_NODES = int(config.get("DWH", "NUM_NODES"))
        params = {
            "NodeType": config.get("DWH", "NODE_TYPE"),
            "ClusterIdentifier": config.get("DWH", "CLUSTER_IDENTIFIER"),
            "DBName": config.get("DB", "DBNAME"),
            "Port": int(config.get("DB", "PORT")),
            "MasterUsername": config.get("DB", "USER"),
            "MasterUserPassword": config.get("DB", "PASSWORD"),
            "IamRoles": [config.get("IAM_ROLE", "ARN")],
        }

        if NUM_NODES > 1:
            params["NumberOfNodes"] = NUM_NODES
            params["ClusterType"] = "multi-node"
        else:
            params["ClusterType"] = "single-node"

        try:
            resp = self.redshift_client.create_cluster(**params)
        except Exception as error:
            print(error)

        return resp

    def sparkifydwh_role_create(self):
        print("AWS MyCluster: Creating role.")

        role_name = "sparkifydwh_role"
        s3_read_only_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        sparkifydwh_role = None

        try:
            sparkifydwh_role = self.iam_client.get_role(RoleName=role_name)

        except self.iam_client.exceptions.NoSuchEntityException as error:
            sparkifydwh_role = self.iam_client.create_role(
                RoleName=role_name,
                Description="Define the aws accesses for the sparkify data warehouse",
                AssumeRolePolicyDocument=json.dumps(
                    {
                        "Version": "2012-10-17",
                        "Statement": {
                            "Effect": "Allow",
                            "Action": "sts:AssumeRole",
                            "Principal": {"Service": "redshift.amazonaws.com"},
                        },
                    }
                ),
            )

            print("AWS MyCluster: Role created.")
        except Exception as error:
            print(error)

        self.iam_client.attach_role_policy(
            RoleName=role_name, PolicyArn=s3_read_only_arn
        )

        return sparkifydwh_role["Role"]["Arn"], role_name

    def sparkifydwh_role_delete(self):
        print("AWS MyCluster: Deleting session.")

        role_name = "sparkifydwh_role"
        s3_read_only_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"

        self.iam_client.detach_role_policy(
            RoleName=role_name, PolicyArn=s3_read_only_arn
        )
        self.iam_client.delete_role(RoleName=role_name)

    def update_role_config(self):
        print("AWS MyCluster: Updating config file (IAM_ROLE ARN).")

        role_name = "sparkifydwh_role"

        try:
            resp = self.iam_client.get_role(RoleName=role_name)

            self.config["IAM_ROLE"]["ARN"] = resp["Role"]["Arn"]
        except self.iam_client.exceptions.NoSuchEntityException as error:
            self.config["IAM_ROLE"]["ARN"] = ""

        self.config.write(open(self.filepath, "w"))

    def update_db_config(self, cluster_props):
        print("AWS MyCluster: Updating config file (DB HOST).")

        self.config["DB"]["HOST"] = cluster_props["Endpoint"]["Address"]
        self.config.write(open(self.filepath, "w"))

    def get_cluster_status(self):
        print("AWS MyCluster: Getting cluster status.")

        try:
            resp = self.redshift_client.describe_clusters(
                ClusterIdentifier="dwhcluster"
            )
            return resp["Clusters"][0]["ClusterStatus"], resp["Clusters"][0]
        except self.redshift_client.exceptions.ClusterNotFoundFault as error:
            return "NotFound", None

    def redshift_cluster_wait(self):
        print("AWS MyCluster: Checking if cluster is in transition state.")

        cluster_status, cluster_props = self.get_cluster_status()
        print("AWS MyCluster: Status returned '%s'." % cluster_status)

        while cluster_status in ["creating", "deleting"]:
            sleep(30)
            cluster_status, cluster_props = self.get_cluster_status()
            print("AWS MyCluster: Status returned '%s'." % cluster_status)

        return cluster_status, cluster_props

    def redshift_cluster_delete(self):
        print("AWS MyCluster: Deliting cluster.")

        self.redshift_client.delete_cluster(
            ClusterIdentifier="dwhcluster", SkipFinalClusterSnapshot=True
        )

    def up(self):
        self.sparkifydwh_role_create()
        self.update_role_config()
        self.redshift_cluster_create()

        cluster_status, cluster_props = self.redshift_cluster_wait()

        if cluster_status == "available":
            self.authorize_ingress(cluster_props)
            self.update_db_config(cluster_props)

    def down(self):
        self.redshift_cluster_delete()
        self.redshift_cluster_wait()
        self.sparkifydwh_role_delete()
