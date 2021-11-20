import configparser
import os
import boto3
import json
import uuid
import logging
from time import sleep


class MyCluster:
    """
    Description: This class is responsible for encapsulating an
    AWS connection session for executing AWS operations.
    """

    _song_json_path = "song_json_path.json"
    _region_name = "us-west-2"
    _sparkifydwh_role_name = "sparkifydwh_role"
    _s3_read_only_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"

    def __init__(self, filepath):
        """
        Description: This function is responsible for creating
        the AWS session and all necessary attributes.

        Arguments:
            filepath (str, required): Configuration file absolute path.

        Returns:
            None
        """
        # set logging
        logging.root.setLevel(logging.INFO)
        logging.info("AWS MyCluster: Creating session.")

        # loading configuration file
        self.filepath = filepath
        config = configparser.ConfigParser()
        config.read_file(open(filepath))

        self.config = config

        # creating session
        session = boto3.session.Session(
            aws_access_key_id=config["AWS"]["KEY"],
            aws_secret_access_key=config["AWS"]["SECRET"],
            region_name=self._region_name,
        )

        # setting attributes
        self.redshift_client = session.client("redshift")
        self.iam_client = session.client("iam")
        self.ec2_client = session.client("ec2")
        self.s3_client = session.client("s3")

    def authorize_ingress(self, vpc_id):
        """
        Description: This function is responsible for authorizing
        external access to the redshift cluster.

        Arguments:
            vpc_id (str, required): Virtual private cloud identity of the
            redshift cluster that needs authorization.

        Returns:
            None
        """
        logging.info("AWS MyCluster: Authorizing ingress.")

        config, ec2_client = self.config, self.ec2_client

        # selecting the security group
        vpc_filter = [{"Name": "vpc-id", "Values": [vpc_id]}]
        resp = ec2_client.describe_security_groups(Filters=vpc_filter)
        default_sg = resp["SecurityGroups"][0]

        # authorizing ingress
        PORT = int(config["DB"]["PORT"])
        try:
            ec2_client.authorize_security_group_ingress(
                CidrIp="0.0.0.0/0",
                IpProtocol="TCP",
                FromPort=PORT,
                ToPort=PORT,
                GroupId=default_sg["GroupId"],
            )
        except Exception as error:
            logging.warning(error)

    def redshift_cluster_create(self):
        """
        Description: This function is responsible for creating
        the redshift cluster.

        Arguments:
            None

        Returns:
            dict: Dictionary of created cluster descriptions returned by boto3 create_cluster function.
            (More: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift.html#Redshift.Client.create_cluster)

        """
        logging.info("AWS MyCluster: Creating cluster.")

        config, redshift_client = self.config, self.redshift_client

        # setting new cluster params
        params = {
            "NodeType": config["DWH"]["NODE_TYPE"],
            "ClusterIdentifier": config["DWH"]["CLUSTER_IDENTIFIER"],
            "DBName": config["DB"]["DBNAME"],
            "Port": int(config["DB"]["PORT"]),
            "MasterUsername": config["DB"]["USER"],
            "MasterUserPassword": config["DB"]["PASSWORD"],
            "IamRoles": [config["IAM_ROLE"]["ARN"]],
        }

        NUM_NODES = int(config["DWH"]["NUM_NODES"])
        if NUM_NODES > 1:
            params["NumberOfNodes"] = NUM_NODES
            params["ClusterType"] = "multi-node"
        else:
            params["ClusterType"] = "single-node"

        # creating cluster
        try:
            resp = redshift_client.create_cluster(**params)
        except Exception as error:
            logging.warning(error)
        return resp

    def sparkifydwh_role_create(self):
        """
        Description: This function is responsible for creating
        the redshift cluster role allowing s3 read access.

        Arguments:
            None

        Returns:
            (str, str): The Arn and name of the role
        """
        logging.info("AWS MyCluster: Creating role.")

        iam_client = self.iam_client

        # setting
        role_name = self._sparkifydwh_role_name
        sparkifydwh_role = None

        try:
            # looging for the role
            sparkifydwh_role = iam_client.get_role(RoleName=role_name)

        except iam_client.exceptions.NoSuchEntityException as error:
            # as role not found, creating it
            sparkifydwh_role = iam_client.create_role(
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

        # attaching s3 read only access to the role
        iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn=self._s3_read_only_arn,
        )

        return sparkifydwh_role["Role"]["Arn"], role_name

    def sparkifydwh_role_delete(self):
        """
        Description: This function is responsible for detaching role
        policy and deleting the sparkify data warehouse role.

        Arguments:
            None

        Returns:
            None
        """
        logging.info("AWS MyCluster: Deleting session.")

        role_name, policy_arn, iam_client = (
            self._sparkifydwh_role_name,
            self._s3_read_only_arn,
            self.iam_client,
        )

        # detaching role
        iam_client.detach_role_policy(RoleName=role_name, PolicyArn=policy_arn)

        # deleting role
        iam_client.delete_role(RoleName=role_name)

    def update_role_config(self):
        """
        Description: This function is responsible for updating
        the data warehouse role Arn information in the configuration file.

        Arguments:
            None

        Returns:
            None
        """
        logging.info("AWS MyCluster: Updating config file (IAM_ROLE ARN).")

        iam_client = self.iam_client

        try:
            # looking for the role
            resp = iam_client.get_role(RoleName=self._sparkifydwh_role_name)
            # updating information
            self.config["IAM_ROLE"]["ARN"] = resp["Role"]["Arn"]

        except iam_client.exceptions.NoSuchEntityException as error:
            # as role not found, cleaning information
            self.config["IAM_ROLE"]["ARN"] = ""

        # saving new configurations to the file
        self.config.write(open(self.filepath, "w"))

    def update_db_config(self, host):
        """
        Description: This function is responsible for updating
        the cluster address information in the configuration file.

        Arguments:
            host (str, required): Endpoint address.

        Returns:
            None
        """
        logging.info("AWS MyCluster: Updating config file (DB HOST).")

        self.config["DB"]["HOST"] = host
        self.config.write(open(self.filepath, "w"))

    def update_song_jsonpath_config(self, bucket):
        """
        Description: This function is responsible for updating
        the song jsonpath information in the configuration file.

        Arguments:
            bucket (str, required): Bucket name.

        Returns:
            None
        """
        logging.info("AWS MyCluster: Updating config file (S3 SONG_JSONPATH).")

        self.config["S3"]["SONG_JSONPATH"] = "s3://%s/%s" % (
            bucket,
            self._song_json_path,
        )

        self.config.write(open(self.filepath, "w"))

    def get_cluster_status(self):
        """
        Description: This function is responsible for checking
        the redshift cluster status.

        Arguments:
            None

        Returns:
            (str, dict): Return the cluster status and the
            cluster properties dictonary.
        """
        logging.info("AWS MyCluster: Getting cluster status.")

        try:
            # looking for the cluster on aws
            resp = self.redshift_client.describe_clusters(
                ClusterIdentifier="dwhcluster"
            )

            return resp["Clusters"][0]["ClusterStatus"], resp["Clusters"][0]

        except self.redshift_client.exceptions.ClusterNotFoundFault as error:
            return "NotFound", None

    def redshift_cluster_wait(self):
        """
        Description: This function is responsible for waiting for a redshift
        cluster status change while the status is in transition.

        Arguments:
            None

        Returns:
            (str, dict): Return the cluster status and the
            cluster properties dictonary.
        """
        logging.info("AWS MyCluster: Checking if cluster is in transition state.")

        # getting cluster status
        cluster_status, cluster_props = self.get_cluster_status()
        logging.info("AWS MyCluster: Status returned '%s'." % cluster_status)

        # check for transition states
        while cluster_status in ["creating", "deleting"]:
            sleep(30)
            cluster_status, cluster_props = self.get_cluster_status()
            logging.info("AWS MyCluster: Status returned '%s'." % cluster_status)

        return cluster_status, cluster_props

    def redshift_cluster_delete(self):
        """
        Description: This function is responsible for waiting for deleting
        the redshift cluster.

        Arguments:
            None

        Returns:
            None
        """
        logging.info("AWS MyCluster: Deliting cluster.")

        try:
            self.redshift_client.delete_cluster(
                ClusterIdentifier="dwhcluster", SkipFinalClusterSnapshot=True
            )
        except Exception as error:
            logging.warning(error)

    def bucket_jsonpaths_get_or_create(self):
        """
        Description: This function is responsible for getting or creating
        an s3 bucket to store json paths files. It looks for a bucket name
        that matches the pattern jsonpaths-*.

        Arguments:
            None

        Returns:
            str: Name of the bucket.
        """
        logging.info("AWS MyCluster: Getting or creating jsonpath bucket.")

        s3_client = self.s3_client
        bucket = None

        try:
            # asking s3 for a list of buckets
            resp = s3_client.list_buckets()

            # looking for a bucket name that matches jsonspaths-*
            bucket = next(
                bucket["Name"]
                for bucket in resp["Buckets"]
                if bucket["Name"].startswith("jsonpaths-")
            )

        except StopIteration as error:
            # as bucket not found, creating it.
            # setting the name pattern
            hash_id = str(uuid.uuid4())
            new_bucket = "jsonpaths-%s" % hash_id[:13]

            # creating
            s3_client.create_bucket(
                Bucket=new_bucket,
                ACL="public-read",
                CreateBucketConfiguration={"LocationConstraint": self._region_name},
            )
            bucket = new_bucket

        return bucket

    def bucket_delete(self, bucket):
        """
        Description: This function is responsible for deleting
        a bucket.

        Arguments:
            bucket (str): Bucket name.

        Returns:
            str: Name of the bucket.
        """
        logging.info("AWS MyCluster: Deleting %s bucket." % bucket)

        s3_client = self.s3_client

        # listing bucket files and deleting before delete the bucket
        resp = s3_client.list_objects_v2(Bucket=bucket)
        if "Contents" in resp:
            objects = list(map(lambda object: {"Key": object["Key"]}, resp["Contents"]))
            s3_client.delete_objects(Bucket=bucket, Delete={"Objects": objects})

        # delete bucket
        resp = s3_client.delete_bucket(Bucket=bucket)

    def songs_jsonpaths_upload(self, bucket):
        """
        Description: This function is responsible for upload the
        songs json paths file to the s3 bucket.

        Arguments:
            bucket (str): Bucket name.

        Returns:
            str: Name of the bucket.
        """
        s3_client = self.s3_client

        try:
            # checking if the file already exists
            s3_client.get_object(Bucket=bucket, Key=self._song_json_path)

        except s3_client.exceptions.NoSuchKey as error:
            # as key was not found, creating it
            songs_jsonpaths = {
                "jsonpaths": [
                    "$['artist_id']",
                    "$['artist_latitude']",
                    "$['artist_location']",
                    "$['artist_longitude']",
                    "$['artist_name']",
                    "$['duration']",
                    "$['num_songs']",
                    "$['song_id']",
                    "$['title']",
                    "$['year']",
                ]
            }

            s3_client.put_object(
                Body=json.dumps(songs_jsonpaths).encode("UTF-8"),
                Bucket=bucket,
                Key=self._song_json_path,
            )
