import sys
import os
from cluster import MyCluster


def aws_function(arg, *params):
    """
    Description: aws function is responsible for setting the cluster up or down.

    Arguments:
        up: For setting the cluster up.
        down: For setting the cluster down.
        bucket_delete: For deleting a bucket.

    Usage:
        python aws.py up
        python aws.py down
        python aws.py bucket_delete [bucket name]

    """
    filepath = os.path.join(os.path.abspath(os.getcwd()), "dwh.cfg")
    cl = MyCluster(filepath)

    if arg == "up":
        cl.sparkifydwh_role_create()
        cl.update_role_config()
        cl.redshift_cluster_create()

        bucket_jsonpaths = cl.bucket_jsonpaths_get_or_create()
        cl.songs_jsonpaths_upload(bucket_jsonpaths)
        cl.update_song_jsonpath_config(bucket_jsonpaths)

        cluster_status, cluster_props = cl.redshift_cluster_wait()

        if cluster_status == "available":
            cl.authorize_ingress(cluster_props["VpcId"])
            cl.update_db_config(cluster_props["Endpoint"]["Address"])

    elif arg == "down":
        cl.redshift_cluster_delete()
        cl.redshift_cluster_wait()
        cl.sparkifydwh_role_delete()

    elif arg == "bucket_delete":
        if len(params):
            cl.bucket_delete(params[0])
        else:
            print(aws_function.__doc__)


if __name__ == "__main__":
    if len(sys.argv) == 1:
        print(aws_function.__doc__)
        exit(0)

    if sys.argv[1] not in ["up", "down", "bucket_delete"]:
        print(aws_function.__doc__)

    else:
        aws_function(*sys.argv[1:])
