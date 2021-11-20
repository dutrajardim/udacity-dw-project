import sys
import os
from dutrajardim import MyCluster

if __name__ == "__main__":
    arg = sys.argv[1]

    if arg not in ["up", "down"]:
        print("help")
        exit(0)

    filepath = os.path.join(os.path.abspath(os.getcwd()), "dwh.cfg")
    print(filepath)
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
            cl.authorize_ingress(cluster_props)
            cl.update_db_config(cluster_props)

        exit(0)

    elif arg == "down":
        cl.redshift_cluster_delete()
        cl.redshift_cluster_wait()
        cl.sparkifydwh_role_delete()
        exit(0)
