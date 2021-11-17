
import configparser, os, boto3

# setting configuration filepath
filepath = os.path.join('..', 'dwh.cfg')

# loading configuration file
config = configparser.ConfigParser()
config.read_file(open(filepath))


def session_get():
    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')

    session = boto3.session.Session(
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
        region_name='us-west-2'
    )

    return session
