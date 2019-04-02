#!/usr/bin/python
import os
import sys
import subprocess
from datetime import datetime
import base64
import boto
from boto.s3.key import Key

KEY = '9213imsskl092i34@@#(*@&*dc9sdcnaasxk)'

DB_USER = 'ballogy'
DB_NAME = 'ballogydb'

BACKUP_PATH = '/db_backup/%s/%s'

FILENAME_PREFIX = 'ballogy_prod_backup_%s.backup' % str(datetime.now())


def encode(key, clear):
    enc = []
    for i in range(len(clear)):
        key_c = key[i % len(key)]
        enc_c = chr((ord(clear[i]) + ord(key_c)) % 256)
        enc.append(enc_c)
    return base64.urlsafe_b64encode("".join(enc))

def decode(key, enc):
    dec = []
    enc = base64.urlsafe_b64decode(enc)
    for i in range(len(enc)):
        key_c = key[i % len(key)]
        dec_c = chr((256 + ord(enc[i]) - ord(key_c)) % 256)
        dec.append(dec_c)
    return "".join(dec)

#Amazon S3 settings.
AWS_ACCESS_KEY_ID = decode(KEY, 'en16dLOxysGfuYNsh7BqZ5GabGk=')
AWS_SECRET_ACCESS_KEY = decode(KEY, 'e5NiitLSo9zfs31sdJt4i5hxaoyAtYx2mJeLy53GsLqk6MebcIKClA==')
AWS_BUCKET_NAME = decode(KEY, 'm5Odn9jU7NXgz5uepg==')


def log_info(string_to_log):
    with open('/home/ubuntu/db_backup/info.log', 'a') as f:
        timestamp = str(datetime.now())
        f.write('%s >> %s\n' % (timestamp, string_to_log))


def main():

    now = datetime.now()

    filename = FILENAME_PREFIX
    filename = '%s_%s' % ('daily', FILENAME_PREFIX)


    destination = r'%s' % (filename)

    log_info('Backing up %s database to %s' % (DB_NAME, destination))
    ps = subprocess.Popen(
        ['pg_dump', '-h', 'localhost', '-p', '2234', '-U', 'ballogy', '-F', 'c', '-b', '-v', '-f', destination, 'ballogydb'],
        stdout=subprocess.PIPE
    )
    output = ps.communicate()[0]
    for line in output.splitlines():
        print line

    log_info('Uploading %s to Amazon S3...' % filename)
    upload_to_s3(destination, BACKUP_PATH % (str(now.date()), filename))
    os.remove(destination)

def upload_to_s3(source_path, destination_filename):
    """
    Upload a file to an AWS S3 bucket.
    """
    print source_path

    print destination_filename
    conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    bucket = conn.get_bucket(AWS_BUCKET_NAME)
    k = Key(bucket)
    k.key = destination_filename
    k.set_contents_from_filename(source_path)


if __name__ == '__main__':
    main()
