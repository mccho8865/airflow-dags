import ftplib
import io

import boto
import boto.s3.connection
from boto.s3.key import Key

access_key = 'Z780FG2AP64YD0Y2EWS8'
secret_key = 'akGdNm3vY9xSCcyscq8StdTh6BMRGtt9FChidPgn'

conn = boto.connect_s3(
        aws_access_key_id = access_key,
        aws_secret_access_key = secret_key,
        host = 'rook-ceph-rgw-my-store.rook-ceph.svc',
        is_secure=False,               # uncomment if you are not using ssl 
        calling_format = boto.s3.connection.OrdinaryCallingFormat(),
        )
coin_bucket = conn.get_bucket("coin-bucket")

def execute(dt, hh):
    read_dir = f"/raw/ticker/dt={dt}/hh={hh}"
    write_dir = f"/raw/ticker_merged/dt={dt}"

    item_list = []
    with ftplib.FTP() as ftp:
        ftp.connect("192.168.0.10", 21)
        ftp.login()
        ftp.cwd(read_dir)
        file_list = ftp.nlst()

        for file in file_list:
            item = []
            ftp.retrlines(f"RETR {file}", item.append)
            item_list.append("".join(item).replace(" ", ""))
        print(f"item count : {len(item_list)}")


    k = Key(coin_bucket)
    k.key = f"warehouse/raw/ticker/dt={dt}/hh_{hh}.txt"
    k.set_contents_from_string("\n".join(item_list))