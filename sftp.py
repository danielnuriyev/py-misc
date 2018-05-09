
# https://www.adampalmer.me/iodigitalsec/2014/11/23/ssh-sftp-paramiko-python/

from paramiko import *
from paramiko.client import SSHClient

k = RSAKey.from_private_key_file('dpd.pem')

client = SSHClient()
client.set_missing_host_key_policy(AutoAddPolicy())
client.load_system_host_keys()
client.connect(hostname='sftp.dpdgroup.co.uk', username='sftp.305995', pkey=k)

sftp = client.open_sftp()
sftp.chdir("/OUT/")
print sftp.listdir()

client.close()