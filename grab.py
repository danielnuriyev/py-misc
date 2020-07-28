from Crypto.Cipher import AES
import base64
import boto3
import datetime
import functools
import json
import os
import subprocess
import requests
import shutil
import socket
import sys
import tempfile
import threading
import time
import traceback

import cherrypy
import multiprocessing
import pylru

class BadResponseError(Exception):
    pass

class DataDoesNotExist(Exception):
    pass

class Throttled(Exception):
    pass

class FreddyFatFingerException(Exception):
    pass

# instead of django settings
class Settings:
    ISDH_CERT_FILE = "./id.decrypted/issink.crt"
    ISDH_KEY_FILE = "./id.decrypted/issink.key"
    ISDH_API_HOST = "haus.chi.insightsquared.com"
    ISDH_API_TEMPLATE = "https://%s/api/1.0"
    DEBUG = True
    IS2_KEY = '45\xb0C\xfa\x05\x96\xef03\x97w]\x99\x9d\x91\x8eg\xe4\xdf\xcdWkZ\xd0\xed\xa7\x02\x89:\xc7\xf2'
    IS2_DOMAIN = "chi.insightsquared.com"

settings = Settings()

def decrypt_token(encrypted_token, use_key=None):
    if use_key is None:
        use_key = settings.IS2_KEY

    encrypted_bytes = base64.urlsafe_b64decode(encrypted_token)
    block_size = 16
    iv_bytes = encrypted_bytes[:block_size]
    encrypted_bytes = encrypted_bytes[block_size:]
    plain_text = AES.new(use_key, AES.MODE_CBC, iv_bytes).decrypt(encrypted_bytes)
    pad = ord(plain_text[-1])
    plain_text = plain_text[:-pad]
    if not plain_text:
        if use_key == settings.OLD_IS2_KEY:
            raise FreddyFatFingerException("IS2_KEY is incorrect")
        else:
            return decrypt_token(encrypted_token, use_key=settings.OLD_IS2_KEY)
    return plain_text

def decrypt_keys(source, destination):
    if not os.path.exists(destination):
        os.makedirs(destination)

    for f in os.listdir(source):
        in_fn = os.path.join(source, f)
        out_fn = os.path.join(destination, f)
        with open(in_fn) as in_f:
            with open(out_fn, "wb") as out_f:
                out_f.write(decrypt_token(in_f.read()))

def retry(partial, retries=None, client_abbr=None):
    wait = 240
    while True:
        retries -= 1
        try:
            response = partial()
            if hasattr(response, 'status_code'):
                if response.status_code not in (requests.codes.ok, requests.codes.gone):
                    print("ERROR: " + str(response.status_code))
                if response.status_code == requests.codes.service_unavailable:
                    raise Throttled('Throttled: %s' % response.text)
                # Continuing other retries for now?
                elif response.status_code not in (requests.codes.ok, requests.codes.gone):
                    raise BadResponseError()
            return response
        except (Throttled, BadResponseError):
            if retries == 0:
                raise
        except DataDoesNotExist:
            raise
        except Exception:
            print(sys.exc_info()[1])

            if retries:
                print("Exception")
            else:
                raise

        time.sleep(wait)
        wait = min(wait + wait, 1200)

class S3Uploader(threading.Thread):

    def __init__(self, dir, file, s3, bucket, path, client, tag, timestamp):

        self.dir = dir
        self.file = file
        self.s3 = s3
        self.bucket = bucket
        self.path = path
        self.client = client
        self.tag = tag
        self.timestamp = timestamp

        threading.Thread.__init__(self)

    def run(self):

        path = os.path.join(
            self.path,
            "data",
            "{}-{}-{}.sqlite".format(self.client, self.tag, self.timestamp)
        )
        print("Uploading to " + path)
        object = self.s3.Object(
            self.bucket,
            path
        )
        object.upload_file(
            os.path.join(
                self.dir,
                self.file
            )
        )

class GrabThread(threading.Thread):

    def __init__(self, config):

        self.client = config["client"]
        self.bucket = config["s3Bucket"]
        self.path = config["s3Path"]
        self.tag = config["tag"]

        self.status = "running" #TODO: enum

        self.created_time = datetime.datetime.now()
        self.timestamp = self.created_time.strftime("%Y-%m-%d-%H-%M-%S")  # TODO: const
        self.id = "{}-{}-{}".format(self.client, self.tag.replace("#", "-"), self.timestamp)

        self.started_time = None
        self.queued_time = None
        self.finished_time = None
        self.run_time = None

        threading.Thread.__init__(self)

    def run(self):

        self.started_time = datetime.datetime.now()
        self.queued_time = self.started_time - self.created_time

        try:
            grab(self.client, self.bucket, self.path, self.tag, self.timestamp)
            self.status = "finished"
        except:
            self.status = "failed"
            traceback.print_exc()

        self.finished_time = datetime.datetime.now()
        self.run_time = self.finished_time - self.started_time

class Grab(object):

    def __init__(self, client_abbr, retries, verbose, tm_id=None):
        self.tm_id = tm_id
        self.retries = 4 if retries else 1
        socket.setdefaulttimeout(180)

        self.ssl_args = dict(verify=False,  # settings.ISDH_CA_CERT_FILE,
                             cert=(settings.ISDH_CERT_FILE, settings.ISDH_KEY_FILE),)

        self._host_name = socket.gethostname()
        self._haus_host = settings.ISDH_API_HOST
        self.client_abbr = client_abbr
        self.verbose = verbose
        self.request_headers = {'Accept': 'application/json'}

        if settings.DEBUG:  # TODO make this dependant on the filesize being transferred
            self.request_headers['Accept-Encoding'] = 'gzip'
        else:
            self.request_headers['Accept-Encoding'] = 'identity'

        # added to the original grab
        self.s3_bucket = None
        self.s3_path = None

    def retry(self, partial):
        return retry(partial, retries=self.retries, client_abbr=self.client_abbr)

    def get_uri(self, path):
        return settings.ISDH_API_TEMPLATE % self._haus_host + path

    def get(self, dest_dir, ident=None, tag=None, copy_local=False, unpack=True):
        client = self.client_abbr
        print("grab.get:tag: " + str(tag))
        return self._get(dest_dir, client, ident=ident, tag=tag, copy_local=copy_local, unpack=unpack)

    def _get(self, dest_dir, client, ident=None, tag=None, copy_local=False, unpack=True):
        result = self.retry(functools.partial(self.do_read, client, dest_dir, ident, tag, copy_local, unpack))
        return result

    def get_info(self, ident=None, tag=None):
        args = dict(client=self.client_abbr, host_name=self._host_name)

        if tag:
            args['tag'] = tag

        if ident:
            args['ident'] = ident

            response = self.retry(functools.partial(requests.get,
                                                    self.get_uri('/session/'),
                                                    params=args,
                                                    headers=self.request_headers,
                                                    **self.ssl_args))

            if response.status_code != requests.codes.ok:
                raise Exception("Unable to read session: %s" % response.text)

            return response.json()
        else:
            response = self.retry(functools.partial(requests.get,
                                                    self.get_uri('/info/'),
                                                    params=args,
                                                    headers=self.request_headers,
                                                    **self.ssl_args))

            if response.status_code != requests.codes.ok:
                raise Exception("Unable to read session: %s" % response.text)

            return response.json()['transactions']

    def put(self, tag, cur_dir, old_dir=None, info=None):
        base_info = {"tm_id": self.tm_id}
        if info:
            base_info.update(info)
        tar_dir, tar_file = self.retry_pack_data(cur_dir)
        ident = None
        try:
            result = self.retry_do_write(self.client_abbr,
                                         tag,
                                         tar_file,
                                         old_dir=old_dir,
                                         info=base_info)
            if result is not None:
                ident = result["ident"]
        finally:
            shutil.rmtree(tar_dir, True)

        return ident

    def do_read(self, abbr, dest_dir, ident=None, tag=None, copy_local=False, unpack=True):
        if not ident:
            return False

        args = dict(ident=ident,
                    client=abbr,
                    host_name=self._host_name)
        if tag:
            args['tag'] = tag

        if self.verbose:
            print("  * getting haus access token")

        print(self.get_uri('/session/'))

        response = self.retry(functools.partial(requests.get,
                                                self.get_uri('/session/'),
                                                params=args,
                                                headers=self.request_headers,
                                                **self.ssl_args))

        if response.status_code == requests.codes.gone:
            raise DataDoesNotExist('Gone tag: %s | ident: %s' % (tag, ident))
        elif response.status_code != requests.codes.ok:
            raise Exception("Unable to read session: %s" % response.text)

        try:
            body = response.json()
            token = body['access_token']
            ident = body['ident']
            files = body['files']
            total_size = body['total_size']
            total_size_mb = round(total_size / 1024.0 / 1024, 1)

            self._haus_host = "%s.%s" % (body['shard_name'], settings.IS2_DOMAIN)

            if self.verbose:
                print("  * haus ident is %s" % ident)
                print("  * Getting data from %s" % self._haus_host)

            shutil.rmtree(dest_dir, True)
            os.makedirs(dest_dir) #os.makedirs(dest_dir, 0755)

            start = cur_time = time.time()
            if self.verbose:
                sys.stdout.write("  * getting haus files")
            multi_file = len(files) > 1

            if settings.DEBUG and self.verbose:
                sys.stdout.write(" (connecting...)")

            fetched = 0
            actual = None

            for f in files:
                response = self.retry(functools.partial(requests.get,
                                                        self.get_uri('/entity/'),
                                                        params={"access_token": token, 'filename': f},
                                                        headers=self.request_headers,
                                                        stream=True,
                                                        **self.ssl_args))
                compressed = True if response.headers.get('content-encoding') == 'gzip' else False

                if response.status_code != requests.codes.ok:
                    raise Exception("Unable to read session: %s" % response.text)

                block = None
                chunk_size = 9000
                with open('%s/%s' % (dest_dir, f), 'wb') as d:
                    for block in response.iter_content(chunk_size):
                        cur_time = time.time()
                        # do this here so we don't throw it for the last chunk, which is allowed to be smaller
                        if not multi_file and not compressed and actual is not None and actual < chunk_size:
                            if self.verbose:
                                print("\r  * chunk starting at %s of %s was less than expected: %s of %s" % (fetched,
                                                                                                             total_size,
                                                                                                             actual,
                                                                                                             chunk_size))

                        actual = len(block)
                        fetched += actual

                        d.write(block)

                        if settings.DEBUG and self.verbose:
                            rate = round(fetched / 1024 / 1024 / (cur_time - start), 1)
                            sys.stdout.write("\r  * getting haus files (%s%% of %sMB %scompressed) %s MB/s   " %
                                             (int((float(fetched) / total_size) * 100), total_size_mb, 'un' if not compressed else '', rate))

                if not multi_file:
                    if self.verbose:
                        print("\n    Fetched %sMB in %s seconds" % (total_size_mb, round(cur_time - start, 1)))
                if not multi_file and fetched != total_size:
                    print("  WARNING only received %s out of %s bytes. last chunk size was %s." % (fetched,
                                                                                                   total_size,
                                                                                                   actual))
                    print("  response code was %s. last block was:" % (response.status_code))
                    if block is not None:
                        print(block + "\n")  # let's see what we got

            if unpack and 'is_stream.tar' in files:
                tarfile = "%s/%s" % (dest_dir, 'is_stream.tar')
                received_size = os.path.getsize(tarfile)

                if received_size != total_size:
                    os.remove(tarfile)
                    print("  File on disk was %s. It's supposed to be %s" % (received_size, total_size))
                    raise Exception("** uh oh, I didn't receive the whole file")

                print(dest_dir)
                print(tarfile)
                cmd = ['tar', 'PxCf', dest_dir, tarfile]
                p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                stdout, _ = p.communicate()
                if p.returncode != 0:
                    raise Exception(stdout)

                """

                # Use is2_datahaus app to create necessary Transaction rows and files in
                # /usr/local/insightsquared/isdh_cache
                if copy_local and settings.DEBUG:
    
                    from bbase import dev_settings
                    self._haus_host = dev_settings.ISDH_API_HOST
                    settings.IS2_DOMAIN = "insightsquared.com"
                    self.put(tag, dest_dir)
                    self._haus_host = settings.ISDH_API_HOST
                    settings.IS2_DOMAIN = "chi.insightsquared.com"
                    
                """

                os.remove(tarfile)

                # added to the original grab
                if self.s3_bucket is not None:
                    self._upload_to_s3(dest_dir, abbr, tag)

            return body
        except Exception:
            print("Failed while writing local data")
            traceback.print_exc()
            raise
        finally:
            self._haus_host = settings.ISDH_API_HOST

        return False

    def _upload_to_s3(self, dir, client, tag):

        s3 = boto3.resource('s3')
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

        timestamped_upload = S3Uploader(dir, "db.sqlite", s3, self.s3_bucket, self.s3_path, client, tag, timestamp)
        latest_upload = S3Uploader(dir, "db.sqlite", s3, self.s3_bucket, self.s3_path, client, tag, "latest")

        timestamped_upload.start()
        latest_upload.start()

        timestamped_upload.join()
        latest_upload.join()


    def retry_pack_data(self, cur_dir):
        return self.retry(functools.partial(self.pack_data,
                                            cur_dir))

    def pack_data(self, cur_dir, include_dirs=False):
        tar_dir = tempfile.mkdtemp()
        try:
            files = []
            abs_path = os.path.abspath(cur_dir)
            for filename in os.listdir(cur_dir):
                abs_filepath = os.path.join(abs_path, filename)
                if filename == "_grab.state" or (not include_dirs and os.path.isdir(abs_filepath)):
                    continue
                files.append(filename)

            tar_file = os.path.join(tar_dir, 'is_stream.tar')

            if not files:
                raise Exception("Cannot tar and push empty directory %s" % abs_path)

            os.environ['GZIP'] = '-1'

            cmd = ['/bin/tar', 'POcCzf', abs_path, tar_file] + files
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            stdout, _ = p.communicate()
            if p.returncode != 0:
                raise Exception(stdout)
            return tar_dir, tar_file
        except Exception as e:
            print("\n\nFAILED HAUS TX:\n%s\n" % e)
            traceback.print_exc()
            shutil.rmtree(tar_dir, True)
            raise


    def pending_put(self, tag, tar_file, old_dir=None, info=None):
        base_info = {"tm_id": self.tm_id}
        if info:
            base_info.update(info)
        return self.retry_do_write(self.client_abbr, tag, tar_file, old_dir=old_dir, info=base_info, finish_action="pending_completion")


    def retry_do_write(self, abbr, tag, tar_file, old_dir=None, info=None, finish_action="finish"):
        return self.retry(functools.partial(self.do_write,
                                            tag,
                                            tar_file,
                                            old_dir,
                                            finish_action,
                                            info=info))

    def do_write(self, tag, tar_file, old_dir, finish_action, info=None):
        abbr = self.client_abbr
        args = dict(client=abbr,
                    tag=tag,
                    info=json.dumps(info),
                    host_name=self._host_name)

        # Try to start a diff session first. If the remote doesn't have this diff, do a full.
        response = None
        old_dir = None
        if self.verbose:
            print(args)
            print("doing full")

        if not response:
            response = self.retry(functools.partial(requests.post,
                                                    self.get_uri('/session/'),
                                                    params=args,
                                                    headers=self.request_headers,
                                                    **self.ssl_args))
            if response.status_code != requests.codes.ok:
                raise Exception("Unable to start session: %s" % response.text)

        body = response.json()
        ident = body['ident']
        token = body['access_token']
        semaphore_name = body.get("semaphore_name", None)
        semaphore_value = body.get("semaphore_value", None)

        if not ident:
            return None

        try:
            if self.verbose:
                print("new haus ident: %s" % ident)

            self._haus_host = "%s.%s" % (body['shard_name'], settings.IS2_DOMAIN)

            with open(tar_file) as pipe:
                params = dict(access_token=token,
                              filename='is_stream.tar',
                              mode='full')
                response = self.retry(functools.partial(requests.post,
                                                        self.get_uri('/entity/'),
                                                        params=params,
                                                        data=pipe,
                                                        headers={'Accept': 'application/json'},
                                                        **self.ssl_args))
            if self.verbose:
                print("finished file transfer")

            params = dict(action=finish_action,
                          semaphore_name=semaphore_name,
                          semaphore_value=semaphore_value,
                          access_token=token,
                          host_name=self._host_name)
            response = self.retry(functools.partial(requests.put,
                                                    self.get_uri('/session/'),
                                                    params=params,
                                                    headers=self.request_headers,
                                                    **self.ssl_args))
            if self.verbose:
                print("closed session")

        except Exception as e:
            print("\n\nFAILED HAUS TX:\n%s\n" % e)
            traceback.print_exc()
            raise

        finally:
            self._haus_host = settings.ISDH_API_HOST

        return body

    def group_complete(self, tokens):
        access_tokens = json.dumps(tokens)
        params = dict(action="group_completion",
                      access_tokens=access_tokens,
                      host_name=self._host_name)
        result = None

        if self.verbose:
            print("marked all transactions as complete")
        try:
            result = self.retry(functools.partial(requests.put,
                                                  self.get_uri('/session/'),
                                                  params=params,
                                                  headers=self.request_headers,
                                                  **self.ssl_args))

        except Exception as e:
            print("\n\nFAILED HAUS TX:\n%s\n" % e)
            traceback.print_exc()
            raise

        finally:
            return result

decrypt_keys("id.encrypted", "id.decrypted")

def grab(client, bucket, path, tag, timestamp):

    ident = "latest"

    g = Grab(client, True, True)
    g.s3_bucket = bucket
    g.s3_path = path

    tmp_path = os.path.join("/tmp", "grab", client, tag, ident, timestamp)

    t = time.time()
    g.get(tmp_path, ident, tag)

    return time.time() - t

downloads = pylru.lrucache(100) #TODO: const, persistent

class Api(object):

    @cherrypy.tools.json_in()
    @cherrypy.expose
    def post(self):

        data = cherrypy.request.json

        g = GrabThread(data)
        g.start()

        downloads[g.id] = g

        cherrypy.response.status = 202
        cherrypy.response.headers["Operation-Location"] = "/ident/{}".format(g.id)

    @cherrypy.tools.json_out()
    @cherrypy.expose
    def get(self, id):

        d = downloads[id]

        if d is None:
            cherrypy.response.status = 404

        elif d.status == "failed":
            cherrypy.response.status = 500

        elif d.status == "finished":
            r = {
                "dataFile": "s3://{}/{}/data/{}-{}-{}.sqlite".format(d.bucket, d.path, d.client, d.tag, d.timestamp),
                "status": "finished",
                "runSeconds": d.run_time.total_seconds(),
                "created": d.created_time.strftime("%Y-%m-%d-%H-%M-%S"),
            }
            return r

        elif d.status == "running":
            r = {
                "status" : "running",
                "resourceLocation": "/ident/{}".format(d.id),
                "runSeconds": (datetime.datetime.now() - d.created_time).total_seconds(),
                "created": d.created_time.strftime("%Y-%m-%d-%H-%M-%S"),
            }
            return r

        else:
            cherrypy.response.status = 500

if __name__ == "__main__":

    api = Api()

    d = cherrypy.dispatch.RoutesDispatcher()
    d.connect(action='post', name='post', route='/ident', controller=api, conditions=dict(method=['POST']))
    d.connect(action='get', name='get', route='/ident/:id', controller=api, conditions=dict(method=['GET']))

    c = {'/': {'request.dispatch': d}}

    cherrypy.server.thread_pool = multiprocessing.cpu_count() * 2
    cherrypy.server.socket_host = '0.0.0.0'
    cherrypy.server.socket_port = 5000
    cherrypy.tree.mount(root=None, config=c)
    cherrypy.engine.start()
    cherrypy.engine.block()
