
from cassandra.cluster import Cluster
import cherrypy
import json
from kafka import KafkaProducer
import logging
import logging.config
import requests
import sys
from ws4py.server.cherrypyserver import WebSocketPlugin, WebSocketTool
from ws4py.websocket import EchoWebSocket

class Api(object):

    def __init__(self):

        self._logger = logging.getLogger()

        self._logger.info('Starting server')

        broker = 'devirlcmbcon001.cimba.cimpress.io:9092'
        self._kafka = KafkaProducer(bootstrap_servers=broker)

        self._logger.info('Server is running')

        cluster = Cluster()
        self._cassandra = cluster.connect()
        self._cassandra.set_keyspace('test')

    def __del__(self):
        if self._kafka is not None:
            self._kafka.flush()
            self._kafka.close()

        if self._cassandra is not None:
            self._cassandra.shutdown()

    @cherrypy.tools.gzip()
    def get_headers(self):
        result = ''
        for key, value in cherrypy.request.headers.items():
            result += key + ': ' + value + '\n'
        self._logger.info(result)
        return result

    @cherrypy.tools.gzip()
    @cherrypy.tools.json_in()
    def test_kafka_http(self):

        url = 'http://devirlcmbcon001.cimba.cimpress.io/topics/test-daniel-py'

        data = {"records": [ { "key" : "any", "value" : cherrypy.request.json} ] }


        r = requests.post(url, data=data, headers={'Content-Type':'application/vnd.kafka.json.v1+json'})

        return str(r.status_code)

    @cherrypy.tools.gzip()
    @cherrypy.tools.json_in()
    def test_kafka_direct(self):

        data = json.dumps(cherrypy.request.json)
        b = bytearray(data, 'utf-8')

        self._kafka.send('test-daniel-py-ts', b)

        return data

    @cherrypy.tools.gzip()
    @cherrypy.tools.json_out()
    def range(self, **kwargs):

        key = kwargs.get('key', '') # TODO raise x
        start = kwargs.get('skip', 0) # TODO limit if 0

        select = "SELECT v FROM test_ts WHERE k=%s AND t>%s"
        rows = self._cassandra.execute(select, [key, start])
        out = []
        for r in rows:
            out.append(r[0])

        return json.dumps(out)

    @cherrypy.expose
    def ws(self):
        # you can access the class instance through
        handler = cherrypy.request.ws_handler

if __name__ == '__main__':

    port = 8080
    if len(sys.argv) == 2:
        port = int(sys.argv[1])

    LOG_CONF = {

        'version': 1,

        'formatters': {
            'void': {
                'format': ''
            },
            'standard': {
                'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
            },
        },

        'handlers': {
            'default': {
                'level': 'INFO',
                'class': 'logging.StreamHandler',
                'formatter': 'void',
                'stream': 'ext://sys.stdout'
            },
            'console': {
                'level': 'INFO',
                'class': 'logging.StreamHandler',
                'formatter': 'void',
                'stream': 'ext://sys.stdout'
            },
            'file': {
                'level': 'INFO',
                'class': 'logging.handlers.RotatingFileHandler',
                'formatter': 'void',
                'filename': 'server.' + str(port) + '.log',
                'maxBytes': 100 * 1024 * 1024,
                'backupCount': 5,
                'encoding': 'utf8'
            },
            'access': {
                'level': 'INFO',
                'class': 'logging.handlers.RotatingFileHandler',
                'formatter': 'void',
                'filename': 'access.' + str(port) + '.log',
                'maxBytes': 100 * 1024 * 1024,
                'backupCount': 5,
                'encoding': 'utf8'
            },
            'error': {
                'level': 'ERROR',
                'class': 'logging.handlers.RotatingFileHandler',
                'formatter': 'void',
                'filename': 'errors.' + str(port) + '.log',
                'maxBytes': 100 * 1024 * 1024,
                'backupCount': 5,
                'encoding': 'utf8'
            },
        },
        'loggers': {
            '': {
                'handlers': ['default', 'file'],
                'level': 'INFO'
            },
            'cherrypy.access': {
                'handlers': ['console', 'access'],
                'level': 'INFO',
                'propagate': False
            },
            'cherrypy.error': {
                'handlers': ['console', 'error'],
                'level': 'ERROR',
                'propagate': False
            },
        }
    }

    cherrypy.engine.unsubscribe('graceful', cherrypy.log.reopen_files)
    logging.config.dictConfig(LOG_CONF)

    controller = Api()

    routes = cherrypy.dispatch.RoutesDispatcher()
    routes.mapper.explicit = True

    routes.connect('in',
                   '/in',
                   controller=controller,
                   action='test_kafka_direct',
                   conditions=dict(method=['OPTIONS', 'POST']))

    routes.connect('out',
                   '/out',
                   controller=controller,
                   action='range',
                   conditions=dict(method=['GET']))

    routes.connect('headers',
                   '/headers',
                   controller=controller,
                   action='get_headers',
                   conditions=dict(method=['GET']))

    conf = {
        '/':
            {
                'request.dispatch': routes,
                'response.headers.server': ''
            },
        '/ws': {'tools.websocket.on': True,
                'tools.websocket.handler_cls': EchoWebSocket # see https://gist.github.com/Lawouach/7698023
                }
    }

    # cherrypy.server.ssl_module = 'builtin'
    # cherrypy.server.ssl_certificate = "cert/cert.pem"
    # cherrypy.server.ssl_private_key = "cert/privkey.pem"

    cherrypy.config.update({'server.socket_port': port,
                            'server.socket_host': '0.0.0.0'
                            })

    WebSocketPlugin(cherrypy.engine).subscribe()
    cherrypy.tools.websocket = WebSocketTool()

    cherrypy.log("Starting on port " + str(port))
    cherrypy.quickstart(controller, '', config=conf)