# -*- coding: utf-8 -*-

# FLEDGE_BEGIN
# See: http://fledge-iot.readthedocs.io/
# FLEDGE_END

""" 
"""

import zmq
import copy
import logging
import asyncio
import json
from threading import Thread

from fledge.common import logger
from fledge.plugins.common import utils
from fledge.services.south import exceptions
from fledge.services.south.ingest import Ingest
import async_ingest

__author__ = "Akli Rahmoun"
__copyright__ = "Copyright (c) 2020 RTE (https://www.rte-france.com)"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

_LOGGER = logger.setup(__name__, level=logging.INFO)

c_callback = None
c_ingest_ref = None
loop = None

_DEFAULT_CONFIG = {
    'plugin': {
        'description': 'ZMQ Subscriber South Plugin',
        'type': 'string',
        'default': 'zmq',
        'readonly': 'true'
    },
    'proxyHost': {
        'description': 'Hostname or IP address of the proxy to connect to',
        'type': 'string',
        'default': 'localhost',
        'order': '1',
        'displayName': 'ZMQ proxy host',
        'mandatory': 'true'
    },
    'proxyPort': {
        'description': 'The network port of the proxy to connect to',
        'type': 'integer',
        'default': '5559',
        'order': '2',
        'displayName': 'ZMQ proxy port',
        'mandatory': 'true'
    },
    'topic': {
        'description': 'The subscription topic to subscribe to receive messages',
        'type': 'string',
        'default': 'DEFAULT',
        'order': '3',
        'displayName': 'Topic To Subscribe',
        'mandatory': 'true'
    },
    'assetName': {
        'description': 'Name of Asset',
        'type': 'string',
        'default': 'zmq-',
        'order': '4',
        'displayName': 'Asset Name',
        'mandatory': 'true'
    }
}


def plugin_info():
    return {
        'name': 'ZMQ Subscriber',
        'version': '0.0.1',
        'mode': 'async',
        'type': 'south',
        'interface': '1.0',
        'config': _DEFAULT_CONFIG
    }


def plugin_init(config):
    """Registers ZMQ Subscriber Client

    Args:
        config: JSON configuration document for the South plugin configuration category
    Returns:
        handle: JSON object to be used in future calls to the plugin
    Raises:
    """
    handle = copy.deepcopy(config)
    handle["_zmq"] = ZMQSubscriberClient(handle)
    return handle


def plugin_start(handle):
    global loop
    loop = asyncio.new_event_loop()

    _LOGGER.info('Starting ZMQ south plugin...')
    try:
        _zmq = handle["_zmq"]
        _zmq.loop = loop
        _zmq.start()
    except Exception as e:
        _LOGGER.exception(str(e))
    else:
        _LOGGER.info('ZMQ south plugin started.')

def plugin_reconfigure(handle, new_config):
    """ Reconfigures the plugin

    it should be called when the configuration of the plugin is changed during the operation of the South service;
    The new configuration category should be passed.

    Args:
        handle: handle returned by the plugin initialisation call
        new_config: JSON object representing the new configuration category for the category
    Returns:
        new_handle: new handle to be used in the future calls
    Raises:
    """
    _LOGGER.info('Reconfiguring ZMQ south plugin...')
    plugin_shutdown(handle)

    new_handle = plugin_init(new_config)
    plugin_start(new_handle)

    _LOGGER.info('ZMQ south plugin reconfigured.')
    return new_handle


def plugin_shutdown(handle):
    """ Shut down the plugin

    To be called prior to the South service being shut down.

    Args:
        handle: handle returned by the plugin initialisation call
    Returns:
    Raises:
    """
    global loop
    try:
        _LOGGER.info('Shutting down ZMQ south plugin...')
        _mqtt = handle["_mqtt"]
        _mqtt.stop()
        
        loop.stop()
        loop = None
    except Exception as e:
        _LOGGER.exception(str(e))
    else:
        _LOGGER.info('ZMQ south plugin shut down.')


def plugin_register_ingest(handle, callback, ingest_ref):
    """ Required plugin interface component to communicate to South C server
    Args:
        handle: handle returned by the plugin initialisation call
        callback: C opaque object required to passed back to C->ingest method
        ingest_ref: C opaque object required to passed back to C->ingest method
    """
    global c_callback, c_ingest_ref
    c_callback = callback
    c_ingest_ref = ingest_ref


class ZMQSubscriberClient(object):

    """ zmq listener class"""

    def __init__(self, config):
        self.proxy_host = config['proxyHost']['value']
        self.proxy_port = int(config['proxyPort']['value'])
        self.topic = config['topic']['value']
        self.asset = config['assetName']['value']
        self.poller = Poller(1, self.proxy_host, self.proxy_port, self.topic, zmq.Context())

    def start(self):
        self.poller.start()
        _LOGGER.info("ZMQ Proxy connecting..., Proxy Host: %s, Port: %s", self.proxy_host, self.proxy_port)

    def stop(self):
        self.poller.stop()
        _LOGGER.info("Subscriber stopped")

    async def save(self, msg):
        """Store msg content to Fledge """
        # TODO: string and other types?
        payload_json = json.loads(msg.payload.decode('utf-8'))
        _LOGGER.debug("Ingesting %s on topic %s", payload_json, str(msg.topic)) 
        data = {
            'asset': self.asset,
            'timestamp': utils.local_timestamp(),
            'readings': payload_json
        }
        async_ingest.ingest_callback(c_callback, c_ingest_ref, data)

class Poller(Thread):

    def __init__(self, id, proxy_host, proxy_port, topic, context):
        super().__init__()
        self.id = id
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port
        self.topic = topic
        self.context = context

    def run(self):
        _LOGGER.info('start poller {}'.format(self.id))
        subscriber = self.context.socket(zmq.SUB)
        subscriber.connect('tcp://{}:{}'.format(self.proxy_host, self.proxy_port))
        subscriber.setsockopt_string(zmq.SUBSCRIBE, self.topic)
        self.loop = True
        while self.loop:
            message = subscriber.recv()
            _LOGGER.info('poller {}: {}'.format(self.id, message))

    def stop(self):
        self.loop = False