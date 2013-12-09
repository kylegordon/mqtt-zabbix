#!/usr/bin/env python
# -*- coding: iso-8859-1 -*-

__author__ = "Kyle Gordon"
__copyright__ = "Copyright (C) Kyle Gordon"

import os
import logging
import signal
import socket
import time
import sys
import csv

import mosquitto
import ConfigParser
import pyzabbix

from datetime import datetime, timedelta

from zbxsend import Metric, send_to_zabbix

# Read the config file
config = ConfigParser.RawConfigParser()
config.read("/etc/mqtt-zabbix/mqtt-zabbix.cfg")

# Use ConfigParser to pick out the settings
DEBUG = config.getboolean("global", "debug")
LOGFILE = config.get("global", "logfile")
MQTT_HOST = config.get("global", "mqtt_host")
MQTT_PORT = config.getint("global", "mqtt_port")
MQTT_TOPIC = config.get("global", "mqtt_topic")

POLLINTERVAL = config.getint("global", "pollinterval")

KEYFILE = config.get("global", "keyfile")
KEYHOST = config.get("global", "keyhost")
ZBXSERVER = config.get("global", "zabbix_server")
ZBXPORT = config.getint("global", "zabbix_port")

client_id = "zabbix_%d" % os.getpid()
mqttc = mosquitto.Mosquitto(client_id)

LOGFORMAT = "%(asctime)-15s %(message)s"

if DEBUG:
    logging.basicConfig(filename=LOGFILE, level=logging.DEBUG, format=LOGFORMAT)
else:
    logging.basicConfig(filename=LOGFILE, level=logging.INFO, format=LOGFORMAT)

logging.info("Starting mqtt-zabbix")
logging.info("INFO MODE")
logging.debug("DEBUG MODE")

def cleanup(signum, frame):
     """
     Signal handler to ensure we disconnect cleanly 
     in the event of a SIGTERM or SIGINT.
     """
     logging.info("Disconnecting from broker")
     # FIXME - This status topic is too far up the hierarchy.
     mqttc.publish("/status/" + socket.getfqdn(), "Offline")
     mqttc.disconnect()
     logging.info("Exiting on signal %d", signum)
     sys.exit(signum)

def connect():
    """
    Connect to the broker, define the callbacks, and subscribe
    """
    logging.debug("Connecting to %s:%s", MQTT_HOST, MQTT_PORT)
    result = mqttc.connect(MQTT_HOST, MQTT_PORT, 60, True)
    if result != 0:
        logging.info("Connection failed with error code %s. Retrying", result)
        time.sleep(10)
        connect()

    #define the callbacks
    mqttc.on_message = on_message
    mqttc.on_connect = on_connect
    mqttc.on_disconnect = on_disconnect

    logging.debug("Subscribing to %s", MQTT_TOPIC)
    mqttc.subscribe(MQTT_TOPIC, 2)

def on_connect(result_code):
     """
     Handle connections (or failures) to the broker.
     """
     ## FIXME - needs fleshing out http://mosquitto.org/documentation/python/
     if result_code == 0:
        logging.info("Connected to broker")
        mqttc.publish("/status/" + socket.getfqdn(), "Online")
     else:
        logging.warning("Something went wrong")
        cleanup()

def on_disconnect(result_code):
     """
     Handle disconnections from the broker
     """
     if result_code == 0:
        logging.info("Clean disconnection")
     else:
        logging.info("Unexpected disconnection! Reconnecting in 5 seconds")
        logging.debug("Result code: %s", result_code)
        time.sleep(5)
        connect()
        main_loop()

class KeyMap:
    """
    Read the topics and keys into a dictionary for internal lookups
    """
    logging.debug("Loading map")
    with open(KEYFILE, mode="r") as inputfile:
        reader = csv.reader(inputfile)
        mapdict = dict((rows[0],rows[1]) for rows in reader)


def on_message(msg):
    """
    What to do once we receive a message
    """
    logging.debug("Received: " + msg.topic)
    if msg.topic in KeyMap.mapdict:
        logging.info("Sending %s %s to Zabbix key %s", msg.topic, msg.payload, KeyMap.mapdict[msg.topic])
	## Zabbix can also accept text and character data... should we sanitize input or just accept it as is?
        send_to_zabbix([Metric(KEYHOST, KeyMap.mapdict[msg.topic], msg.payload)], ZBXSERVER, ZBXPORT)
    else:
        # Received something with a /raw/ topic, but it didn't match. We don't really care about them
        logging.debug("Unknown: %s", msg.topic)


def main_loop():
    """
    The main loop in which we stay connected to the broker
    """
    while mqttc.loop() == 0:
	logging.debug("Looping")
    
# Use the signal module to handle signals
signal.signal(signal.SIGTERM, cleanup)
signal.signal(signal.SIGINT, cleanup)

# Connect to the broker and enter the main loop
connect()
main_loop()
