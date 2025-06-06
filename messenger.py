import sys
from time import sleep
import paho.mqtt.client as mqtt
import logging
from datetime import datetime
import json
import os, errno
import subprocess
import requests
import threading

import downloader

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)

def command_runner(command: list[str]):
    logging.info(f"command runner - executing: {command}")
    before = datetime.now()
    result = subprocess.run(command, capture_output=True, text=True)
    logging.info(f"command runner - completed - elapsed time: {str(datetime.now() - before)}")
    if len(result.stderr) > 0:
        logging.error(result.stderr)
    return result.stdout

class Messenger:
    def __init__(self, settings:dict, aliases:dict, endpoints:dict) -> None:
        
        self.client =  mqtt.Client(mqtt.CallbackAPIVersion.VERSION2) # type: ignore        
        self.client.enable_logger()
        
        self.client.username_pw_set(settings['MQTT_USERNAME'], settings['MQTT_PASSWORD'])
        
        topics = settings['MQTT_TOPICS']
        self.topics = []
        for topic in topics:
            self.topics.append((topic, 2))
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        
        self.message_save_path = settings['MQTT_SAVE_PATH']
        self.download_destination_path = settings['DESTINATION_PATH']
        self.key_folder_path = settings['KEY_FOLDER_PATH']
        self.ssh_username = settings['SSH_USERNAME']
        self.ssh_port = settings['SSH_PORT']
        self.aliases =aliases
        self.endpoints = endpoints
        self._download_lock = threading.Lock()
        self._write_msg_lock = threading.Lock()
        self.client.connect(settings['MQTT_BROKER_HOST'])
        
    
    def put_pubkey(self, target:str, pubkey:str):
        url = f"{self.endpoints[target]}/{self.ssh_username}"
        headers = {'Content-Type': 'application/json'}
        data = json.dumps({'key': pubkey})
        logging.info(f"put_pubkey - target {url} - pubkey {pubkey}" )
        response = requests.put(url=url, headers=headers, data=data)
        if response.status_code == 204:
            logging.debug("put_pubkey - 204 recieved")
            return True
        logging.error(f"put_pubkey - did not recieve 204 status code, recieved: {response.status_code} - response.text = {response.text}")
        return False
    
    def delete_pubkey(self, target:str, pubkey:str):
        url = f"{self.endpoints[target]}/{self.ssh_username}"
        headers = {'Content-Type': 'application/json'}
        data = json.dumps({'key': pubkey})
        logging.info(f"delete_pubkey - target {url} - pubkey {pubkey}")
        response = requests.delete(url=url, headers=headers, data=data)
        if response.status_code == 204:
            logging.debug("delete_pubkey - 204 recieved")
        else:
            logging.error(f"delete_pubkey - did not recieve 204 status code, recieved: {response.status_code} - response.text = {response.text}")
    
    def keygen(self, key_name:str):
        target = f"{self.key_folder_path}/{key_name}"
        logging.info(f"Keygen - generating key at: {target}" )
        try:
            os.remove(target)
            logging.debug(f"Keygen - successful removed previous key at: {target}")
        except OSError as e: # this would be "except OSError, e:" before Python 2.6
            logging.debug("Keygen - failed to remove previous key (probably didnt exist)")
            if e.errno != errno.ENOENT: # errno.ENOENT = no such file or directory
                raise # re-raise exception if a different error occurred
        keygen_cmd = [
            "ssh-keygen", "-t", "rsa", "-q", "-f", target, "-N", '',     # generate key named $key_name using RSA with no password...
        ]
        logging.debug(f"keygen - {command_runner(keygen_cmd)}")
        read_key_cmd = [
            "ssh-keygen", "-y", "-f", target                              # write the pubkey to stdout
        ]
        return command_runner(read_key_cmd)
   
   
   
    def download(self, username:str, torrent_id:str, content_path:str):
        pubkey = self.keygen(torrent_id)
        if self.put_pubkey(username, pubkey):
            sleep(5)
            with self._download_lock:
                logging.debug("Download lock acquired")
                downloader.rsync(
                    ssh_port = self.ssh_port, identity_file = f"{self.key_folder_path}/{torrent_id}", ssh_username = self.ssh_username, 
                    target_ip = self.aliases[username], target_path = content_path, destination_path = f"{self.download_destination_path}{content_path}"
                )
            logging.debug("Download lock released")
            self.delete_pubkey(username, pubkey)
    
    def on_connect(self, client, userdata, flags, reason_code, properties):
        client.subscribe(self.topics)

    def message_handler(self, msg:mqtt.MQTTMessage):
        message_dict={
                        'topic': msg.topic,
                        'payload': json.loads(msg.payload.decode('utf-8'))
        }
        
        if(message_dict['topic'] == "torrents"):
            logging.info(f"topic: {message_dict['topic']} - payload: {message_dict['payload']}")
            payload = message_dict['payload']
            with self._write_msg_lock:
                logging.debug("Write msg lock acquired")
                self.save_message(message_dict)
            logging.debug("Write msg lock released")
            self.download(payload['username'], payload['torrent_id'], payload['content_path'])
        else:
            logging.debug(f"topic: {message_dict['topic']} - payload: {message_dict['payload']}")

    def on_message(self, client, userdata, msg: mqtt.MQTTMessage):
        #userdata.append(msg.payload)
        try:
            threading.Thread(target=self.message_handler, args=(msg,), daemon=True).start()
        except Exception as e:
            logging.exception("failed to start thread: %s", e)
        
        
    def save_message(self, message_dict):
        saveformat = {
                str(datetime.now()):message_dict
            }
        logging.debug("save_message - saving message")
        with open(self.message_save_path, 'a') as out:
            json.dump(saveformat, out)
            out.write('\n')
    
    def listen(self):
        self.client.loop_forever()
           
def main(settings_path, aliases_path, endpoints_path):
    with open(settings_path, 'r') as settings_file:
        settings = json.loads(settings_file.read())
    with open(aliases_path, 'r') as alias_file:
        aliases = json.loads(alias_file.read())
    with open(endpoints_path, 'r') as endpoints_file:
        endpoints = json.loads(endpoints_file.read())
    messenger = Messenger(settings, aliases, endpoints)
    messenger.listen()
        
if __name__ == '__main__':
    sys.exit(main("/appdata/settings.json", "/appdata/aliases.json", "/appdata/endpoints.json"))