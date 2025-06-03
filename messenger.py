import sys
from time import sleep
import paho.mqtt.client as mqtt
import logging
from datetime import datetime, timedelta
import json
import asyncio
import os, errno
import subprocess
import requests

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)

def command_runner(command: list[str]):
    logging.info("command runner - executing: %s" % ''.join(command))
    before = datetime.now()
    result = subprocess.run(command, capture_output=True, text=True)
    logging.info("command runner - completed - elapsed time: %s" % str(datetime.now() - before))
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
        
        self.client.connect(settings['MQTT_BROKER_HOST'])
        
    
    def put_pubkey(self, target:str, pubkey:str):
        url = self.endpoints[target]
        headers = {'Content-Type': 'application/json'}
        data = {'key': pubkey}
        response = requests.put(url=url, headers=headers, data=data)
        if response.status_code == 204:
            return True
        logging.error("Did not recieve 204 status code, recieved: %d, response.text = %s" % response.status_code % response.text)
        return False
    
    def delete_pubkey(self, target:str, pubkey:str):
        url = self.endpoints[target]
        headers = {'Content-Type': 'application/json'}
        data = {'key': pubkey}
        response = requests.delete(url=url, headers=headers, data=data)
        if response.status_code == 204:
            logging.info("Successfully deleted pubkey")
        logging.error("Did not recieve 204 status code, recieved: %d, response.text = %s" % response.status_code % response.text)
    
    def keygen(self, key_name:str):
        target = f"{self.key_folder_path}/{key_name}"
        try:
            os.remove(target)
        except OSError as e: # this would be "except OSError, e:" before Python 2.6
            if e.errno != errno.ENOENT: # errno.ENOENT = no such file or directory
                raise # re-raise exception if a different error occurred
        keygen_cmd = [
            "cd", self.key_folder_path,                                            # move to target folder...
            "&&",                                                           # and...
            "ssh-keygen", "-t", "rsa", "-q", "-f", key_name, "-N", r'""',   # generate key named $key_name using RSA with no password...
            "&&",                                                           # and...
            "ssh-keygen", "-y", "-f", key_name                              # write the pubkey to stdout
        ]
        return command_runner(keygen_cmd)
   
   
   
    async def download(self, username:str, torrent_id:str, content_path:str):
        pubkey = self.keygen(torrent_id)
        if self.put_pubkey(username, pubkey):
            sleep(5)
            download_cmd = [
                "rsync", "-avz", "-e", f"ssh -p {self.ssh_port} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o IdentityFile={self.key_folder_path}/{torrent_id}",
                    f"{self.ssh_username}@{self.aliases[username]}:/{content_path}", f"{self.download_destination_path}{content_path}"
            ]
            stdout = command_runner(download_cmd)
            logging.info("rsync stdout: %s" % stdout)
            self.delete_pubkey(username, pubkey)
    
    def on_connect(self, client, userdata, flags, reason_code, properties):
        client.subscribe(self.topics)

    def on_message(self, client, userdata, msg: mqtt.MQTTMessage):
        #userdata.append(msg.payload)
        if(msg.topic == "torrents"):
            logging.info(msg.payload)
            message_dict={
                        "topic": msg.topic,
                        "payload": json.loads(msg.payload.decode('utf-8'))
                    }
            payload = message_dict['payload']
            self.save_message(message_dict)
            ##LOCKING NEEDS IMPLEMENTING
            asyncio.run(self.download(payload['username'], payload['torrent_id'], payload['content_path']))
        else:
            logging.debug(msg.payload)
        
    def save_message(self, message_dict):
        saveformat = {
                str(datetime.now()):message_dict
            }
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
    sys.exit(main("settings.json", "aliases.json", "endpoints.json"))