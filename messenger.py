import sys
import paho.mqtt.client as mqtt
import logging
from datetime import datetime, timedelta
import json
import asyncio

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)


async def subprosseser_spawner(command: str):
    download_client =await asyncio.create_subprocess_exec(
        command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await download_client.communicate()
    logging.info(stdout)
    logging.error(stderr)
    

class Messenger:
    def __init__(self, settings:dict, aliases:dict) -> None:
        
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
        self.aliases =aliases
        self.client.connect(settings['MQTT_BROKER_HOST'])
    
    def _lftp_cmd_(self, alias, target_path):
        return ''.join(["cd",f"{self.download_destination_path};","lftp", f"sftp://{self.aliases[alias]}/", "-e", f"pget -n {target_path}"])
    
    def command_runner(self, command: str):
        logging.info("command runner - executing: %s" % command)
        before = datetime.now()
        asyncio.run(subprosseser_spawner(command))
        logging.info("command runner - completed - elapsed time: %s" % str(datetime.now() - before))
        
    
    def on_connect(self, client, userdata, flags, reason_code, properties):
        client.subscribe(self.topics)
    
    def download(self, target_alias, target_path):
        command = self._lftp_cmd_(target_alias, target_path)
        self.command_runner(command)
        
    
    def on_message(self, client, userdata, msg):
        #userdata.append(msg.payload)
        if(msg.topic == "torrents"):
            logging.info(msg.payload)
            message_dict={
                        "topic": msg.topic,
                        "payload": json.loads(msg.payload)
                    }
            payload = message_dict['payload']
            self.download(payload['username'], payload['tar_path'])
            ##
            self.save_message(msg)
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
           
def main(settings_path, aliases_path):
    with open(settings_path, 'r') as settings_file:
        settings = json.loads(settings_file.read())
    with open(aliases_path, 'r') as alias_file:
        aliases = json.loads(alias_file.read())
    messenger = Messenger(settings, aliases)
    messenger.listen()
        
if __name__ == '__main__':
    sys.exit(main("settings.json", "aliases.json"))