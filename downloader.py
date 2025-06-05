

from datetime import datetime
import logging
import sys
import subprocess


logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)


def rsync(ssh_port, identity_file, ssh_username, target_ip, target_path, destination_path):
    command = [
                "rsync", "-avz", "-e", f"ssh -p {ssh_port} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o IdentityFile={identity_file}",
                f"{ssh_username}@{target_ip}:/{target_path}", f"{destination_path}"
    ]
    logging.info("rsync runner - executing: %s" % ''.join(command))
    before = datetime.now()
    result = subprocess.run(command, capture_output=True, text=True)
    logging.info("rsync runner - completed - time elapsed: %s" % str(datetime.now() - before))
    logging.error(result.stderr)
    logging.debug(result.stdout)