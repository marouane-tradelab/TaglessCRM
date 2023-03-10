# coding=utf-8
# Copyright 2021 Google LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/usr/bin/env bash
# Setup TaglessCRM to the new virtual machine in GCP.


#######################################
# Print error log to stderr
# Globals:
#   None
# Arguments:
#   String to output
# Outputs:
#   Writes error log message to stderr.
#######################################
err() {
  echo -e "\n[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*" >&2
}

#######################################
# Create a new user "tcrm" for TCRM files and services.
# Globals:
#   TCRM_USER
# Arguments:
#   None
# Outputs:
#   Writes install log message to stdout.
#######################################
create_tcrm_user() {
  TCRM_USER="tcrm"
  sudo useradd -m "${TCRM_USER}"
  sudo usermod -a -G "${TCRM_USER}" "${USER}"
}

#######################################
# Update OS components and install certbot
# Globals:
#   None
# Arguments:
#   None
# Outputs:
#   Writes install log message to stdout.
#######################################
os_components() {
  sudo apt -y update
  sudo apt -y upgrade
  sudo apt install -y --no-install-recommends \
    build-essential \
    snapd \
    python3-dev \
    python3-pip \
    python3-setuptools \
    git
  sudo snap install core
  sudo snap refresh core
  sudo snap install --classic certbot > /dev/null
  if [[ ! -f '/usr/bin/certbot' ]]; then
    sudo ln -s /snap/bin/certbot /usr/bin/certbot
  fi
}

#######################################
# Requesting SSL certificate from LetsEncrypt
# Globals:
#   DOMAIN
#   EMAIL_ADDRESS
# Arguments:
#   None
# Outputs:
#   Writes install log message to stdout.
#######################################
ssl_certificate() {
  if sudo test -d "/etc/letsencrypt/live/${DOMAIN}"; then
    echo "certificate exists."
  else
    sudo certbot certonly --standalone -m "${EMAIL_ADDRESS}" -d "${DOMAIN}" \
      --non-interactive --agree-tos
    if (( $? != 0 )); then
      err "Unable to create certificate for ${DOMAIN}" >&2
      exit 1
    fi
  fi
}

#######################################
# Renew the SSL certificate every 3 month
# Globals:
#   DOMAIN
#   EMAIL_ADDRESS
# Arguments:
#   None
# Outputs:
#   Writes install log message to stdout.
#######################################
certificate_renew_cronjob() {
  grep "0 0,12 \* \* \* root sleep [0-9]* && certbot renew -q" \
    /etc/crontab  > /dev/null
  if (( $? != 0 )); then
    local sleep_time=$(awk 'BEGIN{srand(); print int(rand()*(3600+1))}')
    echo "0 0,12 * * * root sleep ${sleep_time} && certbot renew -q" \
      | sudo tee -a /etc/crontab > /dev/null
  fi
}

#######################################
# Stop airflow web and scheduler service
# Globals:
#   None
# Arguments:
#   None
# Outputs:
#   None
#######################################
stop_services() {
  sudo systemctl stop airflow-scheduler.service
  sudo systemctl stop airflow-webserver.service
}

#######################################
# Creates OAuth2 configuration for airflow
# Globals:
#   AIRFLOW_HOME
#   CLIENT_ID
#   SECRET
# Arguments:
#   None
# Outputs:
#   None
#######################################
webserver_config() {
  cat <<END | tee ./src/webserver_config.py > /dev/null
import os
from flask_appbuilder.security.manager import AUTH_OAUTH
from airflow.configuration import conf
from airflow.www_rbac.security import AirflowSecurityManager
basedir = os.path.abspath(os.path.dirname(__file__))
SQLALCHEMY_DATABASE_URI = conf.get('core', 'SQL_ALCHEMY_CONN')
WTF_CSRF_ENABLED = True
AUTH_TYPE = AUTH_OAUTH
AUTH_ROLE_ADMIN = 'Admin'
AUTH_USER_REGISTRATION = False

OAUTH_PROVIDERS = [{
  'name':'google',
  'token_key':'access_token',
  'icon':'fa-google',
  'remote_app': {
    'base_url':'https://www.googleapis.com/oauth2/v2/',
    'request_token_params':{'scope': 'email profile' },
    'access_token_url':'https://accounts.google.com/o/oauth2/token',
    'authorize_url':'https://accounts.google.com/o/oauth2/auth',
    'request_token_url': None,
    'consumer_key': '${CLIENT_ID}',
    'consumer_secret': '${SECRET}',
  }
}]
END
}

#######################################
# Creates airflow db and inserts connection configuration and admin user info.
# Globals:
#   AIRFLOW_HOME
#   SA_KEY
#   PROJECT_ID
# Arguments:
#   None
# Outputs:
#   None
#######################################
init_airflow_db () {
  mv "${SA_KEY}" "${AIRFLOW_HOME}"
  key_path="${AIRFLOW_HOME}"/$(basename "$SA_KEY")
  extra="{\"extra__google_cloud_platform__project\":\"${PROJECT_ID}\", \
\"extra__google_cloud_platform__key_path\":\"${key_path}\"}"

  airflow initdb

  airflow users create --role Admin --username google_"${USER_ID}" \
    --password admin --email "${EMAIL_ADDRESS}" --firstname Admin \
    --lastname User

  airflow connections -d --conn_id google_cloud_default
  airflow connections -a --conn_id google_cloud_default \
    --conn_type google_cloud_platform --conn_extra "${extra}"
  airflow connections -d --conn_id bigquery_default
  airflow connections -a --conn_id bigquery_default \
    --conn_type google_cloud_platform --conn_extra "${extra}"
  airflow connections -d --conn_id google_cloud_datastore_default
  airflow connections -a --conn_id google_cloud_datastore_default \
    --conn_type google_cloud_platform --conn_extra "${extra}"
}


#######################################
# Setup TCRM
#   1. Clones repos (TCRM and gps_building_blocks)
#   2. Moves to home dir of user "tcrm"
#   3. Call init_airflow_db function
# Globals:
#   AIRFLOW_HOME
#   TCRM_HOME
#   DOMAIN
# Arguments:
#   None
# Outputs:
#   Writes install log message to stdout.
#######################################
setup_tcrm() {
  stop_services

  if [[ -d TaglessCRM ]]; then
    sudo rm -rf TaglessCRM
  fi

  git clone https://github.com/google/TaglessCRM.git

  cd TaglessCRM

  git clone https://github.com/google/gps_building_blocks.git

  mkdir -p ./src/plugins/gps_building_blocks/cloud/utils

  cp -r gps_building_blocks/py/gps_building_blocks/cloud/utils/* \
    ./src/plugins/gps_building_blocks/cloud/utils

  webserver_config

  sudo pip3 install -r ./requirements.txt --require-hashes --no-deps

  cd ..

  sudo mv TaglessCRM ${TCRM_USER_HOME}

  AIRFLOW__CORE__LOAD_EXAMPLES=False \
  AIRFLOW_HOME=${AIRFLOW_HOME} \
  PYTHONPATH=${AIRFLOW_HOME} \
    init_airflow_db
}

#######################################
# Creates airflow http service
# Globals:
#   AIRFLOW_HOME
#   TCRM_HOME
#   DOMAIN
# Arguments:
#   None
# Outputs:
#   None
#######################################
webserver_service() {
  local ssl_cert=/etc/letsencrypt/live/${DOMAIN}/fullchain.pem
  local ssl_key=/etc/letsencrypt/live/${DOMAIN}/privkey.pem
  local service_path=/etc/systemd/system/airflow-webserver.service
  if [[ -f "${service_path}" ]]; then
    return 0
  fi
  cat <<END | sudo tee "${service_path}" > /dev/null
[Unit]
Description=Airflow webserver daemon
After=network.target
Wants=

[Service]
Environment=PYTHONPATH=${TCRM_HOME}/src
Environment=AIRFLOW_HOME=${TCRM_HOME}/src
Environment=AIRFLOW__CORE__LOAD_EXAMPLES=False
Environment=AIRFLOW__WEBSERVER__WEB_SERVER_PORT=443
Environment=AIRFLOW__WEBSERVER__WEB_SERVER_SSL_CERT=${ssl_cert}
Environment=AIRFLOW__WEBSERVER__WEB_SERVER_SSL_KEY=${ssl_key}
Environment=AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True
Environment=AIRFLOW__WEBSERVER__RBAC=True
AmbientCapabilities=CAP_NET_BIND_SERVICE
User=tcrm
Group=tcrm
Type=simple
ExecStart=/usr/local/bin/airflow webserver --pid ${AIRFLOW_HOME}/webserver.pid
Restart=on-failure
RestartSec=5s
PrivateTmp=true

[Install]
WantedBy=multi-user.target
END
}

#######################################
# Creates airflow scheduler service
# Globals:
#   None
# Arguments:
#   None
# Outputs:
#   None
#######################################
scheduler_service() {
  local service_path=/etc/systemd/system/airflow-scheduler.service
  if [[ -f "${service_path}" ]]; then
    return 0
  fi
  cat <<END | sudo tee "${service_path}" > /dev/null
[Unit]
Description=Airflow scheduler daemon
After=network.target
Wants=

[Service]
Environment=PYTHONPATH=${TCRM_HOME}/src
Environment=AIRFLOW_HOME=${TCRM_HOME}/src
Environment=AIRFLOW__CORE__LOAD_EXAMPLES=False
User=tcrm
Group=tcrm
Type=simple
ExecStart=/usr/local/bin/airflow scheduler
Restart=always
RestartSec=5s

[Install]
WantedBy=multi-user.target
END
}

#######################################
# Give permissions to tcrm user
# Globals:
#   TCRM_HOME
# Arguments:
#   None
# Outputs:
#   None
#######################################
fix_permission() {
  sudo chown -R tcrm:tcrm "${TCRM_HOME}"
  sudo chmod -R 755 "${TCRM_HOME}"
  sudo chown -R root:tcrm  /etc/letsencrypt
  sudo chmod -R 755 /etc/letsencrypt
}

#######################################
# Reload systemd daemon to activate airflow http and scheduler service.
# Globals:
#   None
# Arguments:
#   None
# Outputs:
#   None
#######################################
activate_services() {
  sudo systemctl daemon-reload
  sudo systemctl enable airflow-webserver.service
  sudo systemctl enable airflow-scheduler.service
  sudo systemctl start airflow-scheduler.service
  sudo systemctl start airflow-webserver.service
}

#######################################
# Parse GCP identity token to get Google account id
# Globals:
#   None
# Arguments:
#   None
# Outputs:
#   None
#######################################
parse_identity_token() {
  # Split a JWS token into 3 parts and suppose to use the middle part.
  IFS='.'; tokenized_gcp_token=($(gcloud auth print-identity-token)); unset IFS
  claim_base64=${tokenized_gcp_token[1]}
  str_len=$(echo -n "${claim_base64}" | wc -c )
  num_of_padding=$((str_len%4))

  for ((i=4; i>num_of_padding; i--))
  do
    claim_base64=${claim_base64}=
  done

  EMAIL_ADDRESS=$(echo -n "${claim_base64}" | base64 -di | jq -r .email)
  ACCOUNT_ID=$(echo -n "${claim_base64}" | base64 -di | jq -r .sub)
  echo -e "Current user ${EMAIL_ADDRESS} will be the administrator of TCRM web \
admin.\n"
}

#######################################
# Check if the domain is mapping with the IP of the VM just created.
# Globals:
#   DOMAIN
# Arguments:
#   None
# Outputs:
#   Writes command execution message to stdout.
#######################################
check_domain_mapping() {
  read -r -p "Enter the domain name: " DOMAIN
  if [[ -z "${DOMAIN}" ]]; then
    err "Domain name is required.\n"
    exit 1
  fi

  IP=$(dig +short "${DOMAIN}")
  if [[ -z ${IP} ]]; then
    err "${DOMAIN} cannot be resolved. please check the DNS setup.\n"
    exit 1
  fi

  if [[ "${IP}" != "${EXTERNAL_IP}" ]]; then
    err "${DOMAIN} isn't mapping with IP address ${EXTERNAL_IP}, please check \
the DNS setup.\n"
    exit 1
  fi

  echo -e "\n${DOMAIN} is mapping with IP address ${EXTERNAL_IP} correctly!\n"
}

#######################################
# Get required argument from the user and conduct pre-execution checking.
# Globals:
#   CLIENT_ID
#   SECRET
# Arguments:
#   None
# Outputs:
#   Writes command execution message to stdout.
#######################################
prepare_remote_execute() {
  if [[ -f vm_args.txt ]]; then
    source vm_args.txt
  else
    err "VM information not found, have you executed vm_create.sh already?\n"
    exit 1
  fi

  parse_identity_token
  check_domain_mapping

  read -r -p "Enter the OAUTH2 client_id: " CLIENT_ID
  if [[ -z "${CLIENT_ID}" ]]; then
    err "OAUTH2 client_id is required.\n"
    exit 1
  fi

  echo -e

  read -r -p "Enter the OAUTH2 secret: " SECRET
  if [[ -z "${SECRET}" ]]; then
    err "OAUTH2 secret is required.\n"
    exit 1
  fi

  echo -e
}

#######################################
# Copy this script and service account key file to the VM just created.
# Globals:
#   VM
#   ZONE
#   SA_KEY
# Arguments:
#   None
# Outputs:
#   Writes command execution message to stdout.
#######################################
transfer_installation_files() {
  echo -e "\nCopying necessary files to the VM.\n"
  gcloud compute ssh "${VM}" --zone="${ZONE}" --command="mkdir ~/tcrm"
  gcloud compute scp --zone="${ZONE}" ./tcrm_install.sh "${VM}":~/tcrm/
  gcloud compute scp --zone="${ZONE}" ./"${SA_KEY}" "${VM}":~/tcrm/
}

#######################################
# The main function
#   1. If no command line argument is provided, the script read VM information
#      from vm_args.txt which is created by vm_create.sh, then copy required
#      files to the VM, and executes remotely with arguments.
#   2. If command line arugments are provided, the script executes locally to
#      set up TCRM.
# Globals:
#   VM
#   ZONE
#   SA_KEY
# Arguments:
#   None
# Outputs:
#   Writes command execution message to stdout.
#######################################
main() {
  echo -e "Welcome to TaglessCRM installation\n"
  if (( $# == 0 )); then
    local done="false"
    while [[ "${done}" == "false" ]]; do

      prepare_remote_execute

      while true; do
        read -r -p "Are the above details correct? (Y)es to continue | (N)o to \
repeat entry [Yes]: "  confirmation
        local confirmation=${confirmation:-y}
        case "${confirmation}" in
          [yY] | [yY]es])
            done="true"
            break
            ;;
          [nN] | [nN]o)
            echo -e
            break
            ;;
          *)
            echo -e "Unexpected entry: '${confirmation}'\n"
            ;;
        esac
      done
    done

    transfer_installation_files

    echo -e "\nExecuting installation script.\n"
    gcloud compute ssh "${VM}" --zone="${ZONE}" \
      --command="./tcrm/tcrm_install.sh -w ${DOMAIN} -c ${CLIENT_ID} \
-s ${SECRET} -e ${EMAIL_ADDRESS} -u ${ACCOUNT_ID} -a ~/tcrm/${SA_KEY} \
-p ${PROJECT_ID}" \
        | tee tcrm_install.log
  else
    TCRM_USER_HOME=/home/tcrm
    TCRM_HOME=${TCRM_USER_HOME}/TaglessCRM
    AIRFLOW_HOME=${TCRM_HOME}/src
    PYTHONPATH=${AIRFLOW_HOME}
    DOMAIN=''
    CLIENT_ID=''
    SECRET=''
    EMAIL_ADDRESS=''
    USER_ID=''
    SA_KEY=''
    PROJECT_ID=''
    while getopts 'w:c:s:e:d:u:a:p:' flag; do
      case "${flag}" in
        w) DOMAIN=${OPTARG};;
        c) CLIENT_ID=${OPTARG};;
        s) SECRET=${OPTARG};;
        e) EMAIL_ADDRESS=${OPTARG};;
        u) USER_ID=${OPTARG};;
        a) SA_KEY=${OPTARG};;
        p) PROJECT_ID=${OPTARG};;
        *) echo "usage: $0 [-w] Domain [-c] OAUTH2_client_id \
[-s] OAUTH2_secret [-u] Google account user id [-a] [-p] project_id \
service account key path" >&2
           exit 1;;
      esac
    done
    readonly DOMAIN
    readonly CLIENT_ID
    readonly SECRET
    readonly EMAIL_ADDRESS
    readonly USER_ID
    readonly SA_KEY
    readonly PROJECT_ID

    create_tcrm_user
    os_components
    ssl_certificate
    certificate_renew_cronjob
    setup_tcrm
    webserver_service
    scheduler_service
    fix_permission
    activate_services
  fi
}

main "$@"
