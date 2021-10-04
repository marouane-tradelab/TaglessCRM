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
# Creates a VM for preparation of the TaglessCRM installation
# with all the necessary settings.

#######################################
# Get required argument from the user for creating the VM
# Globals:
#   GOOGLE_CLOUD_PROJECT
#   PROJECT_ID
#   VM
#   VM_ARGS
#   ZONE
#   MACHINE_TYPE
#   BOOTDISK_SIZE
#   ACTIVE_USER
#   SA_USER
# Arguments:
#   None
# Outputs:
#   Writes command execution message to stdout.
#######################################
prompt_vm_settings() {
  local done="false"

  while [[ "${done}" == "false" ]]; do
    read -r -p "Enter your project ID [${GOOGLE_CLOUD_PROJECT:-tcrm-project}]: " PROJECT_ID
    if [[ -z "${PROJECT_ID}" ]]; then
      PROJECT_ID=${GOOGLE_CLOUD_PROJECT:-tcrm-project}
    fi
    echo -e "\nEntered project id: ${PROJECT_ID}\n"
    echo "PROJECT_ID=${PROJECT_ID}" | tee -a "${VM_ARGS}" > /dev/null

    read -r -p "Enter your VM instance name [${VM:-tcrm-vm}]: " VM
    VM=${VM:-tcrm-vm}
    echo -e "\nEntered name: ${VM}\n"
    echo "VM=${VM}" | tee -a "${VM_ARGS}" > /dev/null

    read -r -p "Enter your VM zone [${ZONE:-us-west1-a}]: " ZONE
    ZONE=${ZONE:-us-west1-a}
    echo -e "\nEntered zone: ${ZONE}\n"
    echo "ZONE=${ZONE}" | tee -a "${VM_ARGS}" > /dev/null

    read -r -p "Enter your preferred machine type [${MACHINE_TYPE:-n1-standard-1}]: " MACHINE_TYPE
    MACHINE_TYPE=${MACHINE_TYPE:-n1-standard-1}
    echo -e "\nEntered machine type: ${MACHINE_TYPE}\n"
    echo "MACHINE_TYPE=${MACHINE_TYPE}" | tee -a "${VM_ARGS}" > /dev/null

    read -r -p "Enter your preferred bootdisk size [${BOOTDISK_SIZE:-10GB}]: " BOOTDISK_SIZE
    BOOTDISK_SIZE=${BOOTDISK_SIZE:-10GB}
    echo -e "\nEntered bootdisk size: ${BOOTDISK_SIZE}\n"
    echo "BOOTDISK_SIZE=${BOOTDISK_SIZE}" | tee -a "${VM_ARGS}" > /dev/null

    ACTIVE_USER=$(gcloud config get-value account)

    read -r -p "Enter your preferred service account user [${SA_USER:-tcrm-sa}]: " SA_USER
    SA_USER=${SA_USER:-tcrm-sa}
    echo -e "\nEntered service account user: ${SA_USER}\n"
    echo "SA_USER=${SA_USER}" | tee -a "${VM_ARGS}" > /dev/null

    while true; do
      read -r -p "Are the above details correct? (Y)es to continue | (N)o to repeat entry [Yes]: "  confirmation
      local confirmation=${confirmation:-y}
      case "${confirmation}" in
        [yY] | [yY]es])
          done="true"
          break
          ;;
        [nN] | [nN]o)
          break
          ;;
        *)
          echo -e "Unexpected entry: '${confirmation}'\n"
          ;;
      esac
    done
  done
}

#######################################
# Setup Cloud Firewall and Service Account
# Globals:
#   PROJECT_ID
#   VM_ARGS
#   SA_USER
#   SA_EMAIL
#   SA_KEY
# Arguments:
#   None
# Outputs:
#   Writes command execution message to stdout.
#######################################
configure_vm_settings() {
  gcloud config set project "${PROJECT_ID}"

  printf "\nCreating service account for VM...\n\n"
  SA_EMAIL=${SA_USER}@${PROJECT_ID}.iam.gserviceaccount.com
  SA_KEY=tcrm-service-account.json
  echo "SA_EMAIL=${SA_EMAIL}" | tee -a "${VM_ARGS}" > /dev/null
  echo "SA_KEY=${SA_KEY}" | tee -a "${VM_ARGS}" > /dev/null

  gcloud iam service-accounts create "${SA_USER}" \
    --project "${PROJECT_ID}" \
    --display-name "${SA_USER}"

  gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role='roles/editor'

  printf "\nSaving service account JSON key file [%s]...\n\n" "${SA_KEY}"

  gcloud iam service-accounts keys create "${SA_KEY}" \
    --iam-account="${SA_EMAIL}"

  gcloud compute firewall-rules create "tcrm-tcp-rule" --allow=tcp:443,tcp:80
}

#######################################
# Create the VM
# Globals:
#   VM
#   VM_ARGS
#   MACHINE_TYPE
#   BOOTDISK_SIZE
#   ZONE
# Arguments:
#   None
# Outputs:
#   Writes command execution message to stdout.
#######################################
create_vm() {
  printf "\nCreating your TCRM VM instance...\n\n"

  gcloud compute instances create "${VM}" --zone "${ZONE}" \
    --machine-type "${MACHINE_TYPE}" \
    --boot-disk-size="${BOOTDISK_SIZE}" \
    --scopes https://www.googleapis.com/auth/cloud-platform \
    --tags https-server \
      | grep -oP 'EXTERNAL_IP: \K.*' \
      | awk '{print "EXTERNAL_IP="$1}' \
      | tee -a "${VM_ARGS}" > /dev/null
}

#######################################
# Remove the VM arguments from previous run
# Globals:
#   VM_ARGS
# Arguments:
#   None
# Outputs:
#   Writes command execution message to stdout.
#######################################
cleanup_existing_args() {
  if [[ -f ${VM_ARGS} ]]; then
    rm "${VM_ARGS}"
  fi
}

#######################################
# Confirm if the create VM is ready
# Globals:
#   VM
#   ZONE
# Arguments:
#   None
# Outputs:
#   Writes command execution message to stdout.
#######################################
check_vm_up() {
  local counter=0
  local maxRetry=10

  while true ; do
    if (( $counter == $maxRetry )); then
      echo "VM failed to startup"
      exit 1
    fi

    gcloud compute ssh --quiet "${VM}" --zone="${ZONE}" --tunnel-through-iap \
    --command="true" 2> /dev/null

    if (( $? == 0 )); then
      printf "\nYour VM: ${VM} is ready for installation.\n\n"
      exit 0
    else
      ((counter++))
      sleep 1
    fi
  done
}

#######################################
# The main function
#   1. Ask user for required argument
#   2. Setup firewall and service account
#   3. Create a VM
#
# Globals:
#   None
# Arguments:
#   None
# Outputs:
#   Writes command execution message to stdout.
#######################################
main() {
  VM_ARGS=vm_args.txt

  printf "Welcome to TCRM VM creation\n\n"

  cleanup_existing_args

  #1. Get vm settings
  prompt_vm_settings

  #2. Configure VM
  configure_vm_settings

  #3. Create VM
  create_vm

  #4. Check if VM is ready
  check_vm_up
}

main "$@"
