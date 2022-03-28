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
# TCRM Cloud Environment setup script.

set -e

DEFAULT_COMPOSER_ENV_NAME='tcrm-env'
DEFAULT_LOCAL_DAGS_FOLDER='src/'
DEFAULT_LOCATION=us-central1

APIS_TO_BE_ENABLED=(
 'bigquery-json.googleapis.com'
 'cloudapis.googleapis.com'
 'composer.googleapis.com'
 'googleads.googleapis.com'
 'storage-api.googleapis.com'
)

COMPOSER_PYPI_PACKAGES=(
  'dataclasses'
  'immutabledict'
)

# Handling command line arguments
while [ $# -gt 0 ]; do
  case "$1" in
    --project_id=*)
      project_id="${1#*=}"
      ;;
    --composer_env_name=*)
      composer_env_name="${1#*=}"
      ;;
    --local_dags_folder=*)
      local_dags_folder="${1#*=}"
      ;;
    --location=*)
      location="${1#*=}"
      ;;
  esac
  shift
done

if [[ -z "$project_id" ]]; then
  echo "--project_id is required."
  exit 1
fi

composer_env_name=${composer_env_name:-$DEFAULT_COMPOSER_ENV_NAME}
local_dags_folder=${local_dags_folder:-$DEFAULT_LOCAL_DAGS_FOLDER}
location=${location:-$DEFAULT_LOCATION}

# Download TCRM Dependencies.
if [ -d "./gps_building_blocks" ]
then
    rm -rf "./gps_building_blocks"
fi

if [ -d "./src/plugins/gps_building_blocks" ]
then
    rm -rf "./src/plugins/gps_building_blocks"
fi

git clone "https://github.com/google/gps_building_blocks.git"
mkdir -p src/plugins/gps_building_blocks/cloud/utils
cp -r gps_building_blocks/py/gps_building_blocks/cloud/utils/* src/plugins/gps_building_blocks/cloud/utils

# Enable required Cloud APIs.
for i in "${APIS_TO_BE_ENABLED[@]}"
do
  echo "Enabling Cloud API: $i"
  gcloud services enable "$i" --project "$project_id"
done

# Create Cloud Composer environment.
gcloud composer environments create "$composer_env_name" \
  --location=$location \
  --project "$project_id"

# Composer environment variables.
env_python_path=PYTHONPATH='/home/airflow/gcs:/home/airflow/gcs/plugins'
gcloud composer environments update "$composer_env_name" \
  --location $location \
  --update-env-variables=$env_python_path \
  --project "$project_id"

# Install required Python packages on Cloud Composer environment.
if [[ -f pypi_package.txt ]]; then
  rm pypi_package.txt
  touch pypi_package.txt
fi

for i in "${COMPOSER_PYPI_PACKAGES[@]}"
do
  echo "$i" >> pypi_package.txt
done

env_python_path=PYTHONPATH='/home/airflow/gcs:/home/airflow/gcs/plugins'
gcloud composer environments update "$composer_env_name" \
  --location $location \
  --update-pypi-packages-from-file=pypi_package.txt \
  --project "$project_id"

# Override airflow configuration
gcloud composer environments update "$composer_env_name" \
  --location $location \
  --project "$project_id" \
  --update-airflow-configs=scheduler-catchup_by_default=False

# Copy local DAGs and dependencies to Cloud Storage dag and plugins folders.
echo "Copying dags..."
dag_items="./src/dags/*"
for f in $dag_items
do
  gcloud composer environments storage dags import \
    --environment "$composer_env_name" \
    --source="$f" \
    --location=$location \
    --project "$project_id"
done

echo "Copying plugins..."
plugin_items="./src/plugins/*"
for f in $plugin_items
do
  gcloud composer environments storage plugins import \
    --environment "$composer_env_name" \
    --source="$f" \
    --location=$location \
    --project "$project_id"
done
