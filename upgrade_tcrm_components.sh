# coding=utf-8
# Copyright 2022 Google LLC.
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

# TCRM Cloud Environment upgrade script.
#
# This script copies TCRM components to a Cloud Storage bucket attached to an
# existing Cloud Composer environment.
#
# Usage: upgrade_tcrm_components.sh -n [COMPOSER_BUCKET_NAME]

dags_to_delete=(
    bq_to_ads_cm_dag.py
    bq_to_ads_oc_dag.py
    gcs_to_ads_cm_dag.py
    gcs_to_ads_oc_dag.py
)

dags_to_upgrade=(
    base_dag.py
    bq_to_ads_cm_dag_v2.py
    bq_to_ads_oc_dag_v2.py
    bq_to_ads_uac_dag.py
    bq_to_cm_dag.py
    bq_to_ga4_dag.py
    bq_to_ga_dag.py
    gcs_to_ads_cm_dag_v2.py
    gcs_to_ads_oc_dag_v2.py
    gcs_to_ads_uac_dag.py
    gcs_to_cm_dag.py
    gcs_to_ga4_dag.py
    gcs_to_ga_dag.py
    monitoring_cleanup_dag.py
)

hooks_to_upgrade=(
    ads_cm_hook_v2.py
    ads_hook_v2.py
    ads_oc_hook_v2.py
    ads_uac_hook.py
    bq_hook.py
    cm_hook.py
    ga4_hook.py
    ga_hook.py
    gcs_hook.py
    input_hook_interface.py
    monitoring_hook.py
    output_hook_interface.py
)

hooks_to_delete=(
    ads_cm_hook.py
    ads_hook.py
    ads_oc_hook.py
)

operators_to_upgrade=(
    data_connector_operator.py
    error_report_operator.py
    monitoring_cleanup_operator.py
)

utils_to_upgrade=(
    async_utils.py
    blob.py
    errors.py
    hook_factory.py
    retry_utils.py
    system_testing_utils.py
    type_alias.py
)

function exit_if_bucket_does_not_exist() {
  gsutil -q stat "gs://$bucket_name/*"
  if [ $? -ne 0 ]; then
    message="GCS bucket: $bucket_name does not exist. Please check the bucket"
    message+=" name again."
    echo "$message"
    exit 2
  fi
}

function delete_legacy_files() {
  argv=("$@")
  argc=${#argv[@]}

  src="${argv[0]}"

  for file_name in "${argv[@]:1:$argc}"; do
    gsutil rm "$src/$file_name"
  done
}

function copy_files_to_bucket() {
  argv=("$@")
  argc=${#argv[@]}

  src="${argv[0]}"
  dst="${argv[1]}"

  for file_name in "${argv[@]:2:$argc}"; do
    gsutil cp "$src/$file_name" "$dst/$file_name"
  done
}

upgrade_composer_bucket_files() {
  delete_legacy_files "gs://$bucket_name/plugins/pipeline_plugins/hooks" \
    "${hooks_to_delete[@]}"
  delete_legacy_files "gs://$bucket_name/dags" "${dags_to_delete[@]}"
  copy_files_to_bucket "src/dags" \
    "gs://$bucket_name/dags" "${dags_to_upgrade[@]}"
  copy_files_to_bucket "src/plugins/pipeline_plugins/hooks" \
    "gs://$bucket_name/plugins/pipeline_plugins/hooks" "${hooks_to_upgrade[@]}"
  copy_files_to_bucket "src/plugins/pipeline_plugins/operators" \
    "gs://$bucket_name/plugins/pipeline_plugins/operators" \
      "${operators_to_upgrade[@]}"
  copy_files_to_bucket "src/plugins/pipeline_plugins/utils" \
    "gs://$bucket_name/plugins/pipeline_plugins/utils" "${utils_to_upgrade[@]}"
}

show_help() {
    echo "usage: sh ./$(basename -- "$0") -n [COMPOSER_BUCKET_NAME]"
}

while getopts 'n:' OPTION; do
    case "$OPTION" in
        n)  bucket_name="$OPTARG"
            exit_if_bucket_does_not_exist
            upgrade_composer_bucket_files
            ;;

        *)  show_help
            exit 1
    esac
done

if [ $OPTIND -eq 1 ]; then show_help; fi
