#!/usr/bin/python3

import argparse
import mlflow
import os
import posixpath

from mlflow.utils.databricks_utils import get_databricks_host_creds
from mlflow.utils.file_utils import relative_path_to_artifact_path
from mlflow.utils.rest_utils import http_request_safe
from mlflow.utils.string_utils import strip_prefix
from mlflow.exceptions import MlflowException
from mlflow.tracking import artifact_utils


def _get_dbfs_endpoint(artifact_uri, artifact_path):
    return "/dbfs/%s/%s" % (strip_prefix(artifact_uri.rstrip('/'), 'dbfs:/'), strip_prefix(artifact_path, '/'))


def _copy_artifact(local_file, artifact_uri, artifact_path=None):
    basename = os.path.basename(local_file)
    if artifact_path:
        http_endpoint = _get_dbfs_endpoint(artifact_uri, posixpath.join(artifact_path, basename))
    else:
        http_endpoint = _get_dbfs_endpoint(artifact_uri, basename)

    host_creds = get_databricks_host_creds('registry')
    print("Copying file to " + http_endpoint + " in registry workspace")
    try:
        if os.stat(local_file).st_size == 0:
            # The API frontend doesn't like it when we post empty files to it using
            # `requests.request`, potentially due to the bug described in
            # https://github.com/requests/requests/issues/4215
            http_request_safe(host_creds, endpoint=http_endpoint, method='POST', data="", allow_redirects=False)
        else:
            with open(local_file, 'rb') as f:
                http_request_safe(host_creds, endpoint=http_endpoint, method='POST', data=f, allow_redirects=False)
    except MlflowException as e:
        # Note: instead of catching the error here, we could check for the existence of file before trying the copy.
        if "File already exists" in e.message:
            print("File already exists - continuing to the next file.")
            import time
        else:
            throw(e)


# params:
#   artifact_uri: the base path for the run.
#   artifact_path: the relative path under `artifact_uri` to the model.
def copy_artifacts(artifact_uri, artifact_path):
    local_dir = "/dbfs/%s/%s" % (strip_prefix(artifact_uri.rstrip('/'), 'dbfs:/'), strip_prefix(artifact_path, '/'))
    artifact_path = artifact_path or ''
    for (dirpath, _, filenames) in os.walk(local_dir):
        artifact_subdir = artifact_path
        if dirpath != local_dir:
            rel_path = os.path.relpath(dirpath, local_dir)
            rel_path = relative_path_to_artifact_path(rel_path)
            artifact_subdir = posixpath.join(artifact_path, rel_path)
        for name in filenames:
            file_path = os.path.join(dirpath, name)
            _copy_artifact(file_path, artifact_uri, artifact_subdir)

def main():
    parser = argparse.ArgumentParser(description="Execute python scripts in Databricks")
    parser.add_argument("-s", "--shard", help="Databricks workspace", required=True)
    parser.add_argument("-t", "--token", help="Databricks token", required=True)
    parser.add_argument("-m", "--model_name", help="Model Registry Name", required=True)
    args = parser.parse_args()

    shard = args.shard
    token = args.token
    model_name = args.model_name

    cli_profile_name = "registry"
    dbutils.fs.put(f"file:///root/.databrickscfg", f"[{cli_profile_name}]\nhost={shard}\ntoken={token}",
                   overwrite=True)

    TRACKING_URI = f"databricks://{cli_profile_name}"
    print(f"TRACKING_URI: {TRACKING_URI}")
    artifact_path = 'model'
    # TODO: WIP, capture the run_id from the model registry
    # artifact_uri = artifact_utils.get_artifact_uri(run_id)
    #
    # print(f"artifact_uri: {artifact_uri}")
    #
    # copy_artifacts(artifact_uri, artifact_path)
    # from mlflow.tracking import MlflowClient
    # remote_client = MlflowClient(tracking_uri=TRACKING_URI)



if __name__ == '__main__':
    main()

