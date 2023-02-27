# Databricks notebook source
dbutils.widgets.text(name="terraform_version", defaultValue="1.2.2")
dbutils.widgets.text(name="provider_version", defaultValue="1.1.0")

# COMMAND ----------
%run ./constants

# COMMAND ----------
terraform_version = dbutils.widgets.get("terraform_version")
provider_version = dbutils.widgets.get("provider_version")

azure_client_secret = dbutils.secrets.get(
    "vpcx-secret-scope",
    "azure-client-secret",
)

# COMMAND ----------
spark.conf.set(
    f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net",
    "OAuth",
)
spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
)
spark.conf.set(
    f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net",
    azure_client_id,
)
spark.conf.set(
    f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net",
    azure_client_secret,
)
spark.conf.set(
    f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net",
    f"https://login.microsoftonline.com/{azure_tenant_id}/oauth2/token",
)

# COMMAND ----------
import subprocess
import os
import shlex
import datetime
from functools import partial


def install_terraform(version):

    zipped = f"terraform_{version}_linux_amd64.zip "
    uri = f"https://releases.hashicorp.com/terraform/{version}/terraform_{version}_linux_amd64.zip"

    subprocess.run(
        f"wget --quiet {uri} && unzip {zipped} && mv terraform /usr/bin && rm {zipped}",
        shell=True,
        check=True,
    )


def install_provider(version):

    provider_name = "terraform-provider-databricks"
    zipped = f"{provider_name}_{version}_linux_amd64.zip"
    uri = f"https://github.com/databricks/{provider_name}/releases/download/v{version}/{zipped}"

    subprocess.run(
        f"wget --quiet {uri} && unzip {zipped} -d ./{provider_name} && rm {zipped}",
        shell=True,
        check=True,
    )

    executable_path = os.path.abspath(
        os.path.join(os.path.curdir, provider_name)
    )

    return (executable_path, version)


def make_backup_suffix(workspace_name, backup_id):

    return os.path.join(workspace_name, backup_id)


def make_backup_path(container_name, storage_account_name, backup_suffix):

    return os.path.join(
        f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        backup_suffix,
    )


def workspace_export(
    env, provider_info, workspace_name, container_name, storage_account_name
):

    provider_path, provider_version = provider_info
    now = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    export_suffix = make_backup_suffix(
        workspace_name=workspace_name, backup_id=now
    )
    local_export_path = os.path.join(provider_path, export_suffix)
    remote_export_path = make_backup_path(
        container_name=container_name,
        storage_account_name=storage_account_name,
        backup_suffix=export_suffix,
    )

    exporter_cmd = [
        f"./terraform-provider-databricks_v{provider_version}",
        "exporter",
        "-skip-interactive",
        "-directory",
        f"{local_export_path}",
        "-services=jobs,compute,notebooks",
    ]

    subprocess.run(
        exporter_cmd,
        cwd=provider_path,
        env=env,
        check=True,
        capture_output=True,
    )

    return (f"file://{local_export_path}", remote_export_path)


def execute_tf_cmd(cmd_string, env, cwd):
    cmd = subprocess.run(
        shlex.split(cmd_string),
        env=env,
        cwd=cwd,
        check=True,
        capture_output=True,
    )
    return cmd.stdout.decode()


def make_tf_env(db_host, azure_tenant_id, azure_client_id, azure_client_secret):
    os_env = os.environ.copy()
    tf_dict = {
        "DATABRICKS_HOST": db_host,
        "ARM_TENANT_ID": azure_tenant_id,
        "ARM_CLIENT_ID": azure_client_id,
        "ARM_CLIENT_SECRET": azure_client_secret,
    }
    return {**os_env, **tf_dict}


tf_init = partial(execute_tf_cmd, cmd_string="terraform init -input=false")
tf_perms = partial(execute_tf_cmd, cmd_string="chmod 755 import.sh")
tf_import = partial(execute_tf_cmd, cmd_string="./import.sh")
tf_plan = partial(
    execute_tf_cmd, cmd_string="terraform plan -out=tfplan -input=false"
)
tf_apply = partial(
    execute_tf_cmd, cmd_string="terraform apply -input=false tfplan"
)
