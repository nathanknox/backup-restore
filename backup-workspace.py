# Databricks notebook source
%run ./utils

# COMMAND ----------
install_terraform(version=terraform_version)

# COMMAND ----------
provider_info = install_provider(version=provider_version)

# COMMAND ----------
env = make_tf_env(
    db_host, azure_tenant_id, azure_client_id, azure_client_secret
)

local_path, remote_path = workspace_export(
    env=env,
    provider_info=provider_info,
    workspace_name=workspace_name,
    container_name=backup_container_name,
    storage_account_name=storage_account_name
)

# COMMAND ----------
dbutils.fs.mv(local_path, remote_path, recurse=True)

# COMMAND ----------
print(f"Backup written to: {remote_path}")
