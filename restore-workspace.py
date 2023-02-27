# Databricks notebook source
dbutils.widgets.text(name="backup_id", defaultValue="")

# COMMAND ----------
%run ./constants

# COMMAND ----------
backup_id = dbutils.widgets.get("backup_id")
if not backup_id:
    raise Exception(f"Please enter the id of your desired backup (the DATE in '/mnt/databrick-backups/WORKSPACE-NAME/DATE'). To do this, click 'Run now with different parameters' and fill the value for 'backup_id' with DATE")

# COMMAND ----------
%run ./backup-workspace

# COMMAND ----------
remote_backup_suffix = make_backup_suffix(workspace_name, backup_id)
remote_backup_path = make_backup_path(
    container_name=backup_container_name,
    storage_account_name=storage_account_name,
    backup_suffix=remote_backup_suffix,
)
cp_prefix = f"file://{r_local_prefix}"

cp_path = os.path.join(cp_prefix, remote_backup_suffix)
local_backup_path = os.path.join(r_local_prefix, remote_backup_suffix)

dbutils.fs.cp(remote_backup_path, cp_path, recurse=True)
tf_init(env=env, cwd=local_backup_path)
tf_perms(env=env, cwd=local_backup_path)
tf_import(env=env, cwd=local_backup_path)
tf_plan(env=env, cwd=local_backup_path)
tf_apply(env=env, cwd=local_backup_path)

# COMMAND ----------
print(f"Backup '{remote_backup_suffix}' restored.")
