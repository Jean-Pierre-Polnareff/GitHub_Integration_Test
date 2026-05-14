import sys
from azure.identity import DefaultAzureCredential
from fabric_cicd import FabricWorkspace, publish_all_items

workspace_name = sys.argv[sys.argv.index('--workspace_name') + 1]

credential = DefaultAzureCredential()

workspace = FabricWorkspace(
    workspace_name=workspace_name,
    repository_directory=f"PowerBI/{workspace_name}",
    item_type_in_scope=["Report", "SemanticModel"],
    token_credential=credential
)
publish_all_items(workspace)