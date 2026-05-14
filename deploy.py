import sys
from fabric_cicd import FabricWorkspace, publish_all_items

workspace_name = sys.argv[sys.argv.index('--workspace_name') + 1]

workspace = FabricWorkspace(
    workspace_name=workspace_name,
    repository_directory=workspace_name,
    item_type_in_scope=["Report", "SemanticModel"]
)
publish_all_items(workspace)