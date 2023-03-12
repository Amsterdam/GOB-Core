"""GOB workflow typing.

See start_commands.json
"""


from typing import Literal, TypedDict


class CommandArgs(TypedDict):
    """Workflow Command Arguments."""

    action: str
    choices: list[str]
    default: str
    description: str
    name: str
    required: bool


class WorkflowCommandsConfig(TypedDict):
    """Workflow Commands configuration."""

    args: list[CommandArgs]
    description: str
    start_step: str
    workflow: str


class WorkflowKwargs(TypedDict, total=False):
    """Workflow Command kwargs."""

    action: str
    choices: list[str]
    default: str
    help: str
    nargs: Literal["?"]
    type: "type"
