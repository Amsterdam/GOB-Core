import argparse
import json
import os
import sys
from typing import Optional

from gobcore.exceptions import GOBException

START_COMMANDS_CONFIG = 'start_commands.json'


class NoSuchCommandException(GOBException):
    pass


class InvalidArgumentsException(GOBException):
    pass


class StartCommandArgument:
    name: str = ''
    description: str = ''
    required: bool = False
    default: str = None
    choices: list = None
    action: str = None

    def __init__(self, config: dict):
        assert 'name' in config

        self.name = config['name']
        self.description = config.get('description', '')
        self.required = config.get('required', False)
        self.named = config.get('named', False)
        self.default = config.get('default')
        self.choices = config.get('choices')
        self.action = config.get('action')


class StartCommand:
    name: str = ''
    description: str = ''
    args: list = []
    workflow: str = None
    start_step: str = None

    def __init__(self, command_name: str, command_config: dict):
        assert 'workflow' in command_config

        self.name = command_name
        self.description = command_config.get('description', '')
        self.workflow = command_config['workflow']
        self.start_step = command_config.get('start_step')
        self.args = []

        for arg in command_config.get('args', []):
            self.args.append(StartCommandArgument(arg))

    def validate_arguments(self, arguments: dict):
        for arg in self.args:
            input_value = arguments[arg.name] if arg.name in arguments else None

            if arg.required and not input_value:
                raise InvalidArgumentsException(f"Argument {arg.name} is required")

            if arg.choices and input_value not in arg.choices:
                raise InvalidArgumentsException(f"Argument {arg.name} must be one of {','.join(arg.choices)}")


class StartCommands:
    commands: dict = {}

    def __init__(self, commands: Optional[list] = None):
        file_location = os.path.join(os.path.abspath(os.path.dirname(__file__)), START_COMMANDS_CONFIG)

        with open(file_location) as f:
            config = json.load(f)

        self.commands = {
            name: StartCommand(name, cfg) for name, cfg in config.items() if commands is None or name in commands
        }

    def get(self, name: str) -> StartCommand:
        if name not in self.commands:
            raise NoSuchCommandException()

        return self.commands[name]

    def get_all(self) -> dict[str, StartCommand]:
        return self.commands


class WorkflowCommands:

    def __init__(self, commands: list[str]):
        start_commands = StartCommands(commands)

        usage = f'''[<command> [--user USER] [<args>]]

The GOB workflow commands are:'''

        for name, comm in start_commands.get_all().items():
            usage += f'''
    {name:25s}{comm.description}'''

        parser = argparse.ArgumentParser(
            prog='python -m gobworkflow.start',
            description='Start GOB Jobs',
            epilog='Generieke Ontsluiting Basisregistraties',
            usage=usage
        )
        parser.add_argument('command', help='Command to run')
        args = parser.parse_args(sys.argv[1:2])

        try:
            self.start_command = start_commands.get(args.command)
        except NoSuchCommandException:
            print("Unrecognized command")
            parser.print_help()
            exit(1)

    def _extract_parser_arg_kwargs(self, arg: StartCommandArgument):
        kwargs = {'help': arg.description}

        if arg.action:
            kwargs['action'] = arg.action

        # If action 'store_true', the rest of the args should not be added
        if kwargs.get('action') == 'store_true':
            return kwargs

        kwargs['type'] = str

        if not arg.required:
            kwargs['nargs'] = '?'

        if arg.default:
            kwargs['default'] = arg.default

        if arg.choices:
            kwargs['choices'] = arg.choices

        return kwargs

    def _parse_arguments(self):
        """Parse and validate arguments."""
        parser = argparse.ArgumentParser(description=self.start_command.description)

        names = []
        for arg in self.start_command.args:
            kwargs = self._extract_parser_arg_kwargs(arg)

            if arg.named:
                parser.add_argument(f'--{arg.name}', **kwargs)
            else:
                parser.add_argument(arg.name, **kwargs)
            names.append(arg.name)

        parser.add_argument('--user', help='User id that starts the command', required=False)
        names.append('user')
        input_args = parser.parse_args(sys.argv[2:])

        input_values = {}
        for name in names:
            if getattr(input_args, name):
                input_values[name] = getattr(input_args, name)

        return input_values

    def get_message(self):
        """Returns message dict with the arguments under the header key."""
        return {'header': self._parse_arguments()}
