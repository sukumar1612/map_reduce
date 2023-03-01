import os

from client.services.router import BluePrint, EventTypes

commands_handler = BluePrint()


@commands_handler.add_route(event="exit", event_type=EventTypes.COMMAND)
def exit_interactive_shell():
    """exit the interactive shell"""
    exit()


@commands_handler.add_route(event="clear", event_type=EventTypes.COMMAND)
def clear_screen():
    """clear screen"""
    if os.name == "posix":
        os.system("clear")
    else:
        os.system("cls")
