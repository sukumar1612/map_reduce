import os

from client.services.router import BluePrint

commands_handler = BluePrint()


@commands_handler.add_terminal_commands(event="exit")
def exit_interactive_shell():
    exit()


@commands_handler.add_terminal_commands(event="clear")
def clear_screen():
    if os.name == "posix":
        os.system("clear")
    else:
        os.system("cls")
