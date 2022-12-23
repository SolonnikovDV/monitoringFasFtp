import PySimpleGUI as sg


def run_progress_bar(current_value: int, max_value: int, key_message: str):
    sg.one_line_progress_meter(title='Download progress',
                               current_value=current_value,
                               max_value=max_value,
                               key=f'Now downloaded >> {key_message}',
                               orientation='v')