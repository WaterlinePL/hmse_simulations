# Decorator for checking metadata in function

from ...hmse_projects.project_metadata import ProjectMetadata


def hmse_task(func):

    def checking_wrapper(*args, **kwargs):
        total_args = list(args) + list(kwargs.values())
        if not any(map(lambda arg: isinstance(arg, ProjectMetadata), total_args)):
            raise RuntimeError("HMSE task requires ProjectMetadata as an argument")
        return func(*args, **kwargs)

    return checking_wrapper
