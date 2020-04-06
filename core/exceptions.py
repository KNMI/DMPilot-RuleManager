"""Defines exceptions for the Rule Manager."""


class RuleManagerException(Exception):
    """Base class for exceptions in this module."""

    pass


class ExitPipelineException(RuleManagerException):
    """Exception raised to exit a file pipeline.

    Parameters
    ----------
    is_error : `bool`
        Whether or not this was raised by an error.
    message : `str`
        An message about the exception.

    """

    def __init__(self, is_error, message):
        self.is_error = is_error
        self.message = message
