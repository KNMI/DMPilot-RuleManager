"""Defines exceptions for the Rule Manager."""


class RuleManagerError(Exception):
    """Base class for exceptions in this module."""

    pass


class ExitPipelineException(RuleManagerError):
    """Exception raised to exit a file pipeline."""

    pass
