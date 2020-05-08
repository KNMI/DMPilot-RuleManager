import logging
import signal
import json
import jsonschema

from functools import partial, wraps
from core.rule import Rule
from core.exceptions import ExitPipelineException
from configuration import config
from schema import JSON_RULE_SCHEMA


class RuleManager():

    """
    Class RuleManager
    Main manager class for rule functions
    """

    def __init__(self):

        # Initialize logger
        self.logger = logging.getLogger("RuleManager")
        self.logger.debug("Initializing the Rule Manager.")

        self.rules = None
        self.conditions = None
        self.rule_sequence = None

    def __signal_handler(self, signum, frame):
        """Raise an exception when a signal SIGALRM was received."""

        raise TimeoutError("Metric calculation has timed out.")

    def load_rules(self, rule_module, condition_module, rule_sequence_file):
        """Loads the rules.

        Parameters
        ----------
        rule_module : module
            A module containing all the rule handling functions.
        condition_module : module
            A module containing all the condition functions.
        rule_sequence_file : `str`
            The path for a JSON file defining in which order to run the rules,
            and the name of the rule map file.
        """

        # Load the Python scripted rules and conditions
        self.rules = rule_module
        self.conditions = condition_module

        rule_desc = None    # Rule configuration
        rule_seq = None     # Rule order

        # Load the rule sequence JSON file
        try:
            with open(rule_sequence_file) as order_file:
                rule_seq = json.load(order_file)
        except IOError:
            raise IOError("The rule sequence file %s could not be found." % rule_sequence_file)

        # Load the rule configuration JSON file
        rule_map_file = rule_seq["rule_map"]
        try:
            with open(rule_map_file) as rule_file:
                rule_desc = json.load(rule_file)
        except IOError:
            raise IOError("The rulemap %s could not be found." % rule_map_file)

        # Confirm rule map against the schema
        try:
            jsonschema.validate(rule_desc, JSON_RULE_SCHEMA)
        except jsonschema.exceptions.ValidationError:
            raise ValueError("The rulemap %s does not validate against the schema." % rule_map_file)

        # Get the rule from the map
        try:
            self.rule_sequence = [rule_desc[rule_name] for rule_name in rule_seq["sequence"]]
            for rule_name in rule_seq["sequence"]:
                rule_desc[rule_name]["rule_name"] = rule_name
        except KeyError as exception:
            raise ValueError("The rule %s could not be found in the configured rule map %s." %
                             (exception.args[0], rule_map_file))

        # Check if the rules are valid
        self.__check_rule_sequence(self.rule_sequence)

    def __check_rule_sequence(self, sequence):
        """Check validity of the configured rule sequence."""

        # Check each rule that it exists & is a callable Python function
        for item in sequence:

            # Check if the rule exists
            try:
                rule, timeout = self.get_rule(item)
            except AttributeError:
                raise NotImplementedError(
                    "Python rule for configured sequence item %s does not exist." %
                    item)

            # The rule must be callable (function) too
            if not callable(rule.call):
                raise ValueError(
                    "Python rule for configured sequence item %s is not callable." %
                    item)

    def bind_options(self, definitions, item):
        """Bind options to a function call."""

        def invert(f):
            @wraps(f)
            def g(*args, **kwargs):
                return not f(*args, **kwargs)
            return g

        # Invert the boolean result from the condition
        if (definitions == self.conditions) and item["function_name"].startswith("!"):
            return partial(invert(getattr(definitions, item["function_name"][1:])),
                           item["options"])
        else:
            return partial(getattr(definitions, item["function_name"]), item["options"])

    def get_rule(self, rule):
        """Return specific rule from name and its execution timeout."""

        # Bind the rule options to the function call
        # There may be multiple conditions defined per rule
        rule_obj = Rule(
            self.bind_options(self.rules, rule),
            map(lambda x: self.bind_options(self.conditions, x), rule["conditions"]),
            name=rule["rule_name"]
        )

        # Get timeout from rule-specific config or from default value
        timeout = rule.get("timeout") or config["DEFAULT_RULE_TIMEOUT"]

        return (rule_obj, timeout)

    def sequence(self, items):
        """
        Def RuleManager.sequence
        Runs the sequence of rules on the given file list.

        Parameters
        ----------
        items
            An iterable collection of objects that can be processed by the loaded rules.
        """

        total = len(items)

        # Items can be SDSFiles or metadata (XML) files
        for i, item in enumerate(items):

            self.logger.info("%s - Item %d of %d" % (str(item), i+1, total))

            # Get the sequence of rules to be applied
            for rule, timeout in map(self.get_rule, self.rule_sequence):

                # Set a signal
                signal.signal(signal.SIGALRM, self.__signal_handler)
                signal.alarm(timeout)

                # Rule options are bound to the call
                try:
                    self.logger.debug("%s - %s - Executing" % (str(item), rule.name))
                    rule.apply(item)
                    self.logger.info("%s - %s - Success"
                                     % (str(item), rule.name))

                # A rule called for the pipeline to be exited for this file
                except ExitPipelineException as e:
                    if e.is_error:
                        # The exception came from an error
                        self.logger.error("%s - %s - Failure: %s"
                                          % (str(item), rule.name, e.message))
                    else:
                        # A rule executed successfully and called for an exit
                        self.logger.info("%s - %s - Success"
                                         % (str(item), rule.name))

                    self.logger.info("%s - Exit" % (str(item)))

                    # The "finally" block WILL be executed even after breaking
                    break

                # The rule was timed out
                except TimeoutError:
                    self.logger.warning("%s - %s - Timeout"
                                        % (str(item), rule.name))

                # Condition assertion errors
                except AssertionError as e:
                    self.logger.info(
                        "%s - %s - Did not pass condition '%s'."
                        % (str(item), rule.name, e))

                # Other exceptions
                except Exception as e:
                    self.logger.error("%s - %s - Failure: %s"
                                      % (str(item), rule.name, e), exc_info=False)

                # Disable the alarm
                finally:
                    signal.alarm(0)
