"""
This module configures the main/root logger for the rule Manager.

Because several modules are implemented as a _fake singleton_, this module has
to act in the same way and be imported before the others.

Example
-------

```
import logging
import core.logger
...
logger = logging.getLogger("RuleManager")
logger.info("Running SDS Manager.")
```
"""

import os
import logging
from configuration import config


def ini_logger():
    """ Initialize logger """

    # Try to parse config value to logging level, if not default is INFO
    level = getattr(logging, config["LOGGING"]["LEVEL"].upper(), 20)

    # Try to get file name from config, if not use stream (stdout)
    filename = config["LOGGING"].get("FILENAME")
    if filename is not None:
        filename = os.path.expandvars(os.path.expanduser(filename))

    logging.basicConfig(
        format="%(asctime)s - %(module)s - %(levelname)s - %(message)s",
        filename=filename
    )
    logger = logging.getLogger("RuleManager")
    logger.setLevel(level)

    logger.debug("Initialized logger.")


# Initialize main logger
ini_logger()
