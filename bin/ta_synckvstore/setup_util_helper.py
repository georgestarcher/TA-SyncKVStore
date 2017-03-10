from importlib import import_module
import traceback
from pkgutil import find_loader
import re


def get_setup_util(ta_name, splunk_uri, session_key, logger):
    lib_dir = re.sub("[^\w]+", "_", ta_name.lower())
    loader = find_loader(lib_dir + "_setup_util")
    if not loader:
        logger.debug('module="%s" doesn\'t exists, no global setting available',
                    lib_dir + "_setup_util")
        return None

    try:
        setup_util_module = import_module(lib_dir + "_setup_util")
    except ImportError:
        logger.error('Did not import module: "%s", reason="%s"',
                    lib_dir + "_setup_util", traceback.format_exc())
        return None

    return setup_util_module.Setup_Util(splunk_uri, session_key,
                                        logger)
