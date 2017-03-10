
# encoding = utf-8
# Always put this line at the beginning of this file
import ta_synckvstore_declare 

import os
import sys

from alert_actions_base import ModularAlertBase 
import modalert_synckvstore_helper

class AlertActionWorkersynckvstore(ModularAlertBase):

    def __init__(self, ta_name, alert_name):
        super(AlertActionWorkersynckvstore, self).__init__(ta_name, alert_name)

    def validate_params(self):

        if not self.get_param("u_splunkserver"):
            self.log_error('u_splunkserver is a mandatory parameter, but its value is None.')
            return False

        if not self.get_param("u_destappname"):
            self.log_error('u_destappname is a mandatory parameter, but its value is None.')
            return False

        if not self.get_param("u_destcollection"):
            self.log_error('u_destcollection is a mandatory parameter, but its value is None.')
            return False

        if not self.get_param("u_desttableaction"):
            self.log_error('u_desttableaction is a mandatory parameter, but its value is None.')
            return False

        if not self.get_param("u_username"):
            self.log_error('u_username is a mandatory parameter, but its value is None.')
            return False
        return True

    def process_event(self, *args, **kwargs):
        status = 0
        try:

            if not self.validate_params():
                return 3 
            status = modalert_synckvstore_helper.process_event(self, *args, **kwargs)
        except (AttributeError, TypeError) as ae:
            self.log_error("Error: {}. Please double check spelling and also verify that a compatible version of Splunk_SA_CIM is installed.".format(ae.message))
            return 4
        except Exception as e:
            msg = "Unexpected error: {}."
            if e.message:
                self.log_error(msg.format(e.message))
            else:
                import traceback
                self.log_error(msg.format(traceback.format_exc()))
            return 5
        return status

if __name__ == "__main__":
    exitcode = AlertActionWorkersynckvstore("TA-SyncKVStore", "synckvstore").run(sys.argv)
    sys.exit(exitcode)
