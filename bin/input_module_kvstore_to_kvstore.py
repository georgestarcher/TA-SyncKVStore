
# encoding = utf-8

import os
import sys
import time
import datetime

'''
    IMPORTANT
    Edit only the validate_input and collect_events functions.
    Do not edit any other part in this file.
    This file is generated only once when creating the modular input.
'''
'''
# For advanced users, if you want to create single instance mod input, uncomment this method.
def use_single_instance_mode():
    return True
'''

def validate_input(helper, definition):
    """Implement your own validation logic to validate the input stanza configurations"""
    # This example accesses the modular input variable
    # global_account = definition.parameters.get('global_account', None)
    pass

def collect_events(helper, ew):
    """Implement your data collection logic here

    # The following examples get the arguments of this input.
    # Note, for single instance mod input, args will be returned as a dict.
    # For multi instance mod input, args will be returned as a single value.
    opt_global_account = helper.get_arg('global_account')
    # In single instance mode, to get arguments of a particular input, use
    opt_global_account = helper.get_arg('global_account', stanza_name)

    # get input type
    helper.get_input_type()

    # The following examples get input stanzas.
    # get all detailed input stanzas
    helper.get_input_stanza()
    # get specific input stanza with stanza name
    helper.get_input_stanza(stanza_name)
    # get all stanza names
    helper.get_input_stanza_names()

    # The following examples get options from setup page configuration.
    # get the loglevel from the setup page
    loglevel = helper.get_log_level()
    # get proxy setting configuration
    proxy_settings = helper.get_proxy()
    # get account credentials as dictionary
    account = helper.get_user_credential_by_username("username")
    account = helper.get_user_credential_by_id("account id")
    # get global variable configuration
    global_userdefined_global_var = helper.get_global_setting("userdefined_global_var")

    # The following examples show usage of logging related helper functions.
    # write to the log for this modular input using configured global log level or INFO as default
    helper.log("log message")
    # write to the log using specified log level
    helper.log_debug("log message")
    helper.log_info("log message")
    helper.log_warning("log message")
    helper.log_error("log message")
    helper.log_critical("log message")
    # set the log level for this modular input
    # (log_level can be "debug", "info", "warning", "error" or "critical", case insensitive)
    helper.set_log_level(log_level)

    # The following examples send rest requests to some endpoint.
    response = helper.send_http_request(url, method, parameters=None, payload=None,
                                        headers=None, cookies=None, verify=True, cert=None,
                                        timeout=None, use_proxy=True)
    # get the response headers
    r_headers = response.headers
    # get the response body as text
    r_text = response.text
    # get response body as json. If the body text is not a json string, raise a ValueError
    r_json = response.json()
    # get response cookies
    r_cookies = response.cookies
    # get redirect history
    historical_responses = response.history
    # get response status code
    r_status = response.status_code
    # check the response status, if the status is not sucessful, raise requests.HTTPError
    response.raise_for_status()

    # The following examples show usage of check pointing related helper functions.
    # save checkpoint
    helper.save_check_point(key, state)
    # delete checkpoint
    helper.delete_check_point(key)
    # get checkpoint
    state = helper.get_check_point(key)

    # To create a splunk event
    helper.new_event(data, time=None, host=None, index=None, source=None, sourcetype=None, done=True, unbroken=True)
    """

    '''
    # The following example writes a random number as an event. (Multi Instance Mode)
    # Use this code template by default.
    import random
    data = str(random.randint(0,100))
    event = helper.new_event(source=helper.get_input_type(), index=helper.get_output_index(), sourcetype=helper.get_sourcetype(), data=data)
    ew.write_event(event)
    '''

    '''
    # The following example writes a random number as an event for each input config. (Single Instance Mode)
    # For advanced users, if you want to create single instance mod input, please use this code template.
    # Also, you need to uncomment use_single_instance_mode() above.
    import random
    input_type = helper.get_input_type()
    for stanza_name in helper.get_input_stanza_names():
        data = str(random.randint(0,100))
        event = helper.new_event(source=input_type, index=helper.get_output_index(stanza_name), sourcetype=helper.get_sourcetype(stanza_name), data=data)
        ew.write_event(event)
    '''
    
    try:
        import splunklib.client as splunkClient
        import json
        import requests
        import threading, Queue
    except Exception as err_message:
        helper.log_error("{}".format(err_message))
        return 1

    _max_content_bytes = 100000
    _max_content_records = 1000
    _number_of_threads = 5
    _splunk_server_verify = False

    # Define my own class to put data into KVStore. 
    # The Splunk Python SDK is not threaded for KVStore operations
    class splunk_sendto_kvstore:

        def __init__(self, splunk_server, splunk_app, splunk_collection, session_key):
            self.splunk_server = splunk_server
            self.splunk_app = splunk_app
            self.splunk_collection = splunk_collection
            self.session_key = session_key
            self.flushQueue = Queue.Queue(0)
            for x in range(_number_of_threads):
                t = threading.Thread(target=self.batchThread)
                t.daemon = True
                t.start()

        def postDataToSplunk(self, data):
            self.flushQueue.put(data)
        
        def batchThread(self):
            while True:
                data = self.flushQueue.get()
                headers = {'Content-type': 'application/json', 'Accept': 'text/plain', 'Authorization':'Splunk {}'.format(self.session_key)}
                splunk_url = ''.join(['https://',self.splunk_server,':8089/servicesNS/nobody/',self.splunk_app,'/storage/collections/data/',self.splunk_collection,'/','batch_save'])
                payload_length = sum(len(json.dumps(item)) for item in data)
                r = requests.post(splunk_url,verify=_splunk_server_verify,headers=headers,data=json.dumps(data))
                helper.log_info(" API POST: {}".format(r.text))
                if not r.status_code == requests.codes.ok:
                    helper.log_error("{}".format(r.text))
                self.flushQueue.task_done()

        def waitUntilDone(self):
            self.flushQueue.join()
            return
   
    helper.log_info("Modular Input pullkvtokv started.")

    u_session_key = helper.context_meta.get('session_key')
    
    u_splunkserver = helper.get_arg('u_splunkserver')
    helper.log_info("u_splunkserver={}".format(u_splunkserver))

    u_srcappname = helper.get_arg("u_srcapp")
    helper.log_info("u_destappname={}".format(u_srcappname))

    u_srccollection = helper.get_arg("u_srccollection")
    helper.log_info("u_destcollection={}".format(u_srccollection))
    
    u_destappname = helper.get_arg("u_destapp")
    helper.log_info("u_destappname={}".format(u_destappname))

    u_destcollection = helper.get_arg("u_destcollection")
    helper.log_info("u_destcollection={}".format(u_destcollection))
    
    u_desttableaction = helper.get_arg("u_desttableaction")
    helper.log_info("u_desttableaction={}".format(u_desttableaction))
    
    user_account = helper.get_arg('global_account')
    if not user_account:
        helper.log_error("No user account selected")
        return 1
    
    srcSplunkService = splunkClient.connect(host=u_splunkserver, port=8089, username=user_account.get('username'), password=user_account.get('password'),owner='nobody',app=u_srcappname)
    
    srcKVStoreTable = srcSplunkService.kvstore[u_srccollection].data.query()
    
    destSplunkService = splunkClient.connect(token=u_session_key, owner='nobody', app=u_destappname)
    
    #Check if KVStore collection exists
    if u_destcollection not in destSplunkService.kvstore:
        helper.log_error("KVStore collection {0} not on local Splunk instance".format(u_destcollection))
        return 1
   
    # If replace method is selected use SDK KVStore to delete the data in the collection
    if u_desttableaction == "replace":
        destSplunkService.kvstore[u_destcollection].data.delete()
        helper.log_info("action=deleted collection_name={0} message=\"Remote Collection Data Deleted\"".format(u_destcollection))

    # Define our threaded class for KVStore data submission        
    destKVStore = splunk_sendto_kvstore('localhost', u_destappname, u_destcollection, u_session_key)
    
    postList = []
    for entry in srcKVStoreTable:
        if ((len(json.dumps(postList)) + len(json.dumps(entry))) < _max_content_bytes) and (len(postList) + 1 < _max_content_records):
            postList.append(entry)
        else:
            destKVStore.postDataToSplunk(postList)
            postList = []
            postList.append(entry)
            
        destKVStore.postDataToSplunk(postList)
        
    destKVStore.waitUntilDone()
    
    helper.log_info("Modular Input pullkvtokv completed.")


