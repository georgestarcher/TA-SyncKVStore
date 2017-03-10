
# encoding = utf-8

def process_event(helper, *args, **kwargs):
    """
    # IMPORTANT
    # Do not remove the anchor macro:start and macro:end lines.
    # These lines are used to generate sample code. If they are
    # removed, the sample code will not be updated when configurations
    # are updated.

    [sample_code_macro:start]

    # The following example gets account information
    user_account = helper.get_user_credential("<account_name>")

    # The following example gets and sets the log level
    helper.set_log_level(helper.log_level)

    # The following example gets the alert action parameters and prints them to the log
    u_splunkserver = helper.get_param("u_splunkserver")
    helper.log_info("u_splunkserver={}".format(u_splunkserver))

    u_destappname = helper.get_param("u_destappname")
    helper.log_info("u_destappname={}".format(u_destappname))

    u_destcollection = helper.get_param("u_destcollection")
    helper.log_info("u_destcollection={}".format(u_destcollection))

    u_desttableaction = helper.get_param("u_desttableaction")
    helper.log_info("u_desttableaction={}".format(u_desttableaction))

    u_username = helper.get_param("u_username")
    helper.log_info("u_username={}".format(u_username))


    # The following example adds two sample events ("hello", "world")
    # and writes them to Splunk
    # NOTE: Call helper.writeevents() only once after all events
    # have been added
    helper.addevent("hello", sourcetype="sample_sourcetype")
    helper.addevent("world", sourcetype="sample_sourcetype")
    helper.writeevents(index="summary", host="localhost", source="localhost")

    # The following example gets the events that trigger the alert
    events = helper.get_events()
    for event in events:
        helper.log_info("event={}".format(event))

    # helper.settings is a dict that includes environment configuration
    # Example usage: helper.settings["server_uri"]
    helper.log_info("server_uri={}".format(helper.settings["server_uri"]))
    [sample_code_macro:end]
    """

    try:
        import splunklib.client as splunkClient
        import datetime
        import requests
        import threading, Queue
        import json
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

        def __init__(self, splunk_server, splunk_app, splunk_collection, splunk_user, splunk_password):
            self.splunk_server = splunk_server
            self.splunk_app = splunk_app
            self.splunk_collection = splunk_collection
            self.splunk_user = splunk_user
            self.splunk_password = splunk_password
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
                headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
                splunk_url = ''.join(['https://',self.splunk_server,':8089/servicesNS/nobody/',self.splunk_app,'/storage/collections/data/',self.splunk_collection,'/','batch_save'])
                payload_length = sum(len(json.dumps(item)) for item in data)
                r = requests.post(splunk_url,auth=(self.splunk_user,self.splunk_password),verify=_splunk_server_verify,headers=headers,data=json.dumps(data))
                helper.log_info(" API POST: {}".format(r.text))
                if not r.status_code == requests.codes.ok:
                    helper.log_error("{}".format(r.text))
                self.flushQueue.task_done()

        def waitUntilDone(self):
            self.flushQueue.join()
            return
    
    helper.log_info("Alert action synckvstore started.")

    u_splunkserver = helper.get_param("u_splunkserver")
    helper.log_info("u_splunkserver={}".format(u_splunkserver))

    u_destappname = helper.get_param("u_destappname")
    helper.log_info("u_destappname={}".format(u_destappname))

    u_destcollection = helper.get_param("u_destcollection")
    helper.log_info("u_destcollection={}".format(u_destcollection))

    u_desttableaction = helper.get_param("u_desttableaction")
    helper.log_info("u_desttableaction={}".format(u_desttableaction))

    u_username = helper.get_param("u_username")
    helper.log_info("u_username={}".format(u_username))
    
    user_account = helper.get_user_credential(u_username)
    
    searchResults = helper.get_events()
    
    splunkService = splunkClient.connect(host=u_splunkserver, port=8089, username=user_account.get('username'), password=user_account.get('password'),owner='nobody',app=u_destappname)
    
    #Check if KVStore collection exists
    if u_destcollection not in splunkService.kvstore:
        helper.log_error("message=\"Collection Not Found\" dest_server={0} dest_collection={1}".format(u_splunkserver,u_destcollection))
        for collection in splunkService.kvstore:
            helper.log_error("{}".format(collection.name))
        return 1
    
    # If replace method is selected use SDK KVStore to delete the data in the collection
    if u_desttableaction == "replace":
        splunkService.kvstore[u_destcollection].data.delete()
        helper.log_info("action=deleted collection_name={0} message=\"Remote Collection Data Deleted\"".format(u_destcollection))

    # Define our threaded class for KVStore data submission        
    destKVStore = splunk_sendto_kvstore(u_splunkserver, u_destappname, u_destcollection, user_account.get('username'), user_account.get('password'))
    
    postList = []
    for entry in searchResults:
        if ((len(json.dumps(postList)) + len(json.dumps(entry))) < _max_content_bytes) and (len(postList) + 1 < _max_content_records):
            postList.append(entry)
        else:
            destKVStore.postDataToSplunk(postList)
            postList = []
            postList.append(entry)
            
        destKVStore.postDataToSplunk(postList)
        
    destKVStore.waitUntilDone()
    
    helper.log_info("Alert action synckvstore completed.")
    
    return 0
