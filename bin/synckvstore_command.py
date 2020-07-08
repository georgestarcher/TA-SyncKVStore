import os
import sys
import ta_synckvstore_declare
from splunklib.searchcommands import dispatch, ReportingCommand, Configuration, Option, validators
from splunk_aoblib.setup_util import Setup_Util
import threading, six.moves.queue
import splunklib.client as splunkClient
import json
import requests

@Configuration(requires_preop=False)
class SyncKVStoreCommand(ReportingCommand):
    """ Sync search results to a remote kvstore, based on the synckvstore alert by George Starcher 

    ##Syntax

    .. code-block::
        synckvstore splunkserver=<remote_splunk_servername> destappname=<app_on_remote_instance> destcollection=<kvstore_on_remote_instance> desttableaction=<update_or_replace, defaults to update> username=<stored_username> 

    ##Description

    This command is the same as the synckvstore alert action 
    provided with the remote splunk servername (splunkserver), destination application (destappname) 
    destination kvstore name (destcollection), action on destination can be update or replace (desttableaction) 
    and finally the username stored in the TA-SyncKVStore app.

    ##Example

    This example sync's the results of makeresults to a local splunkserver , destination app of search and destination collection of test \
    in word_field.

    .. code-block::
       | makeresults | synckvstore splunkserver=localhost destappname=search destcollection=test username=remote_admin

    This example sync's the results of makeresults to a local splunkserver , destination app of search and destination collection of test with the action of replace \
    in word_field.

    .. code-block::
       | makeresults | synckvstore splunkserver=localhost destappname=search destcollection=test username=remote_admin desttableaction=replace

    """
    # even though map is not implemented the code keeps looking for it!
    def prepare(self):
        pass

    def splunk_sendto_kvstore_func(self, splunkserver, destappname, destcollection, data, username, password):
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        splunk_url = ''.join(['https://', splunkserver,':8089/servicesNS/nobody/', destappname,'/storage/collections/data/',destcollection,'/','batch_save'])
        r = requests.post(splunk_url,auth=(username,password),verify=False,headers=headers,data=json.dumps(data), timeout=60)
        if not r.status_code == requests.codes.ok:
            self.logger.error("{}".format(r.text))

    splunkserver = Option(
        doc='''
        **Syntax:** **splunkserver=***<remote_splunk_servername>*
        **Description:** Destination Splunk Server Name or IP''',
        require=True, validate=validators.Fieldname())

    destappname = Option(
        doc='''
        **Syntax:** **destappname=***<app_on_remote_instance>*
        **Description:** Destination App containing the KV Store Collection''',
        require=True, validate=validators.Fieldname())

    destcollection = Option(
        doc='''
        **Syntax:** **destcollection=***<kvstore_on_remote_instance>*
        **Description:** Destination Collection''',
        require=True, validate=validators.Fieldname())

    username = Option(
        doc='''
        **Syntax:** **username=***<stored_username>*
        **Description:** Username of the stored service account for the remote Splunk Server''',
        require=True, validate=validators.Fieldname())

    desttableaction = Option(
        doc='''
        **Syntax:** **desttableaction=***<update_or_replace, defaults to update>*
        **Description:** Choose to force destination table full replacement or update table''',
        require=False, validate=validators.Fieldname())

    #reduce function to work with the events returned by the search
    def reduce(self, records):
        _max_content_bytes = 100000
        _max_content_records = 1000

        self.logger.debug('synckvstore: %s', self)  # logs command line

        self.logger.info("synckvstore command started.")
        splunkserver = self.splunkserver
        self.logger.debug("splunkserver={}".format(splunkserver))

        destappname = self.destappname
        self.logger.debug("destappname={}".format(destappname))

        destcollection = self.destcollection
        self.logger.debug("destcollection={}".format(destcollection))

        if self.desttableaction:
            desttableaction = self.desttableaction
            self.logger.debug("desttableaction={}".format(desttableaction))
        else:
            desttableaction = "update"
            self.logger.debug("desttableaction=update (default)")

        username = self.username
        self.logger.debug("username={}".format(username))

        setup_util = Setup_Util(self.metadata.searchinfo.splunkd_uri, self.metadata.searchinfo.session_key, self.logger)
        user_account = setup_util.get_credential_by_username(username)

        if not user_account:
            self.logger.error("unable to find user account")
            return

        splunkService = splunkClient.connect(host=splunkserver, port=8089, username=user_account.get('username'), password=user_account.get('password'),owner='nobody',app=destappname)

        #Check if KVStore collection exists
        if destcollection not in splunkService.kvstore:
            self.logger.error("message=\"Collection Not Found\" dest_server={0} dest_collection={1}".format(splunkserver,destcollection))
            for collection in splunkService.kvstore:
                self.logger.error("{}".format(collection.name))
            return 1

        # If replace method is selected use SDK KVStore to delete the data in the collection
        if desttableaction == "replace":
            splunkService.kvstore[destcollection].data.delete()
            self.logger.info("action=deleted collection_name={0} message=\"Remote Collection Data Deleted\"".format(destcollection))

        # any attempts to use the threaded class splunk_sendto_kvstore resulted in the network connection hanging
        # if executed in an independent python script it worked fine so not using the threaded class...
        postList = []
        for entry in records:
            if ((len(json.dumps(postList)) + len(json.dumps(entry))) < _max_content_bytes) and (len(postList) + 1 < _max_content_records):
                postList.append(entry)
            else:
                destKVStore.postDataToSplunk(postList)
                self.splunk_sendto_kvstore_func(splunkserver, destappname, destcollection, postList, user_account.get('username'), user_account.get('password'))
                postList = []
                postList.append(entry)
            yield entry

        self.logger.error("post list: %s" % (postList))
        self.splunk_sendto_kvstore_func(splunkserver, destappname, destcollection, postList, user_account.get('username'), user_account.get('password'))
        self.logger.info("synckvstore command completed.")

dispatch(SyncKVStoreCommand, sys.argv, sys.stdin, sys.stdout, __name__)
