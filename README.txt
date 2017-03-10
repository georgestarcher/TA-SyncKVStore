## TA-SyncKVStore

Author: George Starcher (starcher)
Email: george@georgestarcher.com

## Overview

This is a rework of the modular alert made for the Splunk 2016 .conf talk on KVStore by myself and Duane Waddle.
It includes the original modular alert to send search results to a remote KVStore collection.

Now it has two modular inputs. One to pull a remote KVStore to a local KVStore.  The other pulls the remote KVStore and indexes it in JSON format.

**All materials in this project are provided under the MIT software license. See license.txt for details.**

## Dependencies

* Splunk 6.3+
* Supported on Windows, Linux, MacOS, Solaris, FreeBSD, HP-UX, AIX
* Configured KVStore Collections

## Setup

* Install to your $SPLUNK_HOME/etc/apps directory
* Restart Splunk


## Using

First you need to setup the TA by adding the appropriate Splunk User with permissions to the remote KVStore.
This is stored in the Splunk Encrypted password storage.

###Modular Alert

Devise your search that results are a table matching the format for the remote KVStore based table (collection). Schedule the search and attach the modular alert to send the results to the remote KVStore.

###Modular Input KVStore to KVStore

This expects the same collection definition on the source KVStore as the local receiving KVStore. 

###Modular Input KVStore to Index

This pulls the remote KVStore and drops the hidden fields. It exposes the _key from the KVStore as a field named key and adds the json formatted records to the deestination index.

##Update vs Replace

You have the option to replace the destination KVStore table. This means a delete all data is executed before the new table results are added.

The update option just adds the results to the table. If the _key field matches then it updates the existing records.


