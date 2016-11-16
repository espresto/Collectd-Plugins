import collectd
import urllib2
import json
import itertools
import base64

SOLR_HOST = "localhost"
SOLR_PORT = "8080"
SOLR_LOGIN = ""
SOLR_PASSWORD = ""
VERBOSE_LOGGING = False
SOLR_HANDLERS = {"query": "/select", "suggest": "/suggest"}
SOLR_CACHE_TYPES = ["filterCache", "documentCache", "queryResultCache"]

SOLR_INSTANCES = {
}

def configure_callback(conf):
    """Received configuration information"""
    global SOLR_HOST, SOLR_PORT, SOLR_LOGIN, SOLR_PASSWORD, SOLR_INSTANCES, VERBOSE_LOGGING
    for node in conf.children:
        if node.key == "Login":
            SOLR_LOGIN = node.values[0]
        if node.key == "Password":
            SOLR_PASSWORD = node.values[0]
        if node.key == "Instance":
            # if the instance is named, get the first given name
            if len(node.values):
                if len(node.values) > 1:
                    collectd.info("%s: Ignoring extra instance names (%s)" % (__name__, ", ".join(node.values[1:])) )
                SOLR_INSTANCE = node.values[0]
            # else register an empty name instance
            else:
                SOLR_INSTANCE = 'default'

            for child in node.children:
                if child.key == 'Host':
                    SOLR_HOST = child.values[0]
                elif child.key == 'Port':
                    SOLR_PORT = int(child.values[0])
                elif child.key == 'Verbose':
                    VERBOSE_LOGGING = bool(child.values[0])
                else:
                    collectd.warning('solr_info plugin: Unknown config key: %s.' % node.key)

            # add this instance to the dict of instances
            SOLR_INSTANCES[SOLR_INSTANCE] = "http://" + SOLR_HOST + ":" + str(SOLR_PORT) + "/solr/" + SOLR_INSTANCE
            continue

    log_verbose('Configured with host=%s, port=%s, instance=%s' % (SOLR_HOST, SOLR_PORT, SOLR_INSTANCE))


def dispatch_value(plugin_category, value, value_name, value_type, type_instance=None):
    val = collectd.Values(plugin="solr_info")
    val.type = value_type

    val.plugin_instance = plugin_category.replace('-','_')+"-"+value_name

    if type_instance is not None:
       val.type_instance = type_instance
    else:
       val.type_instance = value_name
    val.values = [value]
    val.dispatch()


def fetch_data():
    global SOLR_INSTANCES, SOLR_HANDLERS
    data = {}

    for SOLR_INSTANCE,URL in SOLR_INSTANCES.iteritems():
        stats_url = "%s/admin/mbeans?stats=true&wt=json" % (URL)
        handle = urllib2.Request(stats_url)

        if SOLR_LOGIN and SOLR_PASSWORD:
            authheader =  "Basic %s" % base64.b64encode('%s:%s' % (SOLR_LOGIN, SOLR_PASSWORD))
            handle.add_header("Authorization", authheader)

        stats = urllib2.urlopen(handle)
        solr_data = json.load(stats)

        # Searcher information
        solr_data = solr_data["solr-mbeans"]

        # Data is return in form of [ "TYPE", { DATA }, "TYPE", ... ] so pair them up
        solr_data_iter = iter(solr_data)
        solr_data = itertools.izip(solr_data_iter, solr_data_iter)

        data[SOLR_INSTANCE] = { "handler_stats": {}, "update_stats": {} }

        for module, module_data in solr_data:
            if module == "CORE":
                data[SOLR_INSTANCE]["docs"] = module_data["searcher"]["stats"]["numDocs"]
            elif module == "CACHE":
                for type in SOLR_CACHE_TYPES:
                    if type in module_data:
                        data[SOLR_INSTANCE][type] = {}
                        data[SOLR_INSTANCE][type]["size"] = module_data[type]["stats"]["size"]
                        data[SOLR_INSTANCE][type]["hitratio"] = module_data[type]["stats"]["hitratio"]
                        data[SOLR_INSTANCE][type]["evictions"] = module_data[type]["stats"]["evictions"]
            elif module == "QUERYHANDLER":
                interesting_handlers = { endpoint: name for name, endpoint in SOLR_HANDLERS.iteritems() }
                for handler, handler_data in module_data.iteritems():
                    if handler not in interesting_handlers:
                        continue

                    handler_name = interesting_handlers[handler]
                    data[SOLR_INSTANCE]["handler_stats"][handler_name] = {}
                    data[SOLR_INSTANCE]["handler_stats"][handler_name]["requests"] = handler_data["stats"]["requests"]
                    data[SOLR_INSTANCE]["handler_stats"][handler_name]["errors"] = handler_data["stats"]["errors"]
                    data[SOLR_INSTANCE]["handler_stats"][handler_name]["timeouts"] = handler_data["stats"]["timeouts"]
                    data[SOLR_INSTANCE]["handler_stats"][handler_name]["avgTimePerRequest"] = handler_data["stats"]["avgTimePerRequest"]
                    data[SOLR_INSTANCE]["handler_stats"][handler_name]["avgRequestsPerSecond"] = handler_data["stats"]["avgRequestsPerSecond"]
                    data[SOLR_INSTANCE]["handler_stats"][handler_name]["5minRateReqsPerSecond"] = handler_data["stats"]["5minRateReqsPerSecond"]
                    data[SOLR_INSTANCE]["handler_stats"][handler_name]["15minRateReqsPerSecond"] = handler_data["stats"]["15minRateReqsPerSecond"]
                    data[SOLR_INSTANCE]["handler_stats"][handler_name]["75thPcRequestTime"] = handler_data["stats"]["75thPcRequestTime"]
                    data[SOLR_INSTANCE]["handler_stats"][handler_name]["95thPcRequestTime"] = handler_data["stats"]["95thPcRequestTime"]
                    data[SOLR_INSTANCE]["handler_stats"][handler_name]["99thPcRequestTime"] = handler_data["stats"]["99thPcRequestTime"]
                    data[SOLR_INSTANCE]["handler_stats"][handler_name]["999thPcRequestTime"] = handler_data["stats"]["999thPcRequestTime"]
            elif module == "UPDATEHANDLER":
                data[SOLR_INSTANCE]["update_stats"]["commits"] = module_data["updateHandler"]["stats"]["commits"]
                data[SOLR_INSTANCE]["update_stats"]["autocommits"] = module_data["updateHandler"]["stats"]["autocommits"]
                if module_data["updateHandler"]["stats"].has_key('soft autocommits'):
                    data[SOLR_INSTANCE]["update_stats"]["soft_autocommits"] = module_data["updateHandler"]["stats"]["soft autocommits"]
                data[SOLR_INSTANCE]["update_stats"]["optimizes"] = module_data["updateHandler"]["stats"]["optimizes"]
                data[SOLR_INSTANCE]["update_stats"]["rollbacks"] = module_data["updateHandler"]["stats"]["rollbacks"]
                data[SOLR_INSTANCE]["update_stats"]["expunges"] = module_data["updateHandler"]["stats"]["expungeDeletes"]
                data[SOLR_INSTANCE]["update_stats"]["pending_docs"] = module_data["updateHandler"]["stats"]["docsPending"]
                data[SOLR_INSTANCE]["update_stats"]["adds"] = module_data["updateHandler"]["stats"]["adds"]
                data[SOLR_INSTANCE]["update_stats"]["deletes_by_id"] = module_data["updateHandler"]["stats"]["deletesById"]
                data[SOLR_INSTANCE]["update_stats"]["deletes_by_query"] = module_data["updateHandler"]["stats"]["deletesByQuery"]
                data[SOLR_INSTANCE]["update_stats"]["errors"] = module_data["updateHandler"]["stats"]["errors"]
    return data


def read_callback():
    data = fetch_data()
    for SOLR_INSTANCE in SOLR_INSTANCES:
        dispatch_value(SOLR_INSTANCE, data[SOLR_INSTANCE]["docs"], "index", "gauge", "documents")
        for type in SOLR_CACHE_TYPES:
            if type in data[SOLR_INSTANCE]:
                dispatch_value(SOLR_INSTANCE, data[SOLR_INSTANCE][type]["size"], "cache", "gauge", type + "_size")
                dispatch_value(SOLR_INSTANCE, data[SOLR_INSTANCE][type]["hitratio"], "cache_hitratio", "gauge", type + "_hitratio")
                dispatch_value(SOLR_INSTANCE, data[SOLR_INSTANCE][type]["evictions"], "cache", "gauge", type + "_evictions")

        for handler_name, handler_data in data[SOLR_INSTANCE]["handler_stats"].iteritems():
            dispatch_value(SOLR_INSTANCE, handler_data["requests"], handler_name, "gauge", "requests")
            dispatch_value(SOLR_INSTANCE, handler_data["errors"], handler_name, "gauge", "errors")
            dispatch_value(SOLR_INSTANCE, handler_data["timeouts"], handler_name, "gauge", "timeouts")
            dispatch_value(SOLR_INSTANCE, handler_data["avgTimePerRequest"], "avgTimePerRequest", "gauge", handler_name)
            dispatch_value(SOLR_INSTANCE, handler_data["avgRequestsPerSecond"], "avgRequestsPerSecond", "gauge", handler_name)
            dispatch_value(SOLR_INSTANCE, handler_data["5minRateReqsPerSecond"], "5minRateReqsPerSecond", "gauge", handler_name)
            dispatch_value(SOLR_INSTANCE, handler_data["15minRateReqsPerSecond"], "15minRateReqsPerSecond", "gauge", handler_name)
            dispatch_value(SOLR_INSTANCE, handler_data["75thPcRequestTime"], "75thPcRequestTime", "gauge", handler_name)
            dispatch_value(SOLR_INSTANCE, handler_data["95thPcRequestTime"], "95thPcRequestTime", "gauge", handler_name)
            dispatch_value(SOLR_INSTANCE, handler_data["99thPcRequestTime"], "99thPcRequestTime", "gauge", handler_name)
            dispatch_value(SOLR_INSTANCE, handler_data["999thPcRequestTime"], "999thPcRequestTime", "gauge", handler_name)

        dispatch_value(SOLR_INSTANCE, data[SOLR_INSTANCE]["update_stats"]["commits"], "update", "gauge", "commits")
        dispatch_value(SOLR_INSTANCE, data[SOLR_INSTANCE]["update_stats"]["autocommits"], "update", "gauge", "autocommits")
        if data[SOLR_INSTANCE]["update_stats"].has_key("soft_autocommits"):
            dispatch_value(SOLR_INSTANCE, data[SOLR_INSTANCE]["update_stats"]["soft_autocommits"], "update", "gauge", "soft_autocommits")
        dispatch_value(SOLR_INSTANCE, data[SOLR_INSTANCE]["update_stats"]["optimizes"], "update", "gauge", "optimizes")
        dispatch_value(SOLR_INSTANCE, data[SOLR_INSTANCE]["update_stats"]["expunges"], "update", "gauge", "expunges")
        dispatch_value(SOLR_INSTANCE, data[SOLR_INSTANCE]["update_stats"]["rollbacks"], "update", "gauge", "rollbacks")
        dispatch_value(SOLR_INSTANCE, data[SOLR_INSTANCE]["update_stats"]["pending_docs"], "update", "gauge", "pending_docs")
        dispatch_value(SOLR_INSTANCE, data[SOLR_INSTANCE]["update_stats"]["adds"], "update", "gauge", "adds")
        dispatch_value(SOLR_INSTANCE, data[SOLR_INSTANCE]["update_stats"]["deletes_by_id"], "update", "gauge", "deletes_by_id")
        dispatch_value(SOLR_INSTANCE, data[SOLR_INSTANCE]["update_stats"]["deletes_by_query"], "update", "gauge", "deletes_by_query")
        dispatch_value(SOLR_INSTANCE, data[SOLR_INSTANCE]["update_stats"]["errors"], "update", "gauge", "errors")


def log_verbose(msg):
    if not VERBOSE_LOGGING:
        return
    collectd.info('solr_info plugin [verbose]: %s' % msg)


collectd.register_config(configure_callback)
collectd.register_read(read_callback)
