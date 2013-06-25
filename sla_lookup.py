#splunk scripted lookup for downtime, based on splunk's example external_lookup.py
#Used by dynamic monitoring - see http://zspace.hosted.jivesoftware.com/docs/DOC-28528
import csv,sys
from utils import get_maintenance_windows, is_in_xrow
from datetime import datetime
import time
import sys, os
import logging, logging.handlers
import splunk

def setup_logging():
    #Copied from http://dev.splunk.com/view/splunk-extensions/SP-CAAAEA9 
    logger = logging.getLogger('splunk.monitis-lookup')    
    SPLUNK_HOME = os.environ['SPLUNK_HOME']
    
    LOGGING_DEFAULT_CONFIG_FILE = os.path.join(SPLUNK_HOME, 'etc', 'log.cfg')
    LOGGING_LOCAL_CONFIG_FILE = os.path.join(SPLUNK_HOME, 'etc', 'log-local.cfg')
    LOGGING_STANZA_NAME = 'python'
    LOGGING_FILE_NAME = "monitis-lookup.log"
    BASE_LOG_PATH = os.path.join('var', 'log', 'splunk')
    LOGGING_FORMAT = "%(asctime)s %(levelname)-s\t%(module)s:%(lineno)d - %(message)s"
    splunk_log_handler = logging.handlers.RotatingFileHandler(os.path.join(SPLUNK_HOME, BASE_LOG_PATH, LOGGING_FILE_NAME), mode='a') 
    splunk_log_handler.setFormatter(logging.Formatter(LOGGING_FORMAT))
    logger.addHandler(splunk_log_handler)
    splunk.setupSplunkLogger(logger, LOGGING_DEFAULT_CONFIG_FILE, LOGGING_LOCAL_CONFIG_FILE, LOGGING_STANZA_NAME)
    return logger

def lookup(test_name, time, windows, logger):
    logger.debug("Checking if {0} was under maintenance at {1}".format(test_name, datetime.fromtimestamp(time)))
    return any(map(lambda window: xrow_matches(test_name, window[0]) and window[1] <= datetime.fromtimestamp(time) <= window[2], windows))

def xrow_matches(test_name, xrow):
	parts = test_name.split("_")[0:3]
	if len(parts) != 3:
		return False
	return is_in_xrow([parts[0], parts[2], parts[1]], xrow)

def main(windows):
    logger = setup_logging()
        
    if len(sys.argv) != 4:
        logger.error("Usage: python sla_lookup.py [event time field] [monitor name field] [is during maintenance field]")
        sys.exit(0)
    
    time_field, name_field, downtime_field = sys.argv[1:4]
    reader = csv.reader(sys.stdin)
    writer = None
    header = []
    first = True
        
    try:
        
        logger.debug("Maintenance Windows: {0}".format(windows))
        
        for line in reader:
            if first:
                header = line
                if time_field not in header or name_field not in header or downtime_field not in header:
                    logger.error("event time, monitor name, and during maintenance flag need to be columns in the csv")
                    sys.exit(0)
                csv.writer(sys.stdout).writerow(header)
                writer = csv.DictWriter(sys.stdout, header)
                first = False
                logger.debug(header)
                continue

            line.extend([''] * max(len(header) - len(line), 0))
            result = { header[i] : line[i] for i in range(len(line)) }
            
            if len(result[downtime_field]):
                writer.writerow(result)

            else:
                result[downtime_field] = lookup(result[name_field], int(result[time_field]), windows, logger)
                writer.writerow(result)
            logger.debug(result)
    except:
        logger.error("Error: {0} {1}".format(sys.exc_info()[0], sys.exc_info()[1]))
        raise

if __name__ == '__main__':
	windows = get_maintenance_windows("maintenance_windows.txt")
	main(windows)