#!/bin/python3.7
import logging
import threading
import time
from logging import handlers
import socket
import boto3
import botocore

import lib
import psutil
import os


def debug_print(message):
    #print(message)
    pass

debug_print ("starting")
def check_proc(processName):
    for proc in psutil.process_iter():
        try:
          if processName.lower() in proc.name().lower():
            if proc.pid  != os.getpid():
                return True
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
          pass
    return False;

if (check_proc("poll_n_write")):
    debug_print("there can only be one!")
    exit(0)

#Config setup for AWS and syslog
config = lib.ConfigHelper()
logging_server = config.rl_syslog_host
logging_port = config.rl_syslog_port
REGION_NAME = config.rl_aws_region
qname = config.rl_aws_queue
poll_interval       = 5
poll_duration       = 10
MaxNumberOfMessages = 10
VisibilityTimeout   = 3600
#SleepTime = 100
SleepTime = 10


syslog_logger     = logging.getLogger('SYSLOG')
syslog_formatter  = logging.Formatter("%(asctime)s PRISMA_CLOUD %(message)s", "%b %d %H:%M:%S")
syslog_logger.setLevel(logging.DEBUG)
#debug_print("Connnecting to",logging_server)
syslog_handler = logging.handlers.SysLogHandler(
    address = (logging_server, logging_port),
    facility = logging.handlers.SysLogHandler.LOG_LOCAL3,
    socktype=socket.SOCK_STREAM)
#debug_print("Connected to",logging_server)
syslog_handler.setFormatter(syslog_formatter)
syslog_logger.addHandler(syslog_handler)


#-------------------------------------------------------------------------------
# Get the specified resource object
def get_resource (service_name,
                    REGION_NAME):
    # Build the Config dict
#    config = Config(proxies=proxies_dict, connect_timeout=300) if proxies_dict else None

    # Construct the resource object
    sqs = boto3.resource(service_name,
                         region_name = REGION_NAME)



    return sqs


#-------------------------------------------------------------------------------
# Connect to the queue
def get_queue(rsrc_sqs, qname):
    # Get the queue by name
    debug_print("looking up {}".format(qname))
    q = rsrc_sqs.get_queue_by_name(QueueName = qname
                                   #QueueOwnerAWSAccountId
    )
    return q


class SyslogThread(threading.Thread):
    def __init__(self, name, refreshTime, queue, poll_interval,poll_duration, MaxNumberOfMessages,VisibilityTimeout):
        threading.Thread.__init__(self, name=name)
        self.event = threading.Event()
        self.refreshTime = refreshTime
        self.queue = queue
        self.poll_interval = poll_interval
        self.poll_duration = poll_duration
        self.MaxNumberOfMessages = MaxNumberOfMessages
        self.VisibilityTimeout = VisibilityTimeout
        debug_print("init thread")

    def run(self):
        debug_print('thread run')
        firstTime = True
        counter = 0
        while not self.event.is_set():
            counter = counter + 1

            if firstTime:

                self.threadTask()

            self.event.wait(self.refreshTime)

        syslog_handler.close()


    def threadTask(self):
        debug_print('in thread')
        _poll_n_write(self.queue,
                        self.poll_interval,
                        self.poll_duration,
                        self.MaxNumberOfMessages,
                        self.VisibilityTimeout
                      )

#-------------------------------------------------------------------------------
# Poll indefinitely
# Write the received messages to socket logger
def _poll_n_write (q,
                    poll_interval,
                    poll_duration,
                    MaxNumberOfMessages,
                    VisibilityTimeout):
    debug_print('Start polling message from SQS')
    msgs = q.receive_messages (MaxNumberOfMessages = MaxNumberOfMessages,
                                   WaitTimeSeconds     = poll_duration,
                                   VisibilityTimeout   = VisibilityTimeout)

    debug_print ("Got message from SQS")
        # Write the received messages to socket logger
        # Delete the message from queue after writing it.
    if len(msgs):
        for m in msgs:
            try:
                syslog_str = m.body
                syslog_logger.info(syslog_str)
                time.sleep(5)

            except:
                debug_print ("Error in processing message:%s %(m.body).strip()")
            try:
                debug_print('Delete SQS message')
                m.delete()
            except:
                debug_print ('Error in deleting message in SQS:%s' %(m.body).strip())

        debug_print('completed message processing. Now poll again')




#put logic after this to kill thread with the same name
# get reference to thread and kill using thread.event.set()
def stopThread():
    debug_print("Stop thread")
    for threadObj in threading.enumerate():
        threadName = threadObj.name
        debug_print('Thread Name Enum %s' %threadName)
        if threadName == "redlockthread":
            debug_print ("Stopping RedLock Thread for %s" % threadName)
            threadObj.event.set()
            threadObj.join()

    is_alive = True
    while is_alive:
        syslog_found = False
        for threadObj in threading.enumerate():
            threadName = threadObj.name

            debug_print('Thread Name Check for %s' %threadName)
            qradar_found = threadName
            debug_print('qradar_found %s' % qradar_found)
            if threadName == "redlockthread":
                debug_print('Got redlock thread %s' % qradar_found)
                syslog_found = True
                debug_print('qradar_found set to %s' % qradar_found)

        if not syslog_found:
            debug_print('making is_alive to False and qradar_found %s' % qradar_found)
            is_alive = False
        debug_print("in loop")
            



#-------------------------------------------------------------------------------
# Get the queue
# Start polling the queue
# Write the received messages to Socket logger
def poll_queue_n_write( REGION_NAME,
                        qname,
                        poll_interval,
                        poll_duration,
                        MaxNumberOfMessages,
                        VisibilityTimeout):
    # Get the SQS resource; Get queue by queue name
    # Handle possible exceptions

#    cipher = AESCipher()

    # Read from data file
#    stored_data = read_data_store()

    debug_print('read data done in poll_queue_n_write method')

    # Encrypt keys if available else read from data file
#    encrypted_access_key = cipher.encrypt(ACCESS_KEY) if ACCESS_KEY else stored_data.get('access_key', '')
#    encrypted_secret_key = cipher.encrypt(SECRET_KEY) if SECRET_KEY else stored_data.get('secret_key', '')

    # Decrypt values
#    ACCESS_KEY = cipher.decrypt(encrypted_access_key) if encrypted_access_key else encrypted_access_key
#    SECRET_KEY = cipher.decrypt(encrypted_secret_key) if encrypted_secret_key else encrypted_secret_key

    # Load other values from data store if not provided

#    REGION_NAME = REGION_NAME or stored_data.get('region_name', '')
#    qname = qname or stored_data.get('qname', '')
#    poll_interval = poll_interval or stored_data.get('poll_interval', '')
#    poll_duration = poll_duration or stored_data.get('poll_duration', '')
#    MaxNumberOfMessages = MaxNumberOfMessages or stored_data.get('max_number_of_messages', '')
#    VisibilityTimeout = VisibilityTimeout or stored_data.get('visibility_timeout', '')

#    proxies_dict = proxies_dict or stored_data.get('proxies_dict', {})

    debug_print('Calling SQS to get data in poll_queue_n_write method')
    try:
        rsrc_sqs = get_resource('sqs', REGION_NAME)
        debug_print('rsrc_sqs {}'.format( rsrc_sqs))
        q = get_queue(rsrc_sqs, qname)
        debug_print('got queue {}'.format( q))

    except botocore.exceptions.ClientError as e:
        debug_print(e)
        raise (e)
        err = lib.Error_SQS_Log_Downloader(operation_name = e.operation_name,
                    error_type = 'ClientError',
                    error_code = e.response['Error']['Code'])
        return err

    except botocore.exceptions.EndpointConnectionError as e:
        debug_print(e)
        raise (e)

        err = lib.Error_SQS_Log_Downloader(error_type = 'EndpointConnectionError',
                    error_msg_pre = e.message)
        return err

    except ValueError as e:
        debug_print(e)
        raise (e)

        if 'Invalid endpoint' in e.message:
            err = lib.Error_SQS_Log_Downloader(error_type = 'EndpointConnectionError',
                        error_msg_pre = e.message)
        else:
            err = lib.Error_SQS_Log_Downloader()
        return err

    except Exception as e:
        debug_print(e)
        raise (e)

        err = lib.Error_SQS_Log_Downloader()

        try     : err.error_code = e.response['Error']['Code']
        except  : pass

        try     : err.error_type = type(e).split('.')[-1]
        except  : pass

        try     : err.operation_name = e.operation_name
        except  : pass

        try     : err.error_msg_actual = e.message
        except  : pass

        err.compute_err_msg()
        return err


    stopThread()

    debug_print("Finished stopping thread")

    refreshTime=1

    debug_print("starting thread")
    thread = SyslogThread(
		"redlockthread",
		refreshTime=refreshTime,
		queue=q,
		poll_interval=poll_interval,
		poll_duration=poll_duration,
		MaxNumberOfMessages=MaxNumberOfMessages,
		VisibilityTimeout=VisibilityTimeout
	     )
    thread.start()
    return True

#-------------------------------------------------------------------------------
# Test the SQS connection
def testSQSConnection(  REGION_NAME,
                        qname,
                        poll_interval,
                        poll_duration,
                        MaxNumberOfMessages,
                        VisibilityTimeout,
                        proxies_dict):
    # Get the SQS resource; Get queue by queue name
    # Handle possible exceptions

    debug_print('testSQSConnection method called')
    try:
        rsrc_sqs = get_resource('sqs', REGION_NAME, proxies_dict)
        q = get_queue(rsrc_sqs, qname)

    except botocore.exceptions.ClientError as e:
        err = lib.Error_SQS_Log_Downloader(operation_name = e.operation_name,
                    error_type = 'ClientError',
                    error_code = e.response['Error']['Code'])
        return err

    except botocore.exceptions.EndpointConnectionError as e:
        err = lib.Error_SQS_Log_Downloader(error_type = 'EndpointConnectionError',
                    error_msg_pre = e.message)
        return err

    except ValueError as e:
        if 'Invalid endpoint' in e.message:
            err = lib.Error_SQS_Log_Downloader(error_type = 'EndpointConnectionError',
                        error_msg_pre = e.message)
        else:
            err = lib.Error_SQS_Log_Downloader()
        return err

    except Exception as e:
        err = lib.Error_SQS_Log_Downloader()

        try     : err.error_code = e.response['Error']['Code']
        except  : pass

        try     : err.error_type = type(e).split('.')[-1]
        except  : pass

        try     : err.operation_name = e.operation_name
        except  : pass

        try     : err.error_msg_actual = e.message
        except  : pass

        err.compute_err_msg()
        return err




    return True

debug_print(__name__)
if __name__ == "__main__":
    debug_print("polling")
    res = poll_queue_n_write(
        REGION_NAME,
        qname,
        poll_interval,
        poll_duration,
        MaxNumberOfMessages,
        VisibilityTimeout)

    if res == True:
        debug_print("Successfully connnected to SQS queue.")
    else:
        debug_print("ERROR")
        debug_print(res.__dict__)
        raise Exception('Error')
    while True:
        try:
            debug_print("sleeping {}".format( SleepTime))
            time.sleep(SleepTime)
            debug_print("waking up")
        except KeyboardInterrupt as e:
            stopThread()
            break;
        except Exception as e :
            debug_print(e)
            raise e
        #else:
        #    debug_print("Error: %s" % res.error_msg)

#    debug_print res.error_msg
debug_print("end of main")
