import boto3
import paramiko
import time
import threading
import multiprocessing

#configure ~/.aws/credentials file

aws_key_file = 'ssh_key.pem'

queue_url='https://sqs.us-east-1.amazonaws.com/187063523558/process_video.fifo'
username = 'ubuntu'
buffer_size = 0 #we experimented with buffer size but didn't notice any significant difference in latency for entire task
max_instances = 20
start_time = time.time()
end_time = time.time()

# initialize ec2
ec2 = boto3.resource('ec2', region_name='us-east-1')
ec2_client = boto3.client('ec2', region_name='us-east-1')

# Initialize SQS
sqs = boto3.resource('sqs', region_name='us-east-1')

sqs_client = boto3.client('sqs',region_name='us-east-1')


# Initialize the SQS queue to be used
queue = sqs.get_queue_by_name(QueueName='process_video.fifo')


#gloabal concurrency arrays
processing = []
stopping = []

#add monitor instance to processing array
monitor_instances = ec2.instances.filter(Filters=[{'Name': 'tag:Name', 'Values' :['Monitor Instance']}])
for monitor in monitor_instances:
    processing.append(monitor.public_ip_address)

def get_queue_length():
    response = sqs_client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=[
            'ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'])

    # taking the count of new instances required in case of a sudden burst in request
    if response:
        print('Approximate Messages: {0}, Approximate Messages Not Visible: {1}, Total Queue Leangth: {2}'.format(int(response['Attributes']['ApproximateNumberOfMessages']), int(response['Attributes']['ApproximateNumberOfMessagesNotVisible']), int(response['Attributes']['ApproximateNumberOfMessages']) + int(response['Attributes']['ApproximateNumberOfMessagesNotVisible'])))

        return int(response['Attributes']['ApproximateNumberOfMessages']) + int(response['Attributes']['ApproximateNumberOfMessagesNotVisible'])

def get_free_instance():
    running_instances1 = ec2.instances.filter(Filters=[{
        'Name': 'instance-state-name',
        'Values': ['running']}])

    for running_instance in running_instances1:
        if running_instance.public_ip_address not in processing and running_instance.public_ip_address not in stopping:
            return running_instance

    #all running instances are processing
    return None

def get_state_instances_count(states):
    return len(list(ec2.instances.filter(Filters=[{
        'Name': 'instance-state-name',
        'Values': states}])))

def refresh_stopping():
    global stopping
    stopped_instances = ec2.instances.filter(  # filter out master instance from here
        Filters=[{'Name': 'instance-state-name', 'Values': ['stopping', 'stopped']}])
    for instance in stopped_instances:
        if instance.public_ip_address in stopping:
            stopping.remove(instance.public_ip_address)


#handles maximum instances
def launch_new_instances(count = 1):
    if count <= 0:
        return
    print('launching {0} instances'.format(count))
    stopped_instances = ec2.instances.filter(  # filter out master instance from here
        Filters=[{'Name': 'instance-state-name', 'Values': ['stopped']}])

    total_running_instances = get_state_instances_count(['running', 'pending'])

    for instance in stopped_instances:
        if total_running_instances == max_instances:
            print('Cant launch instance capacity full')
            break
        count -=1
        ec2_client.start_instances(InstanceIds=[instance.instance_id])
        total_running_instances += 1
        if count == 0:
            break

#handles buffer limitation
def stop_idle_instances(count = 1):
    if count <= 0:
        return
    running_instances = ec2.instances.filter(  # filter out master instance from here
        Filters=[{'Name': 'instance-state-name', 'Values': ['running', 'pending']}])
    total_running_instances = len(list(running_instances))

    if total_running_instances == buffer_size:
        print('Not stopping instance because running instance = buffersize')
        return

    for instance in running_instances:
        if instance.public_ip_address not in processing:
            print('Stopping instance ip: {0}'.format(instance.public_ip_address))
            keyword = {'InstanceIds': [instance.instance_id]}
            ec2_client.stop_instances(InstanceIds=[instance.instance_id])
            stopping.append(instance.public_ip_address)
            #p = multiprocessing.Process(target=ec2_client.stop_instances, kwargs=keyword)
            #p.start()  # run in seprate process
            total_running_instances -= 1
            count -= 1
            if count == 0:
                break

def auto_scale():
    # check queue length, running_instances, increment_instances based on queue length
    refresh_stopping()
    queue_length = get_queue_length()
    running_instances_len = get_state_instances_count(['pending', 'running'])
    processing_instances = len(processing)
    availiable_instances = running_instances_len - processing_instances
    # number of new instances i need to start is queue_length - running_instances_len
    if availiable_instances - buffer_size < queue_length:
        # launch queue_length - availiable_instances with + buffer
        launch_new_instances(queue_length - availiable_instances + buffer_size)
    elif availiable_instances - buffer_size > queue_length:
        #stop idle instances == availiable_instances - queue_length - buffer
        stop_idle_instances(availiable_instances - queue_length - buffer_size)


# Thread function to ssh into instance and call the python script
def ssh_run_py(resp_body, instance):
    global processing
    global end_time

    # Initialize a client to use the AWS key
    key = paramiko.RSAKey.from_private_key_file(aws_key_file)
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    client_connected = False
    while not client_connected:
        try:
            client.connect(hostname=instance.public_ip_address, username=username, pkey=key)
            client_connected = True
        except Exception as e:
            client_connected = False

    stdin, stdout, stderr = client.exec_command(
     "export DISPLAY=:1 && python3 ec2ChildScript.py %s" % resp_body
    )

    stdin.flush()

    exit_status = stdout.channel.recv_exit_status()  # Blocking call

    if exit_status == 0:
        print("Process Executed")
    else:
        print("Error", exit_status)
        print('Out', stdout.read())
        print('Error', stderr.read())

    end_time = max(end_time, time.time())
    print('Max end time till now: {0}'.format(end_time))

    #remove it from processing array after getting the output resposnce
    processing.remove(instance.public_ip_address)
    client.close()
    auto_scale()

while True:
        # First get a message from SQS
        resp = sqs_client.receive_message(QueueUrl=queue_url)
        # if we recieve any message then proceed
        if resp and resp.get('Messages'):
            #firstly we need to scale our system whenever we recieve message based on size of queue
            auto_scale()

            respBody = resp['Messages'][0]['Body']
            respHandle = resp['Messages'][0]['ReceiptHandle']


            # if queue empty stop the unused instaces by calling another thread  | code left
            print("Message retrieved from queue:", respBody)

            # Delete the message from the queue if it has been successfully processed

            if respBody not in ['', ' ', None, 'None', 'null']:
                # call start instance thread to add one extra instance for buffer
                #find running instance not in processing list
                instance_to_use = get_free_instance()
                while not instance_to_use:
                    #no instance availiable, may have to scale
                    auto_scale()
                    instance_to_use = get_free_instance()
                    time.sleep(3) #try again after 3 seconds

                print("Video Chunk %s assigned to %s" % (respBody, instance_to_use.public_ip_address))
                processing.append(instance_to_use.public_ip_address)

                #delete message after scaling, sometimes approximate message takes time to update
                sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=respHandle)

                #creates a worker thread to run asynchronously
                t = threading.Thread(target=ssh_run_py, args=(respBody, instance_to_use))
                t.start()
        else:
            #print('No message in queue. Poll again after sometime')
            #need to descale if approximate number changes
            auto_scale()
            time.sleep(3)



