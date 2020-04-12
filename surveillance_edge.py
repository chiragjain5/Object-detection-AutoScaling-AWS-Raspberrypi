#!/usr/bin/python

'''
SETUP:

    -   -->     GND     -->     PIN6
    +   -->     5V      -->     PIN4
    S   -->     GPIO18  -->     PIN12

'''

import RPi.GPIO as GPIO
import subprocess
import time
import sys
import take_video
import time
import threading
import boto3
import time
import uuid
import os
from datetime import datetime



sensor = 12

GPIO.setwarnings(False)
GPIO.setmode(GPIO.BOARD)
GPIO.setup(sensor, GPIO.IN)

on = 0
off = 0
flag = 0
idx = 0

def is_process_running():
    proc1 = subprocess.Popen(['ps', 'cax'], stdout=subprocess.PIPE)
    proc2 = subprocess.Popen(['grep', 'darknet'], stdin=proc1.stdout,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    proc1.stdout.close() # Allow proc1 to receive a SIGPIPE if proc2 exits.
    out, err = proc2.communicate()

    return True if out else False

def upload(idx):
    video_path = 'video-' + idx + '.h264'

    print("Video uploading start for video-%s" % idx)
    #time.sleep(10)
    input_bucket = "project-input"

    # Initialize SQS and S3
    s3 = boto3.client('s3')
    sqs = boto3.resource('sqs',region_name='us-east-1')

    # upload the video to s3 (Uploading first to S3 and then to SQS to avoid issues)
    s3.upload_file('videos/' + video_path, input_bucket, video_path)

    # raspberry pi edge computing if free
    if not is_process_running():
        print('Assigning to raspberry %s' % video_path)
        run_command = "python raspberryEdgeComputing " + video_path
        try:
            output = subprocess.check_output(run_command, shell=True, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            print('Exception in raspberry, trying in cloud %s' % video_path)
            print(e.output)
            # generate random_id to put in queue.send_message()
            rand_id = str(uuid.uuid1())
            # append the queue in sqs
            queue = sqs.get_queue_by_name(QueueName='process_video.fifo')
            response = queue.send_message(MessageBody=video_path, MessageGroupId='input',
                                          MessageDeduplicationId=rand_id)

    else:
        # generate random_id to put in queue.send_message()
        print('Assigning to Cloud %s' % video_path)
        rand_id = str(uuid.uuid1())
        # append the queue in sqs
        queue = sqs.get_queue_by_name(QueueName='process_video.fifo')
        response = queue.send_message(MessageBody=video_path, MessageGroupId='input', MessageDeduplicationId=rand_id)
        print(response)
        # return


if __name__ == '__main__':
    idx = 0
    print('Start time: {0}'.format(time.time()))
    while True:
        i = GPIO.input(sensor)
        if i == 0:
            off = time.time()
            diff = off - on
            print('time: {0} sec'.format(str(diff % 60)))
            print('')
            flag = 0
            print("No intruders")
            time.sleep(0.2)
        elif i == 1 and flag == 0:
            on = time.time()
            print('intruder')
            current_time = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            take_video.record_video('videos/video-' + current_time+ '.h264'.format(int(time.time())), 5)
            print(datetime.now()) #printing the current time
            t = threading.Thread(target=upload, args=(current_time,)).start()
            idx += 1
            if idx >= 10:
                break
            flag = 1
            time.sleep(2)

