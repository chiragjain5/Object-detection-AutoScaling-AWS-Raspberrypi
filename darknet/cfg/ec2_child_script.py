from __future__ import print_function

import urllib
import os
import subprocess
import boto3


# SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# LIB_DIR = os.path.join(SCRIPT_DIR, 'lib')



def downloadFromS3(strBucket, strKey, strFile):
    s3_client = boto3.client('s3')
    s3_client.download_file(strBucket, strKey, strFile)


def uploadToS3(strFile, strBucket, strKey):
    s3_client = boto3.client('s3')
    s3_client.upload_file(strFile, strBucket, strKey, ExtraArgs={'ACL': 'public-read'})



# ./darknet detector demo  test_video.h264

try:
    # downloadFromS3(strBucket, imagePath, imageFilePath)            #video downalod
    # predictionsPath = '/tmp/predictions.png'
    command = './darknet cfg/coco.data cfg/yolov3-tiny.cfg yolov3-tiny.weights test_video.h264'
    try:
        print('Start')
        output = subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT)
        print('Finish')
            # upload predictions to bucket
        print(output)
        # predictionsFile = '{}.png'.format(os.path.splitext(imageFile)[0])
        # predictionsKey = 'predictions/{}'.format(predictionsFile)
        # uploadToS3(predictionsPath, strBucket, predictionsKey)
    except subprocess.CalledProcessError as e:
        print('Error')
        print(e.output)
except Exception as e:
    print('Error')
    print(e)
    raise e


# return 0