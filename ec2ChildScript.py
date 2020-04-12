# code to be run on child instances

from __future__ import print_function
import sys
import boto3
import subprocess
import re
import time

video_name = sys.argv[1]
input_bucket = 'project-input'
output_bucket = 'project-output'

time_1 = time.time()

# Initialize S3
s3 = boto3.client('s3')

# download the video file from s3 in the darknet folder
s3.download_file(input_bucket, video_name, 'darknet/' +video_name)


# DATA PREPROCESSING: Remove empty lines, Remove ASCII Escape sequences, Start from FPS results
def process_output(out):
    #removes ascii escape sequence
    ansi_escape = re.compile(r'''
        \x1B  # ESC
        (?:   # 7-bit C1 Fe (except CSI)
            [@-Z\\-_]
        |     # or [ for CSI, followed by a control sequence
            \[
            [0-?]*  # Parameter bytes
            [ -/]*  # Intermediate bytes
            [@-~]   # Final byte
        )
    ''', re.VERBOSE)
    result = ansi_escape.sub('', out)
    #removes extra new lines
    out_new = re.sub(r'\n+', '\n', result).strip()
    out_final = out_new
    #starts from output
    start_pos = out_new.find('FPS')
    if start_pos >= 0:
        out_final = out_new[out_new.find('FPS'):]

    result_found = False
    unique_obj = set()
    # if floating point exception:
    for item in out_final.split("\n"):
        if "%" in item:
            result_found = True
            obj_arr = item.strip().split(':')[0]
            obj_name = "".join(obj_arr)
            if obj_name not in unique_obj:
                unique_obj.add(obj_name)

    with open("output.txt", "w") as txt_file:
        txt_file.write(",".join(list(unique_obj)))

    if not result_found:
        with open("output.txt", "w") as txt_file:
            txt_file.write("No Object Detected")

    # upload the output text file in the bucket

    s3.upload_file('output.txt', output_bucket, video_name)


try:

    command = 'cd darknet && ./darknet detector demo cfg/coco.data cfg/yolov3-tiny.cfg yolov3-tiny.weights ' + video_name               #for ec2

    output = subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT)

    out = output.decode("utf-8")

    process_output(out)

except Exception as e:
    # if floating point exception | write it into the output file
    #deals with partial results
    out = e.output.decode("utf-8")
    process_output(out)