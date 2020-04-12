# Object-detection-AutoScaling-AWS-Raspberrypi

Push surveillance_edge.py on Raspberrypi (Records and uploads the video on S3 | Updates the SQS queue in AWS | Performs Edge Computing) <br/>
Push masterController.py on 1 ec2 instance (Checks the size of the queue and performs AutoScaling)</br>
Push ec2ChildScript.py on all the remaining ec2 instances (Downlaods the video from S3 | Executes darknet command on the video and uploads on result on s3)
