import json
import boto3
import time

def lambda_handler(event, context):
    region = 'sa-east-1'
    ec2 = boto3.resource('ec2', region_name=region)
    ssm = boto3.client('ssm')
    ids = ['instance-id']
    ec2.instances.filter(InstanceIds=ids).start()
    time.sleep(30)
    instance_status = boto3.client('ec2', region_name=region).describe_instances(InstanceIds=ids)['Reservations'][0]['Instances'][0]['State']['Name']
    while instance_status != 'running':
        instance_status = boto3.client('ec2', region_name=region).describe_instances(InstanceIds=ids)['Reservations'][0]['Instances'][0]['State']['Name']
        time.sleep(10)
    time.sleep(5)
    try:    
        response = ssm.send_command( InstanceIds=ids, DocumentName='AWS-RunShellScript', Comment='Feed JC', Parameters={ "commands":[ "python3 /home/ubuntu/Feed/Product_Feed.py","python3 /home/ubuntu/Feed/FB_Feed.py","python3 /home/ubuntu/Feed/GoogleMerchant_Feed.py"]  } )
        print("Command id: " + response['Command']['CommandId'])
        status = ssm.list_commands(CommandId=response['Command']['CommandId'],InstanceId=ids[0])['Commands'][0]['Status']
        while status != 'Success':
            if status == 'Failed':
                print("Command Failed")
                break
            time.sleep(5)
            status = ssm.list_commands(CommandId=response['Command']['CommandId'],InstanceId=ids[0])['Commands'][0]['Status']
        ec2.instances.filter(InstanceIds=ids).stop()
    except Exception as ex:
        print(ex)
        ec2.instances.filter(InstanceIds=ids).stop()
    return {
        "statusCode": 200,
        "body": json.dumps('Hello from Lambda!')
    }
