import boto3
import os
import json

class RunRemoteScriptException(Exception):
    pass

def run_remote_script(event, context):
    print("### Starting")
    print("### Event:")
    print("{}".format(event))

    ssm_client = boto3.client('ssm')

    try:
        s3_struct = event['Records'][0]['s3']
        bucket_name = s3_struct['bucket']['name']
        object_name = s3_struct['object']['key']
        object_etag = s3_struct['object']['eTag']
        command_line = " ".join(["/bin/bash", "download-s3-package.sh", object_name, object_etag])
        
        source_info_dict = {
            "owner": "fsulib",
            "repository": "diginole_async_ingest",
            "getOptions": "branch:main",
            "path": "RunRemoteScript/download-s3-package.sh"
        }

        send_command_params = {
            "DocumentName": "AWS-RunRemoteScript",
            "Targets": [
                {
                    "Key": "tag:Name",
                    "Values": ["isle-apache"]
                }
            ],
            "Parameters": {
                "sourceType": ["GitHub"],
                "commandLine": [command_line],
                "sourceInfo": [json.dumps(source_info_dict)]
            }
        }
        
        print("### Parameters:")
        print(send_command_params)
        
        ssm_client.send_command(**send_command_params)
        
    except RunRemoteScriptException as e:
        raise
    
    except Exception as e:
        raise RunRemoteScriptException("Something weird happened: {}".format(e))
