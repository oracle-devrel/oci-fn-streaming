import io
import json
import logging
import oci
import os
from base64 import b64encode, b64decode

# Retrieve the values from environment variables
tenancy_ocid = os.environ.get("OCI_TENANCY_OCID")
user_ocid = os.environ.get("OCI_USER_OCID")
fingerprint = os.environ.get("OCI_FINGERPRINT")
private_key_file_location = os.environ.get("OCI_PRIVATE_KEY_FILE")
region = os.environ.get("OCI_REGION")

# Streaming env variables
stream_ocid = os.environ.get("OCI_STREAM_OCID")
stream_endpoint = os.environ.get("OCI_STREAM_ENDPOINT")
auth_token = os.environ.get("OCI_AUTH_TOKEN")

# Set up the configuration
config = {
    "user": user_ocid,
    "key_file": private_key_file_location,
    "fingerprint": fingerprint,
    "tenancy": tenancy_ocid,
    "region": region
}

def handler(ctx, data: io.BytesIO = None):
    
    try:
        body = json.loads(data.getvalue())
        bucket_name  = body["data"]["additionalDetails"]["bucketName"]
        object_name  = body["data"]["resourceName"]
        logging.getLogger().info('Function invoked for bucket upload: ' + bucket_name)
    except (Exception, ValueError) as ex:
        logging.getLogger().info('error parsing json payload: ' + str(ex))
    

    # Get the object data from Object Storage
    obj = get_object(bucket_name, object_name)

    logging.getLogger().info('Object Content: ' + str(obj))

    logging.getLogger().info("Publishing to stream endpoint -->" + stream_endpoint)
    stream_client = oci.streaming.StreamClient(config, service_endpoint=stream_endpoint)

    # Build message list
    message_list = []
    key = "event-key-1"    
    json_data = data.getvalue().decode('utf-8')    
    encoded_key = b64encode(key.encode()).decode() 
    encoded_value = b64encode(json_data.encode()).decode()   
    message_list.append(oci.streaming.models.PutMessagesDetailsEntry(key=encoded_key, value=encoded_value))
    

    key = "data-key-1"        
    encoded_key = b64encode(key.encode()).decode() 
    encoded_value = b64encode(obj).decode()   
    message_list.append(oci.streaming.models.PutMessagesDetailsEntry(key=encoded_key, value=encoded_value))
    
   
    print("Publishing {} messages to the stream {} ".format(len(message_list), stream_ocid))
    messages = oci.streaming.models.PutMessagesDetails(messages=message_list)
    put_message_result = stream_client.put_messages(stream_ocid, messages)

    # The put_message_result can contain some useful metadata for handling failures
    for entry in put_message_result.data.entries:
        if entry.error:
            print("Error ({}) : {}".format(entry.error, entry.error_message))
        else:
            print("Published message to partition {} , offset {}".format(entry.partition, entry.offset))



    # Return a success status
    return {
        "status": 200,
        "message": "Successfully processed object update"
    }

def get_object(bucketName, objectName):
    signer = oci.auth.signers.get_resource_principals_signer()
    client = oci.object_storage.ObjectStorageClient(config={}, signer=signer)
    namespace = client.get_namespace().data
    try:
        print("Searching for bucket and object", flush=True)
        print("namespace:" + namespace, flush=True)
        object = client.get_object(namespace, bucketName, objectName)
        print("found object", flush=True)
        if object.status == 200:
            print("Success: The object " + objectName + " was retrieved with the content: ", flush=True)
        else:
            print("Failed: The object " + objectName + " could not be retrieved.")
    except Exception as e:
        print("Failed: " + str(e.message))
    return object.data.content