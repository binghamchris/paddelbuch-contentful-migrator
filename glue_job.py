import sys
import requests
from graphqlclient import GraphQLClient
import json
import requests
from contentful_management import Client
import contentful
import copy
from time import sleep
from datetime import datetime
import logging
from awsglue.utils import getResolvedOptions
import boto3
import random

###########################################
### Functions
###########################################
# Function to retrieve the value of a specified parameter from SSM Parameter Store.
def get_ssm_param(ssm_param_name):
    logger.info(f"Querying SSM parameter {ssm_param_name}")
    session = boto3.session.Session()
    client = session.client(service_name="ssm")
    param_response= client.get_parameter(
        Name = ssm_param_name,
        WithDecryption = True
    )
    param_value = param_response['Parameter']['Value']
    logger.debug(f"--- SSM parameter value: {param_value}")
    return param_value

# Function to call the richtext converter Lambda function to convert Hygraph markdown into Contentful richtext.
# A Lambda function is used as Contentful provide this conversion tool only for Node.js, which can't be used in Glue jobs.
def convert_markdown(markdown):
    logger.info(f"Converting markdown to richtext using Lambda function")
    session = boto3.session.Session()
    lambda_client = session.client('lambda',region_name=aws_region)
    response = lambda_client.invoke(
        FunctionName='richtext_converter',
        InvocationType='RequestResponse',
        Payload=json.dumps({"markdown": f"{markdown}"})
    )
    payload = json.loads(response['Payload'].read())
    logger.debug(f"--- payload {payload}")
    return payload['body']

###########################################
### Get Glue arguments
###########################################
args = getResolvedOptions(
    sys.argv,
    [
        "hygraph_key_ssm_param_name",               # The name of the parameter in SSM Parameter Store containing the API key for Hygraph.
        "hygraph_api_ssm_param_name",               # The name of the parameter in SSM Parameter Store containing the project-specific Hygraph content API URL.
        "contentful_mgmt_token_ssm_param_name",     # The name of the parameter in SSM Parameter Store containing the token for the Contentful management API.
        "contentful_delivery_token_ssm_param_name", # The name of the parameter in SSM Parameter Store containing the token for the Contentful content delivery API.
        "contentful_env_name_ssm_param_name",       # The name of the parameter in SSM Parameter Store containing the environment name to use in Contentful.
        "contentful_model_name",                    # The name of the content model (Contentful) / schema (Hygraph) to migrate.
        "locales_ssm_param_name",                   # The name of the parameter in SSM Parameter Store containing the JSON list of locales to migrate.
        "log_level"                                 # The level of logging to perform, e.g. "INFO", "DEBUG", etc.
    ]
)

###########################################
### Setup logger
###########################################
logger = logging.getLogger()
logger.setLevel(f"{args['log_level']}")

###########################################
### Get AWS region
###########################################
# Retrieve the instance identity document from the AWS EC2 metadata API and extract the region name from it.
logger.info("Getting current AWS region")
instance_doc = requests.get("http://169.254.169.254/latest/dynamic/instance-identity/document").json()
aws_region = instance_doc.get('region')

###########################################
### Create random wait time
###########################################
# Create a random decimal to use as a millisecond wait time between API requests.
# This randomises the API query frequency across concurrent runs of the Glue job, to minimise the chance of API rate limits being exceeded and requests being throttled.
logger.info("Creating a random decimal")
random_wait = round(random.uniform(0.025, 0.075), 3)

###########################################
### Get SSM parameter values
###########################################
logger.info(f"Getting SSM parameter values")
hygraph_key = get_ssm_param(args["hygraph_key_ssm_param_name"])
hygraph_api = get_ssm_param(args["hygraph_api_ssm_param_name"])
contentful_mgmt_token = get_ssm_param(args["contentful_mgmt_token_ssm_param_name"])
contentful_delivery_token = get_ssm_param(args["contentful_delivery_token_ssm_param_name"])
contentful_env_name = get_ssm_param(args["contentful_env_name_ssm_param_name"])
contentful_model_name = args["contentful_model_name"]
locales = json.loads(get_ssm_param(args["locales_ssm_param_name"]))['locales']
hygraph_query_fields = get_ssm_param(f"hygraph_query_{contentful_model_name}")
model_transforms = json.loads(get_ssm_param(f"transforms_{contentful_model_name}"))
logger.info(f"Migrating Content Model: {contentful_model_name}")

###########################################
### Setup Hygraph client
###########################################
logger.info(f"Setup GraphQL client for Hygraph")
graphql_client = GraphQLClient(hygraph_api)
graphql_client.inject_token(f"Bearer {hygraph_key}")

###########################################
### Setup Contentful clients
###########################################
logger.info(f"Setup Contentful management client")
mgmt_client = Client(contentful_mgmt_token)
spaces = mgmt_client.spaces().all()
space = mgmt_client.spaces().find(spaces[0].id)
environment = space.environments().find(contentful_env_name)

logger.info(f"Setup Contentful content delivery client")
content_client = contentful.Client(
  space.id, 
  contentful_delivery_token,
  environment=environment.id
)

###########################################
### Query Hygraph
###########################################
logger.info(f"Query Hygraph for the data to be migrated, and restructure the response to fit the Contentful entry JSON structure.")

# Declare a dictionary to fill with the Hygraph content.
hygraph = {}
# Transforming the data later on is a little easier if we separate the locales now, so query each locale separately.
for locale in locales:
    # Use our randomised decimal to introduce a randomised wait between API requests.
    sleep(random_wait)
    # Build out GraphQL query using the data retrieved from job arguments and SSM Parameter Store parameters.
    # All content models have fewer than 1000 entries at this time, so we don't need to handle results pagination here.
    query =f"""query sourceQuery {{
      {contentful_model_name}s(locales: {locale}, first: 1000) {{
        {hygraph_query_fields}
      }}
    }}
    """
    logger.debug(f"Hygraph query = {query}")
    # Extract the entries for the content model from the result
    query_result = json.loads(graphql_client.execute(query))['data'][f"{contentful_model_name}s"]

    # Add the entries to our dictionary, reorganising them to be place the record name (slug) before the locale code, instead of the Hygraph default.
    # This will make processing them to produce the necessary Contentful JSON structure easier later.
    if hygraph == {}:
        for record in query_result:
            hygraph[record['slug']] = {}
    for record in query_result:
        hygraph[record['slug']][locale] = record

###########################################
### Remap fields
###########################################
# This is where we handle schema evolution by changing the names of fields based on a JSON key-value pair list of old and new field names.
logger.info(f"Remap fields in the Hygraph query result")
# Extract the list of fields to be remapped to new names from the model transforms JSON.
remap_fields = model_transforms['remap_fields']
# Make a independent copy of the original Hygraph results dictionary.
# This is needed to ensure that we don't change the original Hygraph results dictionary whilst iterating through it.
hygraph_remapped = copy.deepcopy(hygraph)
# Iterate through the original Hygraph results dictionary.
for record in hygraph:
    for locale in hygraph[record]:
        for field in hygraph[record][locale]:
            if field in remap_fields:
                # For each field which needs to be remapped, renaming it in the copy of the original Hygraph results dictionary.
                logger.debug(f"--- remapping {field} to {remap_fields[field]}")
                hygraph_remapped[record][locale][remap_fields[field]] = hygraph_remapped[record][locale].pop(field)

###########################################
### Migrate data
###########################################
logger.info(f"Migration started at {datetime.now()}")

# Extract the lists of fields requiring special attention from the model transforms JSON.
# Markdown fields need to be converted to Contentful's richtext format.
markdown_fields = model_transforms['markdown_fields']
# Reference fields need to be mapped to the correct Contentful entry IDs.
reference_fields = model_transforms['reference_fields']
# Location fields need to be reformatted into Contentful's location field format.
location_fields = model_transforms['location_fields']

# Iterate through the remapped Hygraph results dictionary.
for record in hygraph_remapped:
    # Use our randomised decimal to introduce a randomised wait between API requests.
    sleep(random_wait)
    logger.info(f"--- processing slug: {record}")
    # Create a started dictionary to hold the Contentful entry content as we build it.
    entry_content = {
        'content_type_id': f'{contentful_model_name}',
        'fields': {}
    }
    for locale in hygraph_remapped[record]:
        for field in hygraph_remapped[record][locale]:
            logger.debug(f"--- processing field: {field} {locale}")
            if field not in entry_content['fields']:
                # If the field hasn't been added to the entry content dictionary, add it.
                # This test is needed as when there are multiple locales, we must add the field only for the first locale processed, to avoid overwriting previously processed entry data.
                entry_content['fields'][field] = {}
            if field in markdown_fields:
                # For each Hygraph markdown field, convert it to Contentful richtext.
                logger.debug(f"--- processing markdown field: {field}")
                if hygraph_remapped[record][locale][field]:
                    # Only convert fields that contain content.
                    logger.debug(f"--- markdown: {hygraph_remapped[record][locale][field]['markdown']}")
                    # Call the conversion function to get the converted content.
                    converted_richtext = convert_markdown(hygraph_remapped[record][locale][field]['markdown'])
                    logger.debug(f"--- converted_richtext: {converted_richtext}")
                    entry_content['fields'][field][locale] = json.loads(converted_richtext)
            elif field in reference_fields:
                # For each reference field, map it to the correct Contentful entry ID.
                logger.debug(f"--- processing reference field: {field}")
                if isinstance(hygraph_remapped[record][locale][field], list):
                    # Check if the reference field is a list, indicating multiple references are permitted.
                    entry_content['fields'][field][locale] = []
                    for link in hygraph_remapped[record][locale][field]:
                        # Process each reference in the list.
                        # Use the Contentful content delivery API to find the correct entry's ID by searching based on entry type and slug.
                        content_entry = content_client.entries({'content_type': field, f'fields.slug[match]':link['slug']})
                        # Build a temporary dictionary containing a single Contentful reference.
                        # This avoids us having the track our position in the reference list.
                        temp_dict = {}
                        temp_dict['sys'] = {}
                        temp_dict['sys']['type'] = 'Link'
                        temp_dict['sys']['linkType'] = 'Entry'
                        temp_dict['sys']['id'] = content_entry[0].id
                        # Add the temporary dictionary to the reference list.
                        entry_content['fields'][field][locale].append(temp_dict)
                else:
                    # Else if the reference field only permits a single entry, process that single entry.
                    entry_content['fields'][field][locale] = {}
                    # Use the Contentful content delivery API to find the correct entry's ID by searching based on entry type and slug.
                    content_entry = content_client.entries({'content_type': field, f'fields.slug[match]':hygraph_remapped[record][locale][field]['slug']})
                    # Add the Contentful reference to the entry.
                    entry_content['fields'][field][locale] = {}
                    entry_content['fields'][field][locale]['sys'] = {}
                    entry_content['fields'][field][locale]['sys']['type'] = 'Link'
                    entry_content['fields'][field][locale]['sys']['linkType'] = 'Entry'
                    entry_content['fields'][field][locale]['sys']['id'] = content_entry[0].id
            elif field in location_fields:
                # For each location field, reformat it to match Contentful's location field format.
                logger.debug(f"--- processing location field: {field}")
                entry_content['fields'][field][locale] = {}
                entry_content['fields'][field][locale]['lat'] = hygraph_remapped[record][locale][field]['latitude']
                entry_content['fields'][field][locale]['lon'] = hygraph_remapped[record][locale][field]['longitude']
            else:
                # And for any other type of field, add its value to the entry content.
                entry_content['fields'][field][locale] = hygraph_remapped[record][locale][field]
                
    logger.debug(f"--- entry_content: {json.dumps(entry_content)}")
    # Create and publish a new entry in Contentful using the Contentful management API.
    new_entry = environment.entries().create(
        None,
        entry_content
    )
    new_entry.save()
    new_entry.publish()

logger.info(f"Migration ended at {datetime.now()}")