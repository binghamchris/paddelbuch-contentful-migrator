# Hygraph to Contentful Data Migration using AWS
This repo provides sample code for an automated migration of entries from Hygraph CMS to Contentful CMS.
For more information on this migration, please see parts 3 and 4 of the [Paddel Buch blog series](https://cloudypandas.ch/series/paddel-buch/).

## Files
* `glue_job.py` contains a sample Glue job for reformatting content entries between the two CMS' respective data structures.
* `lambda_fucntion.js` provides an example of deploying Contentful's [rich text from markdown](https://www.npmjs.com/package/@contentful/rich-text-from-markdown) NPM library as a serverless microfunction.
* `state_machine.json` shows how the execution of the Glue job can be dynamically orchestrated using AWS Parameter Store and AWS Step Functions.