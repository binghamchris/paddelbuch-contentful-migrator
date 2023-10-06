// Import the markdown to richtext converter tool provided by Contentful.
// This npm package needs to be packaged into a Lambda layer, which is then added to the Lambda function, in order to work.
import { richTextFromMarkdown } from '@contentful/rich-text-from-markdown';

// The main event handler which will be called by Lambda when our Lambda function is invoked.
export const handler = async function(event, context) {
  // Extract the 'markdown' field from the Lambda event and convert it to Contentful richtext format using the converter.
  const document = await richTextFromMarkdown(event['markdown']);

  // Return the converted richtext with a HTTP 200 status code.
  const response = {
    statusCode: 200,
    body: serialize(document),
  };
  return response;
};

// a simple function to convert JSON to a string.
var serialize = function(object) {
  return JSON.stringify(object, null, 2);
};