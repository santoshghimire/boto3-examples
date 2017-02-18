import logging

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_function(event, context):
    """
    A simple lambda function handler
    """
    logger.info('Lambda function execution started.')
    # do something with the passed parameters
    date = event.get('date')
    print(date)
