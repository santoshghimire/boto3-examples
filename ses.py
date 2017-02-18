from boto3 import client


class SES(object):
    """Example class to demonstrate SES actions"""
    def __init__(self, *args, **kwargs):
        region_name = self.kwargs.get('region_name', 'us-east-1')
        self.conn = client('ses', region_name=region_name)

    def send_email(self, sender, to, subject, body):
        """
        Send email.
        Note: The emails of sender and receiver should be verified.
        PARAMS
        @sender: sender's email, string
        @to: list of receipient emails eg ['a@b.com', 'c@d.com']
        @subject: subject of the email
        @body: body of the email
        """
        try:
            response = self.conn.send_email(
                Source=sender,
                Destination={
                    'ToAddresses': to
                },
                Message={
                    'Subject': {
                        'Data': subject,
                        'Charset': 'UTF-8'
                    },
                    'Body': {
                        'Text': {
                            'Data': body,
                            'Charset': 'UTF-8'
                        }
                    }
                }
            )
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                return True
            else:
                return False
        except:
            return False
