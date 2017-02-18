import boto3


class SimpleDB(object):
    """Example class that will do operations on simpledb"""

    def __init__(self, *args, **kwargs):
        region = self.kwargs.get('region_name', 'us-east-1')
        self.conn = boto3.client('sdb', region_name=region)

    def insert(
        self, item_name, attributes,
        region='us-east-1', domain_name='defaultdomain',
        conn=None
    ):
        """
        Insert a single record to simpledb domain.
        PARAMS:
        @item_name: unique string for this record.
        @attributes = [
            {'Name': 'duration', 'Value': str(duration), 'Replace': True},
            {'Name': 'date', 'Value': str(date), 'Replace': True},
        ]
        """
        if not conn:
            conn = self.conn
        try:
            status = conn.put_attributes(
                DomainName=domain_name,
                ItemName=item_name,
                Attributes=attributes
            )
        except:
            status = False
        try:
            if status['ResponseMetadata']['HTTPStatusCode'] == 200:
                return True
            else:
                print(status['ResponseMetadata']['HTTPStatusCode'])
                return False
        except:
            return False

    def simpledb_batch_insert(
        self, items, region='us-east-1',
        domain_name='default_domain',
        conn=None
    ):
        """
        Batch insert multiple items in one request.
        Max allowed items = 25
        """
        if not conn:
            conn = self.conn
        try:
            status = conn.batch_put_attributes(
                DomainName=domain_name,
                Items=items
            )
        except:
            return False
        try:
            if status['ResponseMetadata']['HTTPStatusCode'] == 200:
                return True
            else:
                return False
        except:
            return False

    def format_items(self, items):
        """
        Format items to a suitable format
        """
        formatted_items = [i['Attributes'][0]['Value'] for i in items]
        return formatted_items

    def query(
        self, attribute_name, value, region='us-east-1',
        domain_name='defaultdomain', conn=None,
        next_token=None, total_items=None
    ):
        """Recursively get all items returned by query
        by going through all the pages
        """
        if not conn:
            conn = self.conn
        query = 'select * from `{0}` where `{1}`="{2}"'.format(
            domain_name, attribute_name, value
        )
        if next_token:
            response = conn.select(
                SelectExpression=query,
                NextToken=next_token
            )
        else:
            response = conn.select(SelectExpression=query)
        if not response.get('Items'):
            return []
        next_token = response.get("NextToken")
        formatted_items = self.format_items(response['Items'])
        if total_items:
            total_items.update(formatted_items)
        else:
            total_items = formatted_items
        if next_token:
            return_items = self.query(
                attribute_name=attribute_name, value=value,
                domain_name=domain_name, conn=conn,
                next_token=next_token,
                total_items=total_items
            )
            return return_items
        else:
            return total_items

    def create_domain(self, domain_name, conn=None):
        """
        Create a domain
        """
        if not conn:
            conn = self.conn
        response = conn.create_domain(
            DomainName=domain_name
        )
        print(response)
        return True

    def delete_domain(self, domain_name, conn=None):
        """
        delete a domain
        """
        if not conn:
            conn = self.conn
        response = conn.delete_domain(
            DomainName=domain_name
        )
        print(response)
        return True
