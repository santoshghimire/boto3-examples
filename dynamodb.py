import boto3
import time
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr


class DynamoDB(object):
    """docstring for DynamoDB"""
    def __init__(self, arg):
        region_name = self.kwargs.get('region_name', 'us-east-1')
        self.conn = boto3.resource('dynamodb', region_name=region_name)

    def batch_write(self, table_name, items):
        """
        Batch write items to given table name
        """
        dynamodb = self.conn
        table = dynamodb.Table(table_name)
        with table.batch_writer() as batch:
            for item in items:
                batch.put_item(Item=item)
        return True

    def insert_item(self, table_name, item):
        """Insert an item to table"""
        dynamodb = self.conn
        table = dynamodb.Table(table_name)
        response = table.put_item(Item=item)
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return True
        else:
            return False

    def get_item(self, table_name, query_item):
        """
        Get an item given its key
        """
        dynamodb = self.conn
        table = dynamodb.Table(table_name)
        response = table.get_item(
            Key=query_item
        )
        item = response['Item']
        return item

    def update_item(self, table_name, key_dict, update_dict):
        """
        Update an item.
        PARAMS
        @table_name: name of the table
        @key_dict: dict containing the key name and val eg. {"uuid": item_uuid}
        @update_dict: dict containing the key name and val of
        attributes to be updated
        eg. {"attribute": "processing_status", "value": "completed"}
        """
        dynamodb = self.conn
        table = dynamodb.Table(table_name)
        update_expr = 'SET {} = :val1'.format(update_dict['attribute'])
        response = table.update_item(
            Key=key_dict,
            UpdateExpression=update_expr,
            ExpressionAttributeValues={
                ':val1': update_dict['value']
            }
        )
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return True
        else:
            return False

    def query_item(
        self, table_name, sort_key, partition_key,
        index_name=None, total_items=None, start_key=None,
        table=None
    ):
        """
        Query for an item with or without using global secondary index
        PARAMS:
        @table_name: name of the table
        @sort_key: Dict containing key and val of sort key
        e.g. {'name': 'uuid', 'value': '077f4450-96ee-4ba8-8faa-831f6350a860'}
        @partition_key: Dict containing key and val of partition key
        e.g. {'name': 'date', 'value': '2017-02-12'}
        @index_name (optional): Name of the Global Secondary Index
        """
        if not table:
            dynamodb = self.conn
            table = dynamodb.Table(table_name)
        sk = sort_key['name']
        skv = sort_key['value']
        pk = partition_key['name']
        pkv = partition_key['value']
        if not start_key:
            if index_name:
                response = table.query(
                    IndexName=index_name,
                    KeyConditionExpression=Key(sk).eq(skv) &
                    Key(pk).eq(pkv)
                )
            else:
                response = table.query(
                    KeyConditionExpression=Key(sk).eq(skv) &
                    Key(pk).eq(pkv)
                )
        else:
            if index_name:
                response = table.query(
                    IndexName=index_name,
                    KeyConditionExpression=Key(sk).eq(skv) &
                    Key(pk).eq(pkv),
                    ExclusiveStartKey=start_key
                )
            else:
                response = table.query(
                    KeyConditionExpression=Key(sk).eq(skv) &
                    Key(pk).eq(pkv),
                    ExclusiveStartKey=start_key
                )
        if not total_items:
            total_items = response['Items']
        else:
            total_items.extend(response['Items'])
        if response.get('LastEvaluatedKey'):
            start_key = response['LastEvaluatedKey']
            return_items = self.query_item(
                table_name=table_name, sort_key=sort_key,
                partition_key=partition_key, total_items=total_items,
                start_key=start_key, table=table
            )
            return return_items
        else:
            return total_items

    def scan_item(
        self, table_name, attr1, attr2,
        total_items=None, start_key=None,
        table=None
    ):
        """
        Scan for an item with two attributes

        NOTE: SCAN OPERATION SCANS THE WHOLE TABLE AND TAKES CONSIDERABLE
        AMOUNT OF TIME, CONSUMES HIGH READ THROUGHPUT.
        AVOID USING THIS AS MUCH AS YOU CAN.
        TRY CREATING INDEX AND USE QUERY IF POSSIBLE

        PARAMS:
        @table_name: name of the table
        @attr1: Dict containing key and val of first attribute
        e.g. {'name': 'uuid', 'value': '077f4450-96ee-4ba8-8faa-831f6350a860'}
        @attr2: Dict containing key and val of second attribute
        e.g. {'name': 'date', 'value': '2017-02-12'}
        """
        if not table:
            dynamodb = self.conn
            table = dynamodb.Table(table_name)
        a1 = attr1['name']
        a1v = attr1['value']
        a2 = attr2['name']
        a2v = attr2['value']
        if not start_key:
            response = table.scan_item(
                FilterExpression=Attr(a1).eq(a1v) &
                Attr(a2).eq(a2v)
            )
        else:
            response = table.scan_item(
                FilterExpression=Attr(a1).eq(a1v) &
                Attr(a2).eq(a2v),
                ExclusiveStartKey=start_key
            )
        if not total_items:
            total_items = response['Items']
        else:
            total_items.extend(response['Items'])
        if response.get('LastEvaluatedKey'):
            start_key = response['LastEvaluatedKey']
            return_items = self.query_item(
                table_name=table_name, attr1=attr1,
                attr2=attr2, total_items=total_items,
                start_key=start_key, table=table
            )
            return return_items
        else:
            return total_items

    def delete_item(self, table_name, item_key):
        """
        delete an item
        PARAMS
        @table_name: name of the table
        @item_key: dict containing key and val of sort key
        e.g. {'name': 'uuid', 'value': 'some-uuid-val'}
        """
        dynamodb = self.conn
        table = dynamodb.Table(table_name)
        response = table.delete_item(
            Key={item_key['name']: item_key['value']}
        )
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return True
        else:
            return False

    def create_table(
        self, table_name, hash_name,
        read_throughput=5, write_throughput=5
    ):
        """
        Create the DynamoDB table.
        NOTE: THIS IS A DEMO TABLE WITH ONLY A HASH KEY of type String.
        """
        dynamodb = self.conn
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': hash_name,
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': hash_name,
                    'AttributeType': 'S'
                },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': read_throughput,
                'WriteCapacityUnits': write_throughput
            }
        )
        if table:
            print("Success !")
        return table

    def delete_all_items(self, table_name, hash_name):
        """
        Delete all items in a table by recreating the table.
        """
        dynamodb = self.conn
        try:
            table = dynamodb.Table(table_name)
            table.delete()
        except:
            print(
                "Error in deletion. Table {} does not exist.".format(
                    table_name))
        # allow time for table deletion
        time.sleep(5)
        try:
            table = self.create_table(table_name, hash_name=hash_name)
        except:
            print("Error in creating table {}".format(table_name))
