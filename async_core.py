import asyncio
import aioboto3
from boto3.dynamodb.conditions import Key


async def create(dynamo_resource):
    table_name = 'test_table'
    key_schema = [
        {
            'AttributeName': 'pk',
            'KeyType': 'HASH'  #Partition key
        },
        {
            'AttributeName': 'col1',
            'KeyType': 'RANGE'  #Sort key
        }
    ]
    attrs_definition = [
        {
            'AttributeName': 'pk',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'col1',
            'AttributeType': 'S'
        },
    ]
    prov_throughput = {
        'ReadCapacityUnits': 10,
        'WriteCapacityUnits': 10
    }
    await dynamo_resource.create_table(TableName=table_name, KeySchema=key_schema, 
                                               AttributeDefinitions=attrs_definition, 
                                               ProvisionedThroughput=prov_throughput)

async def hello():
    async with aioboto3.resource('dynamodb', region_name='sa-east-1', 
                                 endpoint_url="http://localhost:8000") as dynamo_resource:
        #table  = await create(dynamo_resource)                                 
        table = dynamo_resource.Table('test_table')
        await table.put_item(
            Item={'pk': 'test1', 'col1': 'some_data'}
        )
        result = await table.query(
            KeyConditionExpression=Key('pk').eq('test1')
        )
        print(result['Items'])
        #await delete_table(dynamo_resource)

async def delete_table(dynamo_resource):
    table = dynamo_resource.Table('test_table')
    await table.delete()

def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(hello())

if __name__ == '__main__':
    main()