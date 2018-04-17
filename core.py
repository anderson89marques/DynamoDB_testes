import decimal
import json

import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError


# Using downloadable version
DYNAMODB = boto3.resource('dynamodb', region_name='sa-east-1', endpoint_url="http://localhost:8000")

class DecimalEnconder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return self(DecimalEnconder, self).default(o)

def create():
    table_name = 'Movies'
    key_schema = [
        {
            'AttributeName': 'year',
            'KeyType': 'HASH'  #Partition key
        },
        {
            'AttributeName': 'title',
            'KeyType': 'RANGE'  #Sort key
        }
    ]
    attrs_definition = [
        {
            'AttributeName': 'year',
            'AttributeType': 'N'
        },
        {
            'AttributeName': 'title',
            'AttributeType': 'S'
        },
    ]
    prov_throughput = {
        'ReadCapacityUnits': 10,
        'WriteCapacityUnits': 10
    }
    table = create_table(table_name, key_schema, attrs_definition, prov_throughput)
    print(f"Table status: {table.table_status}")

def create_table(TableName, KeySchema, AttributeDefinitions, ProvisionedThroughput):
    return DYNAMODB.create_table(
        TableName=TableName,
        KeySchema=KeySchema,
        AttributeDefinitions=AttributeDefinitions,
        ProvisionedThroughput=ProvisionedThroughput
    )

def insert_movies():
    table = DYNAMODB.Table('Movies')
    with open('moviedata.json') as json_file:
        movies = json.load(json_file, parse_float=decimal.Decimal)
        for movie in movies:
            year = int(movie['year'])
            title = movie['title']
            info = movie['info']

            print(f"Adding moving: {year}{title}")

            table.put_item(
                Item={
                    'year': year,
                    'title': title,
                    'info': info,
                }
            )

def put_item():
    title = "The Big new Movie"
    year = 2015
    table = DYNAMODB.Table('Movies')
    response = table.put_item(
        Item={
            'year': year,
            'title': title,
            'info': {
                'plot': 'Nothing happens at all.',
                'rating': decimal.Decimal(0)
            }
        }
    )
    print("PutItem Succeeded:")
    print(json.dumps(response, indent=4, cls=DecimalEnconder))

def get_item():
    title = "The Big new Movie"
    year = 2015
    table = DYNAMODB.Table('Movies')
    try:
        response = table.get_item(
            Key={
                'year': year,
                'title': title
            }
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        item = response.get('Item')
        print("GetItem suceeded:")
        print(json.dumps(item, indent=4, cls=DecimalEnconder))

def update_item():
    title = "The Big new Movie"
    year = 2015
    table = DYNAMODB.Table('Movies')
    response = table.update_item(
        Key={
            'year': year,
            'title': title,
        },
        UpdateExpression="set info.rating = :r, info.plot = :p, info.actors = :a",
        ExpressionAttributeValues={
            ':r': decimal.Decimal(5.5),
            ':p': "Everething happens all at once.",
            ':a': ["Larry", "Moe", "Curly"]
        },
        ReturnValues="UPDATED_NEW" # Só retornará os atributos atualizados
    )
    print("UpdateItem Succeeded:")
    print(json.dumps(response, indent=4, cls=DecimalEnconder))

def increment_atomic_count():
    title = "The Big new Movie"
    year = 2015
    table = DYNAMODB.Table('Movies')
    response = table.update_item(
        Key={
            'year': year,
            'title': title,
        },
        UpdateExpression="set info.rating = info.rating + :val",
        ExpressionAttributeValues={
            ':val': decimal.Decimal(1),
        },
        ReturnValues="UPDATED_NEW" # Só retornará os atributos atualizados
    )
    print("UpdateItem Succeeded:")
    print(json.dumps(response, indent=4, cls=DecimalEnconder))

def update_item_conditionally():
    title = "The Big new Movie"
    year = 2015
    table = DYNAMODB.Table('Movies')
    try:
        response = table.update_item(
            Key={
                'year': year,
                'title': title
            },
            UpdateExpression="remove info.actors[0]",
            ConditionExpression="size(info.actors) >= :num",
            ExpressionAttributeValues={
                ':num': 3
            },
            ReturnValues="UPDATED_NEW"
        )
    except ClientError as e:
        if e.response['Error']['Code'] == "ConditionalCheckFailedException":
            print(e.response['Error']['Message'])
        else:
            raise
    else:        
        print("UpdateItemConditionally Succeeded:")
        print(json.dumps(response, indent=4, cls=DecimalEnconder))

def delete_item():
    title = "The Big new Movie"
    year = 2015
    table = DYNAMODB.Table('Movies')
    try:
        response = table.delete_item(
            Key={
                'year': year,
                'title': title
            },
            ConditionExpression="info.rating <= :val",
            ExpressionAttributeValues={
                ':val': decimal.Decimal(5)            
            },
        )
    except ClientError as e:
        if e.response['Error']['Code'] == "ConditionalCheckFailedException":
            print(e.response['Error']['Message'])
        else:
            raise
    else:        
        print("DeleteItem Succeeded:")
        print(json.dumps(response, indent=4, cls=DecimalEnconder))

def query_by_year():
    table = DYNAMODB.Table('Movies')
    print("Movies from 1985")
    response = table.query(
        KeyConditionExpression=Key('year').eq(1985)
    )

    for movie in response['Items']:
        print(movie['year'], ":", movie['title'])

def query_by_year_with_title_beggining_between_A_L():
    table = DYNAMODB.Table('Movies')
    print("Movies from 1992 - title A-L, with genres and lead actors.")
    response = table.query(
        ProjectionExpression="#yr, title, info.genres, info.actors[0]",
        ExpressionAttributeNames={"#yr": "year"},
        KeyConditionExpression=Key('year').eq(1992) & Key('title').between('A', 'L')
    )

    for movie in response['Items']:
        print(json.dumps(movie, cls=DecimalEnconder))

def delete_table():
    table = DYNAMODB.Table('Movies')
    table.delete()

def main():
    #create()
    #insert_movies()
    #put_item()
    #get_item()
    #update_item()
    #increment_atomic_count()
    #update_item_conditionally()
    #delete_item()
    #query_by_year()
    #query_by_year_with_title_beggining_between_A_L()
    #delete_table()

if __name__ == '__main__':
    main()