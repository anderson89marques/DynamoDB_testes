Dynamo db exemplos
==================

para a versão async funcionar as dependências precisam está exatamente nas versões abaixo.
- aioboto3 = 3.0.0
- aiobotocore = 0.6.0
- aiohttp = 3.0.9
- boto3 = 1.6.0
- botocore = 1.8.21

tem um issue aberta no aiobotocore e por isso as versões mais novas do aiohttp não funcionam: [issue aiobotocore](https://github.com/aio-libs/aiobotocore/issues/551).