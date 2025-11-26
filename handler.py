import json
import boto3
import os

BUCKET_NAME = os.environ.get('BUCKET_NAME')
TABLE_NAME = os.environ.get('TABLE_NAME')
# TOPIC_ARN no es necesario leerlo de env si lo vas a poner fijo en el código, 
# pero lo dejamos para no romper tu estructura.

def leerMetada(event, context):
    try:
        # Entrada (json) desde evento S3
        record = event['Records'][0]
        archivo_id = record['s3']['object']['key']
        
        # Validación defensiva por si el archivo está en la raíz sin carpetas
        tenant_id = archivo_id.split('/')[1] if '/' in archivo_id else 'root'
        
        archivo_last_modified = record['eventTime']
        archivo_size = record['s3']['object']['size']
        bucket_name = record['s3']['bucket']['name']
        
        archivo = {
            'tenant_id': tenant_id,
            'archivo_id': archivo_id,
            'archivo_datos': {
                'last_modified': archivo_last_modified,
                'size': archivo_size,
                'bucket_name': bucket_name
            }    
        }
        
        # Publicar en SNS (ARN Hardcoded como solicitaste)
        sns_client = boto3.client('sns')
        response_sns = sns_client.publish(
            TopicArn = 'arn:aws:sns:us-east-1:738683684819:TemaNuevoArchivo',
            Subject = 'Nuevo Archivo',
            Message = json.dumps(archivo),
            MessageAttributes = {
                'tenant_id': {'DataType': 'String', 'StringValue': tenant_id }
            }
        )    
        
        # CORRECCIÓN: Convertir la respuesta de SNS a JSON String para evitar error de datetime
        return {
            'statusCode': 200,
            'body': json.dumps(response_sns, default=str)
        }
    except Exception as e:
        print(e)
        return {'statusCode': 500, 'body': str(e)}

def ponerArchivoDynamo(event, context):
    # El mensaje de SNS viene dentro de 'Sns' -> 'Message' como string
    message = event['Records'][0]['Sns']['Message']
    archivo = json.loads(message)
    
    dynamodb = boto3.resource('dynamodb')
    tabla_archivos = dynamodb.Table(TABLE_NAME)
    
    response_dynamo = tabla_archivos.put_item(Item=archivo)

    return {
        'statusCode': 200,
        'body': json.dumps("Guardado en DynamoDB")
    }

def subirArchivoBucket(event, context):
    try:
        body = json.loads(event['body']) 
        universidad = body['universidad']
        codigoCurso = body['codigoCurso']
        codigoAlumno = body['codigoAlumno']
        archivoNombre = body['archivoNombre']

        s3_client = boto3.client('s3')
        # Estructura: universidades/UTEC/CS111/20201010/foto.jpg
        archivo_key = f'universidades/{universidad}/{codigoCurso}/{codigoAlumno}/{archivoNombre}'

        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=archivo_key,
            Body=b'Contenido del archivo de prueba' 
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Archivo subido exitosamente',
                'archivo_key': archivo_key
            })
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }