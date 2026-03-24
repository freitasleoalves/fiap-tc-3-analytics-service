import os
import sys
import threading
import json
import uuid
import time
import logging
from flask import Flask, jsonify
from dotenv import load_dotenv

# Configura o logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

# Carrega .env para desenvolvimento local
load_dotenv()

# --- Configuração ---
CLOUD_PROVIDER = os.getenv("CLOUD_PROVIDER", "aws")  # "azure" ou "aws" (default)


# ===================== AWS (LocalStack / SQS + DynamoDB) =====================

def init_aws():
    """Inicializa clientes AWS (SQS + DynamoDB)."""
    import boto3
    from botocore.exceptions import NoCredentialsError

    region = os.getenv("AWS_REGION")
    queue_url = os.getenv("AWS_SQS_URL")
    table_name = os.getenv("AWS_DYNAMODB_TABLE")

    if not all([region, queue_url, table_name]):
        log.critical("Erro: AWS_REGION, AWS_SQS_URL, e AWS_DYNAMODB_TABLE devem ser definidos.")
        sys.exit(1)

    try:
        session = boto3.Session(region_name=region)
        endpoint_url = os.getenv("AWS_ENDPOINT_URL")
        sqs = session.client("sqs", endpoint_url=endpoint_url)
        dynamo = session.client("dynamodb", endpoint_url=endpoint_url)
        log.info(f"Clientes Boto3 inicializados na região {region}")
    except NoCredentialsError:
        log.critical("Credenciais da AWS não encontradas.")
        sys.exit(1)

    return sqs, dynamo, queue_url, table_name


def aws_process_message(sqs, dynamo, queue_url, table_name, message):
    """Processa uma mensagem SQS e insere no DynamoDB."""
    from botocore.exceptions import ClientError
    try:
        log.info(f"Processando mensagem ID: {message['MessageId']}")
        body = json.loads(message['Body'])
        event_id = str(uuid.uuid4())

        dynamo.put_item(
            TableName=table_name,
            Item={
                'event_id': {'S': event_id},
                'user_id': {'S': body['user_id']},
                'flag_name': {'S': body['flag_name']},
                'result': {'BOOL': body['result']},
                'timestamp': {'S': body['timestamp']}
            }
        )
        log.info(f"Evento {event_id} (Flag: {body['flag_name']}) salvo no DynamoDB.")

        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=message['ReceiptHandle'])
    except json.JSONDecodeError:
        log.error(f"Erro ao decodificar JSON da mensagem ID: {message['MessageId']}")
    except ClientError as e:
        log.error(f"Erro do Boto3 ao processar {message['MessageId']}: {e}")
    except Exception as e:
        log.error(f"Erro inesperado ao processar {message['MessageId']}: {e}")


def aws_worker_loop(sqs, dynamo, queue_url, table_name):
    """Loop principal do worker SQS."""
    from botocore.exceptions import ClientError
    log.info("Iniciando o worker SQS...")
    while True:
        try:
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20
            )
            messages = response.get('Messages', [])
            if not messages:
                continue
            log.info(f"Recebidas {len(messages)} mensagens.")
            for msg in messages:
                aws_process_message(sqs, dynamo, queue_url, table_name, msg)
        except ClientError as e:
            log.error(f"Erro do Boto3 no loop SQS: {e}")
            time.sleep(10)
        except Exception as e:
            log.error(f"Erro inesperado no loop SQS: {e}")
            time.sleep(10)


# ===================== Azure (Service Bus + Cosmos DB Table) =====================

def init_azure():
    """Inicializa clientes Azure (Service Bus + Cosmos DB Table)."""
    from azure.servicebus import ServiceBusClient
    from azure.data.tables import TableServiceClient

    sb_conn_str = os.getenv("AZURE_SERVICEBUS_CONNECTION_STRING")
    sb_queue_name = os.getenv("AZURE_SERVICEBUS_QUEUE_NAME")
    cosmos_conn_str = os.getenv("AZURE_COSMOSDB_CONNECTION_STRING")
    cosmos_table_name = os.getenv("AZURE_COSMOSDB_TABLE_NAME", "EvaluationEvents")

    if not all([sb_conn_str, sb_queue_name, cosmos_conn_str]):
        log.critical("Erro: AZURE_SERVICEBUS_CONNECTION_STRING, AZURE_SERVICEBUS_QUEUE_NAME e AZURE_COSMOSDB_CONNECTION_STRING devem ser definidos.")
        sys.exit(1)

    sb_client = ServiceBusClient.from_connection_string(sb_conn_str)
    table_service = TableServiceClient.from_connection_string(cosmos_conn_str)
    table_client = table_service.get_table_client(cosmos_table_name)

    log.info("Clientes Azure (Service Bus + Cosmos DB Table) inicializados.")
    return sb_client, sb_queue_name, table_client


def azure_process_message(table_client, message):
    """Processa uma mensagem do Service Bus e insere no Cosmos DB Table."""
    try:
        body = json.loads(str(message))
        event_id = str(uuid.uuid4())

        entity = {
            'PartitionKey': body['flag_name'],
            'RowKey': event_id,
            'user_id': body['user_id'],
            'flag_name': body['flag_name'],
            'result': body['result'],
            'timestamp': body['timestamp']
        }

        table_client.create_entity(entity=entity)
        log.info(f"Evento {event_id} (Flag: {body['flag_name']}) salvo no Cosmos DB Table.")
    except json.JSONDecodeError:
        log.error("Erro ao decodificar JSON da mensagem do Service Bus.")
    except Exception as e:
        log.error(f"Erro ao processar mensagem do Service Bus: {e}")


def azure_worker_loop(sb_client, queue_name, table_client):
    """Loop principal do worker Azure Service Bus."""
    log.info("Iniciando o worker Azure Service Bus...")
    while True:
        try:
            with sb_client.get_queue_receiver(queue_name=queue_name, max_wait_time=20) as receiver:
                for message in receiver:
                    azure_process_message(table_client, message)
                    receiver.complete_message(message)
        except Exception as e:
            log.error(f"Erro no loop Service Bus: {e}")
            time.sleep(10)


# ===================== Flask (Health Check) =====================

app = Flask(__name__)

@app.route('/health')
def health():
    return jsonify({"status": "ok"})


# ===================== Inicialização =====================

def start_worker():
    """Inicia o worker de fila em uma thread separada."""
    if CLOUD_PROVIDER == "azure":
        sb_client, queue_name, table_client = init_azure()
        target = azure_worker_loop
        args = (sb_client, queue_name, table_client)
    else:
        sqs, dynamo, queue_url, table_name = init_aws()
        target = aws_worker_loop
        args = (sqs, dynamo, queue_url, table_name)

    worker_thread = threading.Thread(target=target, args=args, daemon=True)
    worker_thread.start()


start_worker()

if __name__ == '__main__':
    port = int(os.getenv("PORT", 8005))
    app.run(host='0.0.0.0', port=port, debug=False)