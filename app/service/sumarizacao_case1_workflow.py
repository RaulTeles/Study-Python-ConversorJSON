import logging
import time
from fastapi import HTTPException, APIRouter, logger
import requests
import json
import os
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import time
import traceback

load_dotenv()
router = APIRouter()

logger = logging.getLogger(__name__)  
logger.setLevel(logging.INFO)  
  
# Cria um manipulador de arquivo  
file_handler = logging.FileHandler('log.txt', encoding='utf-8')  
file_handler.setLevel(logging.INFO)  
  
# Cria um formatador e o adiciona ao manipulador de arquivo  
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')  
file_handler.setFormatter(formatter)  
  
# Adiciona o manipulador de arquivo ao logger  
logger.addHandler(file_handler) 

# Função para fazer o upload do log para o Blob Storage  
def upload_log_to_blob():  
    try:  
        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        if not connection_string:  
            raise ValueError("A variável de ambiente AZURE_STORAGE_CONNECTION_STRING não está definida.") 
          
        blob_service_client = BlobServiceClient.from_connection_string(conn_str=connection_string)  
           
        container_name = "logs"
        blob_name = "log"   

        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)    
  
        with open("log.txt", "rb") as data:  
            blob_client.upload_blob(data, overwrite=True)
        logger.info("Upload do log.txt realizado com sucesso.")   
          
    except Exception as e:  
        logger.error(f"Erro ao fazer upload do log para o blob: {e}")  
        print(f"Erro ao fazer upload do log para o blob: {e}") 

# Chamar a função para realizar o upload do log 
upload_log_to_blob()  

# ----- Obter token de autenticação -----
def TokenAuth():
    API_URL = os.getenv("URL_TOKEN")
    API_KEY = os.getenv("API_KEY")
    payload = f'"{API_KEY}"'
    try:
        # Realiza a requisição POST para a API
        logger.info("Iniciando requisição para a API.")
        response = requests.post(API_URL, data=payload, headers={"Content-Type": "application/json"})
        response.raise_for_status()  # Levanta uma exceção para status codes 4xx/5xx
        # Converte a resposta para JSON
        data = response.json()
        logger.info("Conversão da resposta para JSON concluída com sucesso.")
        return data
    except requests.exceptions.HTTPError as err:
        logger.error(f'Erro na requisição para a API {str(e)}')
        stack_trace = traceback.format_exc()
        logger.error(stack_trace)
        raise HTTPException(
            status_code=err.response.status_code, detail=str(err))
    except Exception as e:
        logger.error(f'Erro na requisição para a API {str(e)}')
        stack_trace = traceback.format_exc()
        logger.error(stack_trace)
        raise HTTPException(status_code=500, detail="Sistema indisponível no momento. Tente novamente em alguns minutos.")


# ----- Get token -----
def get_token():
    try:
        logger.info("Iniciando processo de obtenção do token de acesso.")
        token_response = TokenAuth()
        # Ajuste conforme a estrutura do seu JSON de resposta
        logger.info("Token de acesso obtido com sucesso.")
        return token_response['result']['accessToken']
    except KeyError as e:
        logger.error(f'Erro ao encontrar o token na resposta {str(e)}')
        stack_trace = traceback.format_exc()
        logger.error(stack_trace)
        raise HTTPException(status_code=500, detail=f"Token não encontrado na resposta: {str(e)}")
    except Exception as e:
        logger.error(f'Erro ao obter o token de acesso {str(e)}')
        stack_trace = traceback.format_exc()
        logger.error(stack_trace)
        raise HTTPException(status_code=500, detail=f"Erro ao obter o token: {str(e)}")


# ----- Start new template workflow -----
def start_new_template_workflow(code):
    logger.info("Iniciando workflow com função start_new_template_workflow")
    token = get_token()
    url = os.getenv("ENDPOINT_START_NEW_WORKFLOW_TEMPLATE")
    workflow_code = code
    email = ""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Avanade-Tenant-Id": os.getenv("TENANT_ID")
    }

    payload = {
        "workflowConfigurationCode": workflow_code,
        "userEmail": email
    }

    response = requests.post(url, json=payload, headers=headers)

    if response.status_code == 200:
        #print("Workflow iniciado com sucesso!")
        logger.info("Workflow iniciado com sucesso.")
        return response.json()
    else:
        #print(f"Erro ao iniciar workflow: {response.status_code} - {response.text}")
        logger.error(f'Erro ao iniciar o workflow: {response.status_code} - {response.text}')
        return None


# ----- Converter json em string -----
def json_to_string(json_data):
    try:
        logger.info("Iniciando conversão de JSON para string.")
        json_string = json.dumps(json_data, ensure_ascii=False)
        logger.info("JSON convertido para string com sucesso.")
        return json_string
    except Exception as e:
        logger.error(f'Erro ao converter JSON para string: {str(e)}')
        stack_trace = traceback.format_exc()
        logger.error(stack_trace)
        return str(e)


# ----- Atualizar campo actual_value -----
def update_workflow_response(workflow_response, new_value):
    try:
        logger.info("Iniciando atualização do workflow response.")
        workflow_response['result']['inputCollection']['inputProperties'][0]['workflowStepProperties'][0]['actual_value'] = new_value
        logger.info("Workflow response atualizado com sucesso.")
        return workflow_response
    except KeyError as e:
        logger.error(f'Erro ao atualizar o workflow response: {str(e)}')
        stack_trace = traceback.format_exc()
        logger.error(stack_trace)
        raise HTTPException(
            status_code=400, detail="Sistema indisponível no momento. Tente novamente em alguns minutos.")

# ----- Get status -----
def GetWorkflowExecutionStatus(workflow_id: str):
    logger.info("Iniciando função de execução de status do workflow.")
    url = os.getenv("ENDPOINT_GET_WORKFLOW_EXEC_STATUS")
    token = get_token()

    headers = {
        "Authorization": f"Bearer {token}",
        "Avanade-Tenant-Id": os.getenv("TENANT_ID")
    }

    processando = True

    while processando == True:
        time.sleep(2)
        response = requests.get(f"{url}?WorkflowExecutionId={workflow_id}", headers=headers)
        if response.status_code == 200:
            response_json = response.json()
            if response_json['result']['status'] == 'WF_COMPLETED_SUCCESS':
                #log(response_json)
                logger.info("Workflow completado com sucesso.")
                return response_json['result']['output_collection']['output_datas'][0]['workflow_step_output_collection']['output']['json_data']['context_markdown']
            elif response_json['result']['status'] == 'WF_COMPLETED_ERROR':
                return 'Erro ao iniciar o workflow'
        else:
            #print(f"Erro ao iniciar workflow: {response.status_code} - {response.text}")
            logger.error(f'Erro ao iniciar o workflow: {response.status_code} - {response.text}')
            return None


# ----- Log -----
""" def log(mensagem):
    if isinstance(mensagem, str):
        # Escrever a mensagem em texto puro
        with open('log.txt', 'w') as arquivo_texto:
            arquivo_texto.write(mensagem)
    else:
        # Converter a mensagem em JSON e escrever no arquivo
        with open('log.json', 'w') as arquivo_json:
            json.dump(mensagem, arquivo_json, ensure_ascii=False, indent=4) """


# ----- Calcular total produzido por usina -----
def calcular_soma_quantidade(data): 
    logger.info("Iniciando cálculo do total produzido.") 
    for usina in data['usinas']:   
        soma_quantidade = sum(evento['quantidade'] for evento in usina['eventos'])  
        usina["total_produzido"] = soma_quantidade  

    logger.info("Cálculo do total produzido realizado com sucesso.")    
    return data   

# ----- Desconsiderar perdas maiores que -500 -----
def desconsiderar_perdas_menores(data):
    logger.info("Iniciando função para desconsiderar perdas menores.") 
    for usina in data["usinas"]:
        if usina["total_produzido"] > -500:
            usina["eventos"] = []

    logger.info("Verificação para desconsiderar perdas menores realizada com sucesso.")        
    return data

# ----- Agrupar perdas similares -----
def agrupar_similares(data): 
    logger.info("Iniciando função para agrupar itens similares.")  
    # Dicionário para armazenar grupos de itens com observações semelhantes  
    total_usinas = 0
    grouped_data = {  
        "regiao": data["regiao"],
        "total_regiao": 0,
        "usinas": []  
    }  
  
    for usina in data["usinas"]:  
        if usina["total_produzido"] > -500:
            usina_dict = {  
                "usina": usina["usina"],
                "total_produzido": 100,  
                "eventos": []  
            }
        else:  
            usina_dict = {  
                "usina": usina["usina"],
                "total_produzido": usina["total_produzido"],  
                "eventos": []  
            }  

        total_usinas += usina["total_produzido"]
        # Dicionário para agrupar eventos com a mesma justificativa  
        eventos_agrupados = {}  
  
        for evento in usina["eventos"]:    
            justificativa = evento["justificativa"]  
            quantidade = evento["quantidade"]  
  
            if justificativa:  # Agrupa apenas eventos com justificativa  
                if justificativa not in eventos_agrupados:  
                    eventos_agrupados[justificativa] = {  
                        "quantidade": quantidade,  
                        "justificativa": justificativa  
                    }  
                else:  
                    eventos_agrupados[justificativa]["quantidade"] += quantidade  
            else:   
                if quantidade < 0:
                # Se não houver justificativa, adiciona o evento como está  
                    evento_sem_justificativa = {  
                        "quantidade": quantidade,  
                        "justificativa": justificativa  
                    } 
                    usina_dict["eventos"].append(evento_sem_justificativa)  
  
        # Adiciona os eventos agrupados  
        for evento in eventos_agrupados.values():  
            usina_dict["eventos"].append(evento)  
            
        # Ordena os eventos da menor quantidade para a maior  
        usina_dict["eventos"] = sorted(usina_dict["eventos"], key=lambda x: x["quantidade"])  
  
        grouped_data["usinas"].append(usina_dict)    
        grouped_data["total_regiao"] = total_usinas
    logger.info("Agrupamento de itens similares realizado com sucesso.")
    return grouped_data 

# ----- Gerar json de entrada para o resumo executivo -----
def json_executivo(data): 
    logger.info("Iniciando função para gerar o JSON do resumo executivo.")  
    # Dicionário para armazenar grupos de itens com observações semelhantes  
    grouped_data = {  
        "regiao": data["regiao"],
        "total_regiao": data["total_regiao"],
        "usinas": []  
    }  
    
    if data["total_regiao"] > 0:
        grouped_data["usinas"] = []
    elif data["total_regiao"] >= -5000:
        for usina in data["usinas"]:  
            for evento in usina["eventos"]:    
                grouped_data["usinas"].append(evento)  
        grouped_data["usinas"] = sorted(grouped_data["usinas"], key=lambda x: x["quantidade"])
        grouped_data["usinas"] = grouped_data["usinas"][0]          
    else:
        for usina in data["usinas"]:  
            for evento in usina["eventos"]:    
                grouped_data["usinas"].append(evento)  
        grouped_data["usinas"] = sorted(grouped_data["usinas"], key=lambda x: x["quantidade"])
        
    # Ordena os eventos da menor quantidade para a maior  
    
    logger.info("JSON do resumo executivo gerado com sucesso.")
    return grouped_data 


# ----- Start new workflow -----
@router.post("/sumarizacao_case1_Plataforma")
async def ExecuteWorkflow(json_input: dict):
    start = time.time()
    text = agrupar_similares(desconsiderar_perdas_menores(calcular_soma_quantidade(json_input)))
    url = os.getenv("ENDPOINT_POST_START_NEW_WORKFLOW")
    
    headers = {
        "Authorization": f"Bearer {get_token()}",
        "Content-Type": "application/json; charset=utf-8",
        "Avanade-Tenant-Id": os.getenv("TENANT_ID")
    }
    resposta_completa = {
        "regiao": text["regiao"]
    }
    
    # Sumarização comentário usina
    data_usinas = start_new_template_workflow(os.getenv("WORKFLOW_CODE_CS1_USINAS"))
    json_usinas = json_to_string(text)
    updated_response_usinas = update_workflow_response(data_usinas, json_usinas)
    
    result_usinas = updated_response_usinas.get('result')
    try:
        logger.info(f'[CASE 1 PLATAFORMA - Iniciando processo de sumarização de comentários das Usinas]') 
        response = requests.post(url, json=result_usinas, headers=headers)
        if (response.status_code == 200):
            response_json = response.json()
            resposta_usinas = GetWorkflowExecutionStatus(response_json['result']['workflowExecutionId'])
            
            # Converter a string corrigida em um objeto JSON  
            json_resposta_usinas = json.loads(resposta_usinas) 
            end = time.time()
            resposta_completa["usinas"] = json_resposta_usinas
            print(f'Tempo de execução: {(end - start) / 60:.2f} minutos')
        else:
            response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f'Erro a o enviar dados para a API externa: {str(e)}')
        stack_trace = traceback.format_exc()
        logger.error(stack_trace)
        raise HTTPException(status_code=500, detail="Sistema indisponível no momento. Tente novamente em alguns minutos.")
    
    # Sumarização resumo executivo
    data_executivo = start_new_template_workflow(os.getenv("WORKFLOW_CODE_CS1_EXECUTIVO"))
    json_exec = json_to_string(json_executivo(text))
    updated_response_executivo = update_workflow_response(data_executivo, json_exec)
    
    result_executivo = updated_response_executivo.get('result')
    try:
        logger.info(f'[CASE 1 PLATAFORMA - Iniciando processo de sumarização do resumo executivo]') 
        response = requests.post(url, json=result_executivo, headers=headers)
        if (response.status_code == 200):
            response_json = response.json()
            resposta_executivo = GetWorkflowExecutionStatus(response_json['result']['workflowExecutionId'])
            
            # Converter a string corrigida em um objeto JSON  
            json_resposta_executivo = json.loads(resposta_executivo) 
            end = time.time()
            resposta_completa.update(json_resposta_executivo)
            print(f'Tempo de execução: {(end - start) / 60:.2f} minutos')
        else:
            response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f'Erro a o enviar dados para a API externa: {str(e)}')
        stack_trace = traceback.format_exc()
        logger.error(stack_trace)
        raise HTTPException(status_code=500, detail="Sistema indisponível no momento. Tente novamente em alguns minutos.")
    logger.info("Sumarização realizada com sucesso.")
    return resposta_completa