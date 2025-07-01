import logging
from collections import defaultdict
import json
import time
from fastapi import APIRouter, HTTPException, logger
import requests
from dotenv import load_dotenv
import os
from azure.storage.blob import BlobServiceClient
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
        logger.info("Token de acesso obtido com sucesso.")
        return token_response['result']['accessToken']  # Ajuste conforme a estrutura do seu JSON de resposta
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
        time.sleep(5)
        response = requests.get(f"{url}?WorkflowExecutionId={workflow_id}", headers=headers)
        if response.status_code == 200:
            response_json = response.json()
            if response_json['result']['status'] == 'WF_COMPLETED_SUCCESS':
                #log(response_json, 2)
                logger.info("Workflow completado com sucesso.")
                return response_json['result']['output_collection']['output_datas'][0]['workflow_step_output_collection']['output']['json_data']['context_markdown']
            elif response_json['result']['status'] == 'WF_COMPLETED_ERROR':
                return 'Erro ao iniciar o workflow'
        else:
            #print(f"Erro ao iniciar workflow: {response.status_code} - {response.text}")
            logger.error(f'Erro ao iniciar o workflow: {response.status_code} - {response.text}')
            return None

# ----- Log -----        
""" def log(mensagem, num):
    if num == 1:
        with open('log.json', 'w') as arquivo_json:
            json.dump(mensagem, arquivo_json, ensure_ascii=False, indent=4)
    elif num == 2:
        with open('log_extr.json', 'w') as arquivo_json:
            json.dump(mensagem, arquivo_json, ensure_ascii=False, indent=4) """
        

def atualizar_perda(data):  
    logger.info(f'Iniciando função para atualizar perda.')
    def calcular_perda(item):  
        duracaoAjustada = item.get("duracaoAjustada", 0)  
        UF = item.get("UF", 0) / 100  
        DF = item.get("DF", 0) / 100  
        taxa = item.get("taxa", 0)  
  
        # Cálculo da perda  
        perda = duracaoAjustada * UF * DF * taxa  
        item["perda"] = round(perda) * -1
  
    # Função recursiva para percorrer o dicionário  
    def percorrer_dicionario(d):  
        if isinstance(d, dict):  
            for key, value in d.items():  
                if isinstance(value, list):  
                    for item in value:  
                        if isinstance(item, dict):  
                            if all(k in item for k in ["duracaoAjustada", "UF", "DF", "taxa"]):  
                                calcular_perda(item)  
                            percorrer_dicionario(item)  # Adiciona essa linha para percorrer dicionários aninhados  
                else:  
                    percorrer_dicionario(value)  # Adiciona essa linha para percorrer dicionários aninhados  
        elif isinstance(d, list):  
            for item in d:  
                percorrer_dicionario(item)   
  
    # Inicia a busca  
    percorrer_dicionario(data)  
    logger.info("Campo perda atualizado com sucesso.")
    return data  

def calcular_diferenca(json_data):  
    logger.info("Iniciando função para calcular a diferença.")
    # Percorre cada equipamento e soma as perdas de HMC e HOI 
    for sistema in json_data["sistemas"]: 
        for equipamento in sistema['equipamentos']:  
            hmc = equipamento.get('HMC', [])  
            hoi = equipamento.get('HOI', [])  
            taxa = equipamento.get('taxaHora', [])
            
            saldoUF = equipamento.get('saldoUF', 0)  
            saldoDF = equipamento.get('saldoDF', 0)  
            saldoTaxa = equipamento.get('saldoTaxa', 0)
            
            def calculo(indicador, indicador_lista, saldo):
                total = 0
                
                for evento in indicador_lista:
                    total += evento.get('perda', 0)
                    
                if saldo > total:
                    if saldo < 0:
                        saldo *= -1
                        diferenca = abs(saldo + (total))
                    else:
                        diferenca = abs(saldo - (total))
                else:
                    diferenca = -1
                
                if indicador == 'uf':
                    obs = "Compensação por dias com maior utilização física."
                if indicador == 'df':
                    obs = "Compensação por dias com maior disponibilidade física."
                if indicador == 'taxa':
                    obs = "Compensação por dias com maior taxa horária."
                
                if diferenca > 0:
                    compensacao = {  
                        "duracaoAjustada": 0,  
                        "UF": 0,  
                        "DF": 0,  
                        "taxa": 0,  
                        "causa": "",  
                        "falha": "",  
                        "EGP": "",  
                        "observacao": obs,  
                        "perda": diferenca  
                    } 
                    indicador_lista.append(compensacao)
                    
            calculo('uf', hoi, saldoUF)       
            calculo('df', hmc, saldoDF)       
            calculo('taxa', taxa, saldoTaxa)       
            
            
    
    # Retorna sempre a menor diferença
    logger.info("Valores positivos adicionados com sucesso.")  
    return json_data

def agrupar_similares(data):  
    logger.info("Iniciando função para agrupar similares.")
    # Dicionário para armazenar grupos de itens com observações semelhantes  
    grouped_data = {  
        "tipo": data["tipo"],
        "sistemas": []  
    }  
  
    for sistema in data["sistemas"]:  
        sistema_dict = {  
            "sistema": sistema["sistema"],
            "totalSistema": sistema["totalSistema"],  
            "equipamentos": []  
        }  
  
        for equipamento in sistema["equipamentos"]:  
            equipamento_dict = { 
                "equipamento": equipamento["equipamento"],
                "totalEquipamento": equipamento["totalEquipamento"],
                "saldoDF": equipamento["saldoDF"],  
                "HMC": [],  
                "saldoUF": equipamento["saldoUF"],  
                "HOI": [],  
                "saldoTaxa": equipamento["saldoTaxa"],  
                "taxaHora": []  
            }  
  
            # Função auxiliar para agrupar itens
            def agrupar_itens(items, item_type):
                groups = defaultdict(lambda: {
                    "duracaoAjustada": 0,
                    "UF": 0,
                    "DF": 0,
                    "taxa": 0,
                    "causa": "",
                    "falha": "",
                    "EGP": "",
                    "observacao": "",
                    "perda": 0
                })

                for item in items:
                    key = (item["causa"], item["falha"], item["EGP"], item["observacao"])
                    group = groups[key]
                    group["duracaoAjustada"] += item["duracaoAjustada"]
                    group["UF"] = item["UF"]
                    group["DF"] = item["DF"]
                    group["taxa"] = item["taxa"]
                    group["causa"] = item["causa"]
                    group["falha"] = item["falha"]
                    group["EGP"] = item["EGP"]
                    group["observacao"] = item["observacao"]
                    group["perda"] += item["perda"]

                result = []
                for group in groups.values():
                    group["duracaoAjustada"] = round(group["duracaoAjustada"], 2)
                    group["perda"] = round(group["perda"])
                    result.append(group)

                return result

            # Agrupar HMC, HOI e taxaHora
            equipamento_dict["HMC"] = agrupar_itens(equipamento["HMC"], "HMC")
            equipamento_dict["HOI"] = agrupar_itens(equipamento["HOI"], "HOI")
            equipamento_dict["taxaHora"] = agrupar_itens(equipamento["taxaHora"], "taxaHora")

            sistema_dict["equipamentos"].append(equipamento_dict)
  
        grouped_data["sistemas"].append(sistema_dict)  
  
    logger.info("Perdas similares agrupadas com sucesso.")
    return grouped_data 

def calcular_total_sistema(data):
    logger.info("Iniciando função para calcular o total do sistema e dos equipamentos.")
    for sistema in data["sistemas"]:  
        total = 0
        for equipamento in sistema["equipamentos"]:
            total_equipamento = 0
            total_equipamento += equipamento["saldoDF"]
            total_equipamento += equipamento["saldoUF"]
            total_equipamento += equipamento["saldoTaxa"]

            equipamento["totalEquipamento"] = total_equipamento
            total += equipamento["totalEquipamento"]
        sistema["totalSistema"] = total
    logger.info("Total do sistema e dos equipamentos calculados com sucesso.")
    return data 

def agrupar_sistemas(data):  
    logger.info("Iniciando função para agrupar todos os sistemas.")
    total_sistema = 0
    total_equipamento = 0
    total_saldoDF = 0
    total_saldoUF = 0
    total_saldoTaxa = 0
    # Inicializa o dicionário final  
    resultado = {  
        'tipo': data['tipo'],
        'sistemas': []
    }  
    
    if data['tipo'] == 'PRODUÇÃO':
        sistema_agrupado = {
            "sistema": "Usinas",
            "totalSistema": 0,
            "equipamentos": []
        }  
    
    if data['tipo'] == 'ESTÉRIL':
        sistema_agrupado = {
            "sistema": "5ª Britagem",
            "totalSistema": 0,
            "equipamentos": []
        }  
    
    equip_agrupado = {  
        'equipamento': 'Equipamentos agrupados', 
        "totalEquipamento": 0, 
        'saldoDF': 0,  
        'HMC': [],  
        'saldoUF': 0,  
        'HOI': [], 
        'saldoTaxa': 0,
        "taxaHora": []  
    }  
    
    # Itera sobre os sistemas  
    for sistema in data['sistemas']: 
        total_sistema += sistema['totalSistema']
        sistema_agrupado['totalSistema'] = total_sistema
        # Itera sobre os equipamentos  
        for equipamento in sistema['equipamentos']:  
            total_equipamento += equipamento['totalEquipamento']
            total_saldoDF += equipamento['saldoDF']
            total_saldoUF += equipamento['saldoUF']
            total_saldoTaxa += equipamento['saldoTaxa']
            
            equip_agrupado["totalEquipamento"] = total_equipamento
            equip_agrupado["saldoDF"] = total_saldoDF
            equip_agrupado["saldoUF"] = total_saldoUF
            equip_agrupado["saldoTaxa"] = total_saldoTaxa
            # Cria um dicionário para o equipamento agrupado  
            for hmc in equipamento['HMC']:
                equip_agrupado["HMC"].append(hmc)
            for hoi in equipamento['HOI']:
                equip_agrupado["HOI"].append(hoi)
            for taxa in equipamento["taxaHora"]:
                equip_agrupado["taxaHora"].append(taxa)
                
    sistema_agrupado['equipamentos'].append(equip_agrupado)
            
    resultado["sistemas"].append(sistema_agrupado) 
    logger.info("Sistemas agrupados com sucesso.")
    return resultado 
 
def limpar_lista(data):
    logger.info("Iniciando função para limpar listas com saldo positivo.")
    for sistema in data['sistemas']: 
        # Itera sobre os equipamentos  
        for equipamento in sistema['equipamentos']:  
            if equipamento["saldoDF"] > 400:
                equipamento['HMC'] = []
            if equipamento["saldoUF"] > 400:
                equipamento['HOI'] = []
            if equipamento["saldoTaxa"] > 400:
                equipamento['taxaHora'] = []             
    # Ordena os eventos da menor quantidade para a maior  
    logger.info("Listas limpas com sucesso.")
    return data

def organizar_estrutura(data):
    logger.info("Iniciando função para organizar a estrutura do json.")
    resultado = {  
        'tipo': data['tipo'],
        'sistemas': []
    }   
    
    # Itera sobre os sistemas  
    for sistema in data['sistemas']:
        if data['tipo'] == 'ROM':
            total_sis = round(sistema['totalSistema'], 1)
            decimal_sis = total_sis % 1
            if decimal_sis == 0:
                total_sis = round(sistema['totalSistema'])
        else: 
            total_sis = round(sistema['totalSistema'])
        
        sistema_org = {
            "sistema": sistema['sistema'],
            "totalSistema": total_sis,
            "equipamentos": []
        }   
        # Itera sobre os equipamentos  
        for equipamento in sistema['equipamentos']:
            if data['tipo'] == 'ROM':
                total_equip = round(equipamento['totalEquipamento'], 1)
                decimal_equip = total_equip % 1
                if decimal_equip == 0:
                    total_equip = round(equipamento['totalEquipamento'])
            else:
                total_equip = round(equipamento['totalEquipamento'])
                
            equip_org = {  
                'equipamento': equipamento['equipamento'], 
                'totalEquipamento': total_equip,
                'indicadores': [],
            }    
            
            def organizar_indicador(tipo, indicador_lista, total_indicador):
                if data['tipo'] == 'ROM':
                    total_ind = round(total_indicador, 1)
                    decimal_ind = total_ind % 1
                    if decimal_ind == 0:
                        total_ind = round(total_indicador)
                else:
                    total_ind = round(total_indicador)
                        
                    
                indicador = {
                    'tipo': tipo,
                    'totalIndicador': total_ind,
                    'detalhes': [],
                }
                
                for item in indicador_lista:
                    indicador_novo = {
                        "causa": item['causa'],
                        "falha": item['falha'],
                        "EGP": item['EGP'],
                        "observacao": item['observacao'],
                        "perda": item['perda']
                    }
                    if item['perda'] != 0:
                        indicador["detalhes"].append(indicador_novo)
                
                indicador["detalhes"] = sorted(indicador["detalhes"], key=lambda x: x["perda"])
                equip_org["indicadores"].append(indicador)
                
            organizar_indicador('UF', equipamento['HOI'], equipamento['saldoUF'])
            organizar_indicador('DF', equipamento['HMC'], equipamento['saldoDF'])
            organizar_indicador('taxa', equipamento['taxaHora'], equipamento['saldoTaxa'])
            
            equip_org["indicadores"] = sorted(equip_org["indicadores"], key=lambda x: x["totalIndicador"])
            sistema_org['equipamentos'].append(equip_org)
        sistema_org['equipamentos'] = sorted(sistema_org['equipamentos'], key=lambda x: x["totalEquipamento"])
        resultado["sistemas"].append(sistema_org)
    resultado["sistemas"] = sorted(resultado['sistemas'], key=lambda x: x["totalSistema"])
    logger.info("Estrutura organizada com sucesso.") 
    return resultado

# ----- Start new workflow -----
@router.post("/sumarizacao_case2_Plataforma")
async def ExecuteWorkflow(json_input: dict):
    text = calcular_total_sistema(calcular_diferenca(atualizar_perda(json_input)))
    
    url = os.getenv("ENDPOINT_POST_START_NEW_WORKFLOW")
    headers = {
        "Authorization": f"Bearer {get_token()}",
        "Content-Type": "application/json; charset=utf-8",
        "Avanade-Tenant-Id": os.getenv("TENANT_ID")
    }
    
    resposta_completa = {}
    buildUp_tecnico = []
    buildUp_semanal = []
    resumo_executivo = []

    tipo = text["tipo"]
    text["tipo"] = text["tipo"].upper()

    start = time.time()
    if text["tipo"] == "ROM":
        logger.info(f'Iniciando processo tipo ROM.')
        text = organizar_estrutura(agrupar_similares(limpar_lista(text)))
        for sistema in text['sistemas']:
            start1 = time.time()
            # sumarização buildup tecnico
            start_sum1 = start_new_template_workflow(os.getenv('WORKFLOW_CODE_CS2_TECNICO_ROM'))
            json_sum1 = json_to_string(sistema)
            updated_sum1 = update_workflow_response(start_sum1, json_sum1)    
            get_sum1 = updated_sum1.get('result')

            # Enviando o POST para a API externa
            try:
                logger.info(f'[CASE 2 PLATAFORMA - Iniciando processo de sumarização do BuildUp Técnico]') 
                response_sum1 = requests.post(url, json=get_sum1, headers=headers)
                if(response_sum1.status_code == 200):
                    response_sum_json1 = response_sum1.json()
                    resposta_sum1 = GetWorkflowExecutionStatus(response_sum_json1['result']['workflowExecutionId'])
                    json_resposta_sum1 = json.loads(resposta_sum1)
                    buildUp_tecnico.append(json_resposta_sum1)
                    end1 = time.time()
                    print(f'({sistema["sistema"]}) BuildUp Tecnico - Tempo de execução: {(end1 - start1) / 60:.2f} minutos')
                else:
                    response_sum1.raise_for_status()
                
            except requests.exceptions.RequestException as e:
                logger.error(f'Erro a o enviar dados para a API externa: {str(e)}')
                stack_trace = traceback.format_exc()
                logger.error(stack_trace)
                raise HTTPException(status_code=500, detail="Sistema indisponível no momento. Tente novamente em alguns minutos.")
            
            start2 = time.time()
            # sumarização buildup semanal
            start_sum2 = start_new_template_workflow(os.getenv('WORKFLOW_CODE_CS2_SEMANAL_ROM'))
            json_sum2 = json_to_string(resposta_sum1)
            updated_sum2 = update_workflow_response(start_sum2, json_sum2)    
            get_sum2 = updated_sum2.get('result')
            #log(get_sum2, 1)

            # Enviando o POST para a API externa
            try:
                logger.info(f'[CASE 2 PLATAFORMA - Iniciando processo de sumarização do BuildUp Semanal]') 
                response_sum2 = requests.post(url, json=get_sum2, headers=headers)
                if(response_sum2.status_code == 200):
                    response_sum_json2 = response_sum2.json()
                    resposta_sum2 = GetWorkflowExecutionStatus(response_sum_json2['result']['workflowExecutionId'])
                    json_resposta_sum2 = json.loads(resposta_sum2)
                    buildUp_semanal.append(json_resposta_sum2)
                    end2 = time.time()
                    print(f'({sistema["sistema"]}) BuildUp Semanal - Tempo de execução: {(end2 - start2) / 60:.2f} minutos')
                else:
                    response_sum2.raise_for_status()
                
            except requests.exceptions.RequestException as e:
                logger.error(f'Erro a o enviar dados para a API externa: {str(e)}')
                stack_trace = traceback.format_exc()
                logger.error(stack_trace)
                raise HTTPException(status_code=500, detail="Sistema indisponível no momento. Tente novamente em alguns minutos.")
            
            
            start3 = time.time()
            # sumarização resumo executivo
            start_sum3 = start_new_template_workflow(os.getenv('WORKFLOW_CODE_CS2_EXECUTIVO_ROM'))
            json_sum3 = json_to_string(resposta_sum1)
            updated_sum3 = update_workflow_response(start_sum3, json_sum3)    
            get_sum3 = updated_sum3.get('result')
            #log(get_sum3, 1)

            # Enviando o POST para a API externa
            try:
                logger.info(f'[CASE 2 PLATAFORMA - Iniciando processo de sumarização do resumo executivo]') 
                response_sum3 = requests.post(url, json=get_sum3, headers=headers)
                if(response_sum3.status_code == 200):
                    response_sum_json3 = response_sum3.json()
                    resposta_sum3 = GetWorkflowExecutionStatus(response_sum_json3['result']['workflowExecutionId'])
                    json_resposta_sum3 = json.loads(resposta_sum3)
                    if json_resposta_sum1["total_sistema"] < 0:
                        resumo_executivo.append(json_resposta_sum3)
                    end3 = time.time()
                    print(f'({sistema["sistema"]}) Resumo Executivo - Tempo de execução: {(end3 - start3) / 60:.2f} minutos')
                else:
                    response_sum3.raise_for_status()
                
            except requests.exceptions.RequestException as e:
                logger.error(f'Erro a o enviar dados para a API externa: {str(e)}')
                stack_trace = traceback.format_exc()
                logger.error(stack_trace)
                raise HTTPException(status_code=500, detail="Sistema indisponível no momento. Tente novamente em alguns minutos.")
    elif text["tipo"] == "ESTÉRIL":
        logger.info(f'Iniciando processo tipo Estéril.')
        text = organizar_estrutura(agrupar_similares(limpar_lista(agrupar_sistemas(text))))
        for sistema in text['sistemas']:
            start1 = time.time()
            # sumarização buildup tecnico
            start_sum1 = start_new_template_workflow(os.getenv('WORKFLOW_CODE_CS2_TECNICO'))
            json_sum1 = json_to_string(sistema)
            updated_sum1 = update_workflow_response(start_sum1, json_sum1)    
            get_sum1 = updated_sum1.get('result')

            # Enviando o POST para a API externa
            try:
                logger.info(f'[CASE 2 PLATAFORMA - Iniciando processo de sumarização do BuildUp Técnico]') 
                response_sum1 = requests.post(url, json=get_sum1, headers=headers)
                if(response_sum1.status_code == 200):
                    response_sum_json1 = response_sum1.json()
                    resposta_sum1 = GetWorkflowExecutionStatus(response_sum_json1['result']['workflowExecutionId'])
                    json_resposta_sum1 = json.loads(resposta_sum1)
                    buildUp_tecnico.append(json_resposta_sum1)
                    end1 = time.time()
                    print(f'({sistema["sistema"]}) BuildUp Tecnico - Tempo de execução: {(end1 - start1) / 60:.2f} minutos')
                else:
                    response_sum1.raise_for_status()
                
            except requests.exceptions.RequestException as e:
                logger.error(f'Erro a o enviar dados para a API externa: {str(e)}')
                stack_trace = traceback.format_exc()
                logger.error(stack_trace)
                raise HTTPException(status_code=500, detail="Sistema indisponível no momento. Tente novamente em alguns minutos.")
            
            start2 = time.time()
            # sumarização buildup semanal
            start_sum2 = start_new_template_workflow(os.getenv('WORKFLOW_CODE_CS2_SEMANAL'))
            json_sum2 = json_to_string(resposta_sum1)
            updated_sum2 = update_workflow_response(start_sum2, json_sum2)    
            get_sum2 = updated_sum2.get('result')
            #log(get_sum2, 1)

            # Enviando o POST para a API externa
            try:
                logger.info(f'[CASE 2 PLATAFORMA - Iniciando processo de sumarização do BuildUp Semanal]') 
                response_sum2 = requests.post(url, json=get_sum2, headers=headers)
                if(response_sum2.status_code == 200):
                    response_sum_json2 = response_sum2.json()
                    resposta_sum2 = GetWorkflowExecutionStatus(response_sum_json2['result']['workflowExecutionId'])
                    json_resposta_sum2 = json.loads(resposta_sum2)
                    buildUp_semanal.append(json_resposta_sum2)
                    end2 = time.time()
                    print(f'({sistema["sistema"]}) BuildUp Semanal - Tempo de execução: {(end2 - start2) / 60:.2f} minutos')
                else:
                    response_sum2.raise_for_status()
                
            except requests.exceptions.RequestException as e:
                logger.error(f'Erro a o enviar dados para a API externa: {str(e)}')
                stack_trace = traceback.format_exc()
                logger.error(stack_trace)
                raise HTTPException(status_code=500, detail="Sistema indisponível no momento. Tente novamente em alguns minutos.")
            
            
            start3 = time.time()
            # sumarização resumo executivo
            start_sum3 = start_new_template_workflow(os.getenv('WORKFLOW_CODE_CS2_EXECUTIVO_ESTERIL'))
            json_sum3 = json_to_string(resposta_sum1)
            updated_sum3 = update_workflow_response(start_sum3, json_sum3)    
            get_sum3 = updated_sum3.get('result')
            #log(get_sum3, 1)

            # Enviando o POST para a API externa
            try:
                logger.info(f'[CASE 2 PLATAFORMA - Iniciando processo de sumarização do resumo executivo]') 
                response_sum3 = requests.post(url, json=get_sum3, headers=headers)
                if(response_sum3.status_code == 200):
                    response_sum_json3 = response_sum3.json()
                    resposta_sum3 = GetWorkflowExecutionStatus(response_sum_json3['result']['workflowExecutionId'])
                    json_resposta_sum3 = json.loads(resposta_sum3)
                    if json_resposta_sum3["resumo"] != "":
                        resumo_executivo.append(json_resposta_sum3)
                    end3 = time.time()
                    print(f'({sistema["sistema"]}) Resumo Executivo - Tempo de execução: {(end3 - start3) / 60:.2f} minutos')
                else:
                    response_sum3.raise_for_status()
                
            except requests.exceptions.RequestException as e:
                logger.error(f'Erro a o enviar dados para a API externa: {str(e)}')
                stack_trace = traceback.format_exc()
                logger.error(stack_trace)
                raise HTTPException(status_code=500, detail="Sistema indisponível no momento. Tente novamente em alguns minutos.")
    elif text['tipo'] == "PRODUÇÃO":
        logger.info(f'Iniciando processo tipo Produção.')
        text = organizar_estrutura(agrupar_similares(limpar_lista(agrupar_sistemas(text))))
        for sistema in text['sistemas']:
            start1 = time.time()
            # sumarização buildup tecnico
            start_sum1 = start_new_template_workflow(os.getenv('WORKFLOW_CODE_CS2_TECNICO'))
            json_sum1 = json_to_string(sistema)
            updated_sum1 = update_workflow_response(start_sum1, json_sum1)    
            get_sum1 = updated_sum1.get('result')

            # Enviando o POST para a API externa
            try:
                logger.info(f'[CASE 2 PLATAFORMA - Iniciando processo de sumarização do BuildUp Técnico]') 
                response_sum1 = requests.post(url, json=get_sum1, headers=headers)
                if(response_sum1.status_code == 200):
                    response_sum_json1 = response_sum1.json()
                    resposta_sum1 = GetWorkflowExecutionStatus(response_sum_json1['result']['workflowExecutionId'])
                    json_resposta_sum1 = json.loads(resposta_sum1)
                    buildUp_tecnico.append(json_resposta_sum1)
                    end1 = time.time()
                    print(f'({sistema["sistema"]}) BuildUp Tecnico - Tempo de execução: {(end1 - start1) / 60:.2f} minutos')
                else:
                    response_sum1.raise_for_status()
                
            except requests.exceptions.RequestException as e:
                logger.error(f'Erro a o enviar dados para a API externa: {str(e)}')
                stack_trace = traceback.format_exc()
                logger.error(stack_trace)
                raise HTTPException(status_code=500, detail="Sistema indisponível no momento. Tente novamente em alguns minutos.")
            
            start2 = time.time()
            # sumarização buildup semanal
            start_sum2 = start_new_template_workflow(os.getenv('WORKFLOW_CODE_CS2_SEMANAL'))
            json_sum2 = json_to_string(resposta_sum1)
            updated_sum2 = update_workflow_response(start_sum2, json_sum2)    
            get_sum2 = updated_sum2.get('result')
            #log(get_sum2, 1)

            # Enviando o POST para a API externa
            try:
                logger.info(f'[CASE 2 PLATAFORMA - Iniciando processo de sumarização do BuildUp Semanal]') 
                response_sum2 = requests.post(url, json=get_sum2, headers=headers)
                if(response_sum2.status_code == 200):
                    response_sum_json2 = response_sum2.json()
                    resposta_sum2 = GetWorkflowExecutionStatus(response_sum_json2['result']['workflowExecutionId'])
                    json_resposta_sum2 = json.loads(resposta_sum2)
                    buildUp_semanal.append(json_resposta_sum2)
                    end2 = time.time()
                    print(f'({sistema["sistema"]}) BuildUp Semanal - Tempo de execução: {(end2 - start2) / 60:.2f} minutos')
                else:
                    response_sum2.raise_for_status()
                
            except requests.exceptions.RequestException as e:
                logger.error(f'Erro a o enviar dados para a API externa: {str(e)}')
                stack_trace = traceback.format_exc()
                logger.error(stack_trace)
                raise HTTPException(status_code=500, detail="Sistema indisponível no momento. Tente novamente em alguns minutos.")
            
            
            start3 = time.time()
            # sumarização resumo executivo
            start_sum3 = start_new_template_workflow(os.getenv('WORKFLOW_CODE_CS2_EXECUTIVO_PRODUCAO'))
            json_sum3 = json_to_string(resposta_sum1)
            updated_sum3 = update_workflow_response(start_sum3, json_sum3)    
            get_sum3 = updated_sum3.get('result')
            #log(get_sum3, 1)

            # Enviando o POST para a API externa
            try:
                logger.info(f'[CASE 2 PLATAFORMA - Iniciando processo de sumarização do resumo executivo]') 
                response_sum3 = requests.post(url, json=get_sum3, headers=headers)
                if(response_sum3.status_code == 200):
                    response_sum_json3 = response_sum3.json()
                    resposta_sum3 = GetWorkflowExecutionStatus(response_sum_json3['result']['workflowExecutionId'])
                    json_resposta_sum3 = json.loads(resposta_sum3)
                    if json_resposta_sum3["resumo"] != "":
                        resumo_executivo.append(json_resposta_sum3)
                    end3 = time.time()
                    print(f'({sistema["sistema"]}) Resumo Executivo - Tempo de execução: {(end3 - start3) / 60:.2f} minutos')
                else:
                    response_sum3.raise_for_status()
                
            except requests.exceptions.RequestException as e:
                logger.error(f'Erro a o enviar dados para a API externa: {str(e)}')
                stack_trace = traceback.format_exc()
                logger.error(stack_trace)
                raise HTTPException(status_code=500, detail="Sistema indisponível no momento. Tente novamente em alguns minutos.")
    else:
        return "Tipo não encontrado. Tipos aceitos: ROM, Estéril e Produção."

    end = time.time()

    print(f'Tempo de execução: {(end - start) / 60:.2f} minutos')

    resposta_completa['tipo'] = tipo
    resposta_completa['buildUp_tecnico'] = buildUp_tecnico
    resposta_completa['buildUp_semanal'] = buildUp_semanal
    resposta_completa['resumo_executivo'] = resumo_executivo

    logger.info("Sumarização realizada com sucesso.")    
    return resposta_completa 