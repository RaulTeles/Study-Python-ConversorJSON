from collections import defaultdict
import logging
import os
import time
import traceback
from openai import AzureOpenAI
from dotenv import load_dotenv
from fastapi import APIRouter, HTTPException
import json
from azure.storage.blob import BlobServiceClient

load_dotenv()
router = APIRouter()

# Configuração do logger para salvar em um arquivo  
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

@router.post("/sumarizacao_case2_OpenAI/")
async def summarize_text(text_to_summarize: dict):
    try:
        logger.info(f'[CASE 2 OPENAI - Iniciando processo de sumarização]')
        client = AzureOpenAI(azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT1"), api_key=os.getenv("AZURE_OPENAI_API_KEY1"), api_version="2024-02-01")
        # Requisição para a API da OpenAI
        text = calcular_total_sistema(calcular_diferenca(atualizar_perda(text_to_summarize)))
        resposta_completa = {}
        buildUp_tecnico = []
        buildUp_semanal = []
        resumo_executivo = []
        
        tipo = text["tipo"]
        text["tipo"] = text["tipo"].upper()

        start = time.time()
        if text["tipo"] == "ROM":
            text = organizar_estrutura(agrupar_similares(text))
            logger.info(f'Iniciando processo tipo ROM.')
            for sistema in text['sistemas']:
                resposta = buildUp_tecnico_sumarizacao(sistema, client)
                buildUp_tecnico.append(resposta)
                resposta2 = buildUp_semanal_sumarizacao(resposta, client)
                buildUp_semanal.append(resposta2)
                resposta3 = resumo_executivo_sumarizacao(resposta, client)
                if resposta3["resumo"] != "":
                    resumo_executivo.append(resposta3)
        elif text["tipo"] == "ESTÉRIL":
            logger.info(f'Iniciando processo tipo Estéril.')
            group = organizar_estrutura(agrupar_similares(agrupar_sistemas(text)))
            for sistema in group['sistemas']:
                resposta = buildUp_tecnico_sumarizacao2(sistema, client)
                buildUp_tecnico.append(resposta)
                resposta2 = buildUp_semanal_sumarizacao2(resposta, client)
                buildUp_semanal.append(resposta2)
                resposta3 = resumo_executivo_sumarizacao2(resposta, client)
                resumo_executivo.append(resposta3)
        elif text["tipo"] == "PRODUÇÃO":
            logger.info(f'Iniciando processo tipo Produção.')
            group = organizar_estrutura(agrupar_similares(agrupar_sistemas(text)))
            for sistema in group['sistemas']:
                resposta = buildUp_tecnico_sumarizacao2(sistema, client)
                buildUp_tecnico.append(resposta)
                resposta2 = buildUp_semanal_sumarizacao2(resposta, client)
                buildUp_semanal.append(resposta2)
                resposta3 = resumo_executivo_sumarizacao3(resposta, client)
                resumo_executivo.append(resposta3)
        else:
            return "Tipo não encontrado. Tipos aceitos: ROM, Estéril e Produção."
            
        end = time.time()
        print(f"Tempo de execução: {(end - start) / 60:.2f}")
        
        resposta_completa["tipo"] = tipo
        resposta_completa["buildUp_tecnico"] = buildUp_tecnico
        resposta_completa["buildUp_semanal"] = buildUp_semanal
        resposta_completa["resumo_executivo"] = resumo_executivo
        logger.info("Processo concluido com sucesso!")
        
        return resposta_completa
        org = organizar_estrutura(agrupar_similares(agrupar_sistemas(text)))
        return org
    except Exception as e:
        logger.error(f'Erro na rota summarize_text: {e}')
        stack_trace = traceback.format_exc()
        logger.error(stack_trace)
        raise HTTPException(status_code=500, detail="Sistema indisponível no momento. Tente novamente em alguns minutos.")
    
def buildUp_tecnico_sumarizacao(data, client):
    logger.info(f'Iniciando processo de sumarização do buildUp técnico.')
    prompt = '''
    Como Entrada, você irá receber informações no formato JSON, que contém informações de sistemas da empresa Vale.
O JSON de Entrada terá o seguinte formato:
{
    "sistema": "Nome do sistema",
    "totalSistema": Saldo total do sistema,
    "equipamentos": [
        {
            "equipamento": "Nome do equipamento",
            "totalEquipamento": Saldo total do equipamento,
            "indicadores": [
                {
                    "tipo": "Nome do indicador",
                    "totalIndicador": Saldo total do indicador,
                    "detalhes": [
                        {
                            "causa": "Causa da falha",
                            "falha": "Falha especifica",
                            "EGP": "Código associado",
                            "observacao": "Observação sobre a falha e/ou a causa",
                            "perda": perda associada (o valor pode ser positivo ou negativo)
                        }
                    ]
                }
            ]
        }
    ]
}

Como Saida, preciso que você gere outro JSON no formato abaixo:
{
    "sistema": "Nome do sistema",
    "total_sistema": Saldo total do sistema,
    "equipamentos": [
        {
            "equipamento": "Nome do equipamento",
            "total_equipamento": Saldo total do equipamento,
            "indicadores": [
                {
                    "tipo": "Nome do indicador",
                    "total_indicador": Saldo total do indicador,
                    "detalhes": [
                        {
                            "saldo": Deverá ser o MESMO valor informado no JSON de Entrada no campo 'perda',
                            "justificativa": "Siga as instruções abaixo para preencher este campo."
                        }
                    ]
                }
            ]
        }
    ]
}

Instruções para gerar o campo JUSTIFICATIVA:
1. Analise os campos 'causa', 'falha' e 'observação' do json de entrada.
2. A partir da analise feita, gere uma frase coerente.
3. Você não deve preencher o campo 'justificativa' com os valores dos campos 'causa', 'falha', 'EGP' e 'observação' separados por ',' ou '-'.

Formato da resposta: Nunca coloque ```json ou ``` na sua resposta. Retorne apenas o JSON, nada mais deverá ser incluido.

JSON de Entrada:
{text}
    '''
    prompt = prompt.replace("{text}", str(data))
    exemplo = "{\"sistema\": \"S1SS\", \"total_sistema\": 330, \"equipamentos\": [{\"equipamento\": \"MSR-04\", \"total_equipamento\": 330, \"indicadores\": [{\"tipo\": \"UF\", \"total_indicador\": -150, \"detalhes\": [{\"saldo\": -60, \"justificativa\": \"Nível alto de pilha em função de super oferta de mina (-45Kt) e restrição no TCLD (-18Kt).\"}, {\"saldo\": -60, \"justificativa\": \"PC31 vazamento de óleo na transmissão, rompimento da mangueira hidráulica.\"}, {\"saldo\": -60, \"justificativa\": \"Limite de capacidade circ. posterior\"}, {\"saldo\": 30, \"justificativa\": \"Compensação por dias com maior ultilização física.\"}]}, {\"tipo\": \"DF\", \"total_categoria\": -20, \"detalhes\": [{\"saldo\": -60, \"justificativa\": \"Nível alto de pilha em função de super oferta de mina (-45Kt) e restrição no TCLD (-18Kt).\"}, {\"saldo\": 40, \"justificativa\": \"Compensação por dias com maior disponibilidade física.\"}]}, {\"tipo\": \"taxa\", \"total_categoria\": 500, \"detalhes\": [{\"saldo\": -60, \"justificativa\": \"Nível alto de pilha em função de super oferta de mina (-45Kt) e restrição no TCLD (-18Kt).\"}, {\"saldo\": 560, \"justificativa\": \"Compensação por dias com maior taxa horária.\"}]}]}]}"  
    response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "Atue como um Analista Sênior de Produção com especialização em Mineração da empresa brasileira Vale."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=3000,
            n=1,
            stop=None,
            temperature=0.01,
        ) 
        # Extrair o texto sumarizado da resposta
    summary = response.choices[0].message.content
    logger.info("BuildUp tecnico concluido com sucesso.")
    return format_response(summary)

def buildUp_tecnico_sumarizacao2(data, client):
    logger.info(f'Iniciando processo de sumarização do buildUp técnico.')
    prompt = '''
    Como Entrada, você irá receber informações no formato JSON, que contém informações de sistemas da empresa Vale.
O JSON de Entrada terá o seguinte formato:
{
    "sistema": "Nome do sistema",
    "totalSistema": Saldo total do sistema,
    "equipamentos": [
        {
            "equipamento": "Nome do equipamento",
            "totalEquipamento": Saldo total do equipamento,
            "indicadores": [
                {
                    "tipo": "Nome do indicador",
                    "totalIndicador": Saldo total do indicador,
                    "detalhes": [
                        {
                            "causa": "Causa da falha",
                            "falha": "Falha especifica",
                            "EGP": "Código associado",
                            "observacao": "Observação sobre a falha e/ou a causa",
                            "perda": perda associada (o valor pode ser positivo ou negativo)
                        }
                    ]
                }
            ]
        }
    ]
}

Como Saida, preciso que você gere outro JSON no formato abaixo:
{
    "sistema": "Nome do sistema",
    "total_sistema": Saldo total do sistema,
    "indicadores": [
        {
            "tipo": "Nome do indicador",
            "total_indicador": Saldo total do indicador,
            "detalhes": [
                {
                    "saldo": Deverá ser o MESMO valor informado no JSON de Entrada no campo 'perda',
                    "justificativa": "Siga as instruções abaixo para preencher este campo."
                }
            ]
        }
    ]
}

Instruções para gerar o campo JUSTIFICATIVA:
1. Analise os campos 'causa', 'falha' e 'observação' do json de entrada.
2. A partir da analise feita, gere uma frase coerente.
3. Você não deve preencher o campo 'justificativa' com os valores dos campos 'causa', 'falha', 'EGP' e 'observação' separados por ',' ou '-'.

Formato da resposta: Nunca coloque ```json ou ``` na sua resposta. Retorne apenas o JSON, nada mais deverá ser incluido.

JSON de Entrada:
{text}
    '''
    prompt = prompt.replace("{text}", str(data))
    exemplo = "{\"sistema\": \"S1SS\", \"total_sistema\": 330, \"equipamentos\": [{\"equipamento\": \"MSR-04\", \"total_equipamento\": 330, \"indicadores\": [{\"tipo\": \"UF\", \"total_indicador\": -150, \"detalhes\": [{\"saldo\": -60, \"justificativa\": \"Nível alto de pilha em função de super oferta de mina (-45Kt) e restrição no TCLD (-18Kt).\"}, {\"saldo\": -60, \"justificativa\": \"PC31 vazamento de óleo na transmissão, rompimento da mangueira hidráulica.\"}, {\"saldo\": -60, \"justificativa\": \"Limite de capacidade circ. posterior\"}, {\"saldo\": 30, \"justificativa\": \"Compensação por dias com maior ultilização física.\"}]}, {\"tipo\": \"DF\", \"total_categoria\": -20, \"detalhes\": [{\"saldo\": -60, \"justificativa\": \"Nível alto de pilha em função de super oferta de mina (-45Kt) e restrição no TCLD (-18Kt).\"}, {\"saldo\": 40, \"justificativa\": \"Compensação por dias com maior disponibilidade física.\"}]}, {\"tipo\": \"taxa\", \"total_categoria\": 500, \"detalhes\": [{\"saldo\": -60, \"justificativa\": \"Nível alto de pilha em função de super oferta de mina (-45Kt) e restrição no TCLD (-18Kt).\"}, {\"saldo\": 560, \"justificativa\": \"Compensação por dias com maior taxa horária.\"}]}]}]}"  
    response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "Atue como um Analista Sênior de Produção com especialização em Mineração da empresa brasileira Vale."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=3000,
            n=1,
            stop=None,
            temperature=0.01,
        ) 
        # Extrair o texto sumarizado da resposta
    summary = response.choices[0].message.content
    logger.info("BuildUp tecnico concluido com sucesso.")
    return format_response(summary)

def buildUp_semanal_sumarizacao(data, client):
    logger.info(f'Iniciando processo de sumarização do buildUp semanal.')
    exemplo = "{\"sistema\": \"S1SS\", \"buildUp\": \"**+711Kt MSAL1080KS04**\\n-**-113Kt Disponibilidade Física**\\n\\t-**-18Kt** Perda de interface com o COI devido a falha de comunicação no PLC e Remota do MSAL1080KS04.\\n\\t-**-16Kt** Falha de comunicação nos ativos de rede, incluindo falha no extrator de metais no TR1082KS02 e sensor de velocidade na TR1081KS-13.\\n\\t-**-14Kt** Reposicionamento do sensor de sub velocidade no alimentador SE1001-1002 devido a falha no sensor de velocidade no MSAL1080KS04.\\n\\t-**-11Kt** Reposição de cavalete durante manutenção corretiva na mesa de impacto lança descarga BM04.\\n\\t-**-10Kt** Troca de bateria LCT devido a falha no freio da elevação da EC53.\\n\\t-**-8Kt** Ajuste no pino da mola do freio devido a falha no freio da elevação da EC1013KS53.\\n\\t-**-8Kt** Ajuste e limpeza no sensor de sub velocidade SE1001,1002 no alimentador devido a falha no sensor de velocidade no MSAL1080KS04.\\n-**-94Kt Taxa Horária**\\n\\t-**-10Kt** Troca de bateria LCT devido a falha no freio da elevação da EC53.\\n\\t-**-8Kt** Ajuste no pino da mola do freio devido a falha no freio da elevação da EC1013KS53.\\n\\t-**-8Kt** Ajuste e limpeza no sensor de sub velocidade SE1001,1002 no alimentador devido a falha no sensor de velocidade no MSAL1080KS04.\\n\\t-**+120Kt** Compensação por dias com maior taxa horária.\n-**+824Kt Utilização Física**\\n\\t-**-7Kt** Mudança de corredor com inversão EC durante parada operacional no BM1080KS04.\\n\\t-**-7Kt** Troca de rolete na TR201104 devido a quebra do rolete de carga.\\n\\t-**-7Kt** Corte de borda na TR2011KS07 após atuação do rip cord.\\n\\t-**-8Kt** Locomoção e reposicionamento pós manutenção durante parada operacional no BM1080KS04.\\n\\t-**+853Kt** Compensação por dias com maior utilização física.\\n**-93Kt AL1080KS901**\\n-**-39Kt Disponibilidade Física**\\n\\t-**-23Kt** Troca de cabo do Feedback YS1080KS9003 devido a falha de contator trip no EX901.\\n\\t-**-14Kt** Ajuste no sensor do eletroímã no EX1080KS901.\\n-**+10Kt Taxa Horária**\\n\\t-**-20Kt** Acerto de praça devido a falta de alimentação no AL1080KS901.\\n\\t-**+10Kt** Compensação por dias com maior taxa horária.\n-**-54Kt Utilização Física**\n\t-**-6Kt** Acerto de praça devido a falta de alimentação no AL1080KS901.\\n\\t-**-8Kt** Revezamento e refeição do operador devido a falta de mão de obra no AL1080KS901.\\n\\t-**-8Kt** Parado para formação de pilha durante parada operacional no BM1080KS901.\\n\\t-**-11Kt** Limite de capacidade circ. posterior durante parada operacional no AL1080KS901.\\n\\t-**-12Kt** Vazamento de óleo na transmissão e rompimento da mangueira.\"}"  
    markdown = "**+711Kt MSAL1080KS04**\n-**-113Kt Disponibilidade Física**\n\t-**-18Kt** Perda de interface com o COI devido a falha de comunicação no PLC e Remota do MSAL1080KS04.\n\t-**-16Kt** Falha de comunicação nos ativos de rede, incluindo falha no extrator de metais no TR1082KS02 e sensor de velocidade na TR1081KS-13.\n\t-**-14Kt** Reposicionamento do sensor de sub velocidade no alimentador SE1001-1002 devido a falha no sensor de velocidade no MSAL1080KS04.\n\t-**-11Kt** Reposição de cavalete durante manutenção corretiva na mesa de impacto lança descarga BM04.\n\t-**-10Kt** Troca de bateria LCT devido a falha no freio da elevação da EC53.\n\t-**-8Kt** Ajuste no pino da mola do freio devido a falha no freio da elevação da EC1013KS53.\n\t-**-8Kt** Ajuste e limpeza no sensor de sub velocidade SE1001,1002 no alimentador devido a falha no sensor de velocidade no MSAL1080KS04.\n-**-94Kt Taxa Horária**\n\t-**-10Kt** Troca de bateria LCT devido a falha no freio da elevação da EC53.\n\t-**-8Kt** Ajuste no pino da mola do freio devido a falha no freio da elevação da EC1013KS53.\n\t-**-8Kt** Ajuste e limpeza no sensor de sub velocidade SE1001,1002 no alimentador devido a falha no sensor de velocidade no MSAL1080KS04.\n\t-**+120Kt** Compensação por dias com maior taxa horária.\n-**+824Kt Utilização Física**\n\t-**-7Kt** Mudança de corredor com inversão EC durante parada operacional no BM1080KS04.\n\t-**-7Kt** Troca de rolete na TR201104 devido a quebra do rolete de carga.\n\t-**-7Kt** Corte de borda na TR2011KS07 após atuação do rip cord.\n\t-**-8Kt** Locomoção e reposicionamento pós manutenção durante parada operacional no BM1080KS04.\n\t-**+853Kt** Compensação por dias com maior utilização física.\n**-93Kt AL1080KS901**\n-**-39Kt Disponibilidade Física**\n\t-**-23Kt** Troca de cabo do Feedback YS1080KS9003 devido a falha de contator trip no EX901.\n\t-**-14Kt** Ajuste no sensor do eletroímã no EX1080KS901.\n-**+10Kt Taxa Horária**\n\t-**-20Kt** Acerto de praça devido a falta de alimentação no AL1080KS901.\n\t-**+10Kt** Compensação por dias com maior taxa horária.\n-**-54Kt Utilização Física**\n\t-**-6Kt** Acerto de praça devido a falta de alimentação no AL1080KS901.\n\t-**-8Kt** Revezamento e refeição do operador devido a falta de mão de obra no AL1080KS901.\n\t-**-8Kt** Parado para formação de pilha durante parada operacional no BM1080KS901.\n\t-**-11Kt** Limite de capacidade circ. posterior durante parada operacional no AL1080KS901.\n\t-**-12Kt** Vazamento de óleo na transmissão e rompimento da mangueira."  
    
    response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "Atue como um Analista Sênior de Produção com especialização em Mineração da empresa brasileira Vale."},
                {"role": "user", "content": f"Você receberá um JSON com os campos 'sistema', 'equipamentos', 'equipamento', 'total_equipamento', 'indicadores', 'tipo' (do indicador), 'total_indicadores', 'detalhes', 'saldo' e 'justificativa'.\
                1. Exemplo de retorno: {exemplo}\
                2. Gere um markdown utilizando os campos do json de entrada.\
                3. Exemplo de markdown:{markdown}\
                4. No markdown de retorno, a ordem dos itens deve ser a mesma do JSON de entrada.\
                5. UF = Utilização Física, DF = Disponibilidade Física, e Taxa = Taxa Horária.\
                6. Se os valores dos campos 'total_equipamento', 'total_indicador' e 'saldo' forem positivos, coloque um '+' na frente do número.\
                7. O retorno deve ter apenas dois campos: 'sistema' (com o nome do sistema) e 'buildUp' (com o markdown).\
                8. (IMPORTANTE) Retorne apenas o JSON, nada mais deverá ser incluido. Nunca coloque ```json ou ``` na sua resposta.\
                A seguir está o json, com as informações citadas: {data}"}
            ],
            max_tokens=2000,
            n=1,
            stop=None,
            temperature=0.1,
        ) 
    summary = response.choices[0].message.content
    logger.info("BuildUp semanal concluido com sucesso.")
    return format_response(summary)

def buildUp_semanal_sumarizacao2(data, client):
    logger.info(f'Iniciando processo de sumarização do buildUp semanal.')
    exemplo = "{\"sistema\": \"S1SS\", \"buildUp\": \"-**-113Kt Disponibilidade Física**\\n\\t-**-18Kt** Perda de interface com o COI devido a falha de comunicação no PLC e Remota do MSAL1080KS04.\\n\\t-**-16Kt** Falha de comunicação nos ativos de rede, incluindo falha no extrator de metais no TR1082KS02 e sensor de velocidade na TR1081KS-13.\\n\\t-**-14Kt** Reposicionamento do sensor de sub velocidade no alimentador SE1001-1002 devido a falha no sensor de velocidade no MSAL1080KS04.\\n\\t-**-11Kt** Reposição de cavalete durante manutenção corretiva na mesa de impacto lança descarga BM04.\\n\\t-**-10Kt** Troca de bateria LCT devido a falha no freio da elevação da EC53.\\n\\t-**-8Kt** Ajuste no pino da mola do freio devido a falha no freio da elevação da EC1013KS53.\\n\\t-**-8Kt** Ajuste e limpeza no sensor de sub velocidade SE1001,1002 no alimentador devido a falha no sensor de velocidade no MSAL1080KS04.\\n-**-94Kt Taxa Horária**\\n\\t-**-10Kt** Troca de bateria LCT devido a falha no freio da elevação da EC53.\\n\\t-**-8Kt** Ajuste no pino da mola do freio devido a falha no freio da elevação da EC1013KS53.\\n\\t-**-8Kt** Ajuste e limpeza no sensor de sub velocidade SE1001,1002 no alimentador devido a falha no sensor de velocidade no MSAL1080KS04.\\n\\t-**+120Kt** Compensação por dias com maior taxa horária.\n-**+824Kt Utilização Física**\\n\\t-**-7Kt** Mudança de corredor com inversão EC durante parada operacional no BM1080KS04.\\n\\t-**-7Kt** Troca de rolete na TR201104 devido a quebra do rolete de carga.\\n\\t-**-7Kt** Corte de borda na TR2011KS07 após atuação do rip cord.\\n\\t-**-8Kt** Locomoção e reposicionamento pós manutenção durante parada operacional no BM1080KS04.\\n\\t-**+853Kt** Compensação por dias com maior utilização física.\"}"  
    markdown = "-**-113Kt Disponibilidade Física**\n\t-**-18Kt** Perda de interface com o COI devido a falha de comunicação no PLC e Remota do MSAL1080KS04.\n\t-**-16Kt** Falha de comunicação nos ativos de rede, incluindo falha no extrator de metais no TR1082KS02 e sensor de velocidade na TR1081KS-13.\n\t-**-14Kt** Reposicionamento do sensor de sub velocidade no alimentador SE1001-1002 devido a falha no sensor de velocidade no MSAL1080KS04.\n\t-**-11Kt** Reposição de cavalete durante manutenção corretiva na mesa de impacto lança descarga BM04.\n\t-**-10Kt** Troca de bateria LCT devido a falha no freio da elevação da EC53.\n\t-**-8Kt** Ajuste no pino da mola do freio devido a falha no freio da elevação da EC1013KS53.\n\t-**-8Kt** Ajuste e limpeza no sensor de sub velocidade SE1001,1002 no alimentador devido a falha no sensor de velocidade no MSAL1080KS04.\n-**-94Kt Taxa Horária**\n\t-**-10Kt** Troca de bateria LCT devido a falha no freio da elevação da EC53.\n\t-**-8Kt** Ajuste no pino da mola do freio devido a falha no freio da elevação da EC1013KS53.\n\t-**-8Kt** Ajuste e limpeza no sensor de sub velocidade SE1001,1002 no alimentador devido a falha no sensor de velocidade no MSAL1080KS04.\n\t-**+120Kt** Compensação por dias com maior taxa horária.\n-**+824Kt Utilização Física**\n\t-**-7Kt** Mudança de corredor com inversão EC durante parada operacional no BM1080KS04.\n\t-**-7Kt** Troca de rolete na TR201104 devido a quebra do rolete de carga.\n\t-**-7Kt** Corte de borda na TR2011KS07 após atuação do rip cord.\n\t-**-8Kt** Locomoção e reposicionamento pós manutenção durante parada operacional no BM1080KS04.\n\t-**+853Kt** Compensação por dias com maior utilização física."  
    
    response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "Atue como um Analista Sênior de Produção com especialização em Mineração da empresa brasileira Vale."},
                {"role": "user", "content": f"Você receberá um JSON com os campos 'sistema', 'equipamentos', 'equipamento', 'total_equipamento', 'indicadores', 'tipo' (do indicador), 'total_indicadores', 'detalhes', 'saldo' e 'justificativa'.\
                1. Exemplo de retorno: {exemplo}\
                2. Gere um markdown utilizando os campos do json de entrada.\
                3. Exemplo de markdown:{markdown}\
                4. No markdown de retorno, a ordem dos itens deve ser a mesma do JSON de entrada.\
                5. UF = Utilização Física, DF = Disponibilidade Física, e Taxa = Taxa Horária.\
                6. Se os valores dos campos 'total_equipamento', 'total_indicador' e 'saldo' forem positivos, coloque um '+' na frente do número.\
                7. O retorno deve ter apenas dois campos: 'sistema' (com o nome do sistema) e 'buildUp' (com o markdown).\
                8. (IMPORTANTE) Retorne apenas o JSON, nada mais deverá ser incluido. Nunca coloque ```json ou ``` na sua resposta.\
                A seguir está o json, com as informações citadas: {data}"}
            ],
            max_tokens=2000,
            n=1,
            stop=None,
            temperature=0.1,
        ) 
    summary = response.choices[0].message.content
    logger.info("BuildUp semanal concluido com sucesso.")
    return format_response(summary)

def resumo_executivo_sumarizacao(data, client):
    logger.info(f'Iniciando processo de sumarização do resumo executivo.')
    exemplo_resumo_executivo = "{\"sistema\": \"S3SS\", \"resumo\": \"**-253Kt** Despriorização do SLM-902 em função do nível baixo de pilha dos sistemas oriundo do backlog dos meses anteriores (impacto compensado pela melhor performance dos sistemas 1, 2 e 4).\"}"
    response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "Atue como um Analista Sênior de Produção com especialização em Mineração da empresa brasileira Vale."},
                {"role": "user", "content": f"Você receberá um JSON com os campos 'buildUp_tecnico', contendo uma lista de sistemas. O campo 'sistema' contém uma lista de equipamentos, com os campos 'equipamento', 'total_equipamento' (que representa a soma de todos os indicadores por equipamento), e uma lista de 'indicadores' contendo o 'tipo' ('DF', 'UF' ou 'taxa'), seus respectivos saldos e justificativas \
                Siga as seguintes instruções:\
	            1. Exemplo de retorno: {exemplo_resumo_executivo}\
                2. O json de retorno, deve conter duas chaves: 'sistema' e 'resumo'\
                3. Se a soma do total de todos os equipamentos do sistema resultar em um número maior que zero ou positivo, preencha o campo 'resumo' com ''\
                5. A chave 'sistema' deve conter o nome do sistema e a chave 'resumo' deve conter o resumo executivo com a maior perda do sistema resumida em uma única linha\
                6. O resumo deve começar com o valor do campo 'total_sistema', após esse número você deve fazer o resumo executivo apenas com a justificativa da maior perda.\
                7. (IMPORTANTE) Retorne apenas o JSON, nada mais deverá ser incluido. Nunca coloque ```json ou ``` na sua resposta.\
                A seguir está o json, com as informações citadas: {data}"}
            ],
            max_tokens=2000,
            n=1,
            stop=None,
            temperature=0.1,
        )
    summary = response.choices[0].message.content
    logger.info("Resumo executivo concluido com sucesso.")
    return format_response(summary)

def resumo_executivo_sumarizacao2(data, client):
    logger.info(f'Iniciando processo de sumarização do resumo executivo.')
    exemplo_resumo_executivo = {"sistema": "**5ª Britagem**", "resumo": "**-221Kt** Despriorização de estéril para recomposição do nível de pilha nos sistemas truckless (impacto parcialmente compensado pela melhor disponibilidade física e taxa horária do mês)"}
    response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "Atue como um Analista Sênior de Produção com especialização em Mineração da empresa brasileira Vale."},
                {"role": "user", "content": f"Você receberá um JSON com os campos 'sistema', uma lista de 'indicadores' contendo o 'tipo' ('DF', 'UF' ou 'taxa'), 'total_indicadores' contendo o saldo total do indicador, e uma lista de 'detalhes' com os respectivos saldos e justificativas.\
                Siga as seguintes instruções:\
	            1. Exemplo de retorno: {exemplo_resumo_executivo}\
                2. O json de retorno, deve conter as chaves: 'sistema' e 'resumo'\
                3. A chave 'sistema' deve conter o nome do sistema que teve a maior perda.\
                4. Você deve fazer o resumo executivo somente se o valor do campo 'total_indicadores' for negativo.\
                5. Para a chave 'resumo' você deverá fazer um resumo executivo citando o valor e justificativa do indicador com a maior perda negativa.\
                6. (IMPORTANTE) Retorne apenas o JSON, nada mais deverá ser incluido. Nunca coloque ```json ou ``` na sua resposta.\
                A seguir está o json, com as informações citadas: {data}"}
            ],
            max_tokens=2000,
            n=1,
            stop=None,
            temperature=0.1,
        )
    summary = response.choices[0].message.content
    logger.info("Resumo executivo concluido com sucesso.")
    return format_response(summary)

def resumo_executivo_sumarizacao3(data, client):
    logger.info(f'Iniciando processo de sumarização do resumo executivo.')
    exemplo_resumo_executivo = {"sistema": "**Disponibilidade Física:**", "resumo": "-**-144Kt** Atraso de MP da USA devido à quebra da base do mancal do dromo e da viga de sustentação da caixa do contrapeso da TR-2020KS-13\n-**-100Kt** Manutenções de oportunidade realizadas na USB e USA em cenário de baixa oferta de ROM"}
    response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "Atue como um Analista Sênior de Produção com especialização em Mineração da empresa brasileira Vale."},
                {"role": "user", "content": f"Você receberá um JSON com os campos 'sistema', uma lista de 'indicadores' contendo o 'tipo' ('DF', 'UF' ou 'taxa'), 'total_indicadores' contendo o saldo total do indicador, e uma lista de 'detalhes' com os respectivos saldos e justificativas.\
                Siga as seguintes instruções:\
	            1. Exemplo de retorno: {exemplo_resumo_executivo}\
                2. O json de retorno, deve conter duas chaves: 'sistema' e 'resumo'\
                3. A chave 'sistema' deve conter o nome do sistema que teve a maior perda.\
                4. UF = Utilização Física, DF = Disponibilidade Física, e Taxa = Taxa Horária.\
                5. Você deve fazer o resumo executivo somente se o valor do campo 'total_indicadores' for negativo.\
                6. O resumo deve começar com o nome do indicador que teve a maior perda, após isso você deve fazer o resumo executivo citando os valores e justificativas das duas maiores perdas do indicador.\
                7. (IMPORTANTE) Retorne apenas o JSON, nada mais deverá ser incluido. Nunca coloque ```json ou ``` na sua resposta.\
                A seguir está o json, com as informações citadas: {data}"}
            ],
            max_tokens=2000,
            n=1,
            stop=None,
            temperature=0.1,
        )
    summary = response.choices[0].message.content
    logger.info("Resumo executivo concluido com sucesso.")
    return format_response(summary)

def format_response(summary: str) -> dict:  
    try:  
        logger.info("Iniciando formatação da resposta")
        json_response = json.loads(summary) 
        logger.info("Formatação da resposta concluída com êxito") 
        return json_response  
    except json.JSONDecodeError as e: 
        logger.error(f'Erro ao formatar a resposta: {str(e)}') 
        raise HTTPException(status_code=500, detail=f"Failed to decode JSON response: {str(e)}") 
    
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
            if equipamento["saldoDF"] > 0:
                equipamento['HMC'] = []
            if equipamento["saldoUF"] > 0:
                equipamento['HOI'] = []
            if equipamento["saldoTaxa"] > 0:
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
        logger.info("Estrutura organizada com sucesso.") 
    return resultado