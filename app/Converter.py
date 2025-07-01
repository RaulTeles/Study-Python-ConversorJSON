import pandas as pd
import json
from datetime import datetime, time

def default_converter(o):
    if isinstance(o, datetime):
        return o.isoformat()
    elif isinstance(o, time):
        return o.strftime('%H:%M:%S')
    raise TypeError(f'Object of type {o.__class__.__name__} is not JSON serializable')

def xlsm_to_json(xlsm_file_path, sheet_name, json_file_path):
    # Carregar a planilha específica do arquivo XLSM com limitação de colunas e linhas
    xlsm_data = pd.read_excel(xlsm_file_path, sheet_name=sheet_name, engine='openpyxl', usecols='A:T', nrows=26, dtype=str)

    # Remover a segunda linha (índice 1)
    xlsm_data.drop(index=0, inplace=True)
    
    # Reiniciar o índice do DataFrame
    xlsm_data.reset_index(drop=True, inplace=True)
    
    # Exibir as primeiras linhas do DataFrame para inspeção
    #print(xlsm_data.head())
    
    # Preencher os valores NaN com uma string vazia para evitar problemas de serialização
    xlsm_data.fillna('', inplace=True)
    
    # Identificação das colunas relevantes
    colunas_usina_i = ['Usina', 'USINA I', 'Unnamed: 2', 'Unnamed: 3']
    colunas_usina_ii = ['Usina', 'USINA II', 'Unnamed: 6', 'Unnamed: 7']
    colunas_usina_iii = ['Usina', 'USINA III', 'Unnamed: 10', 'Unnamed: 11']
    colunas_usina_iv = ['Usina', 'USINA IV', 'Unnamed: 14', 'Unnamed: 15']
    colunas_usina_v = ['Usina', 'USINA V', 'Unnamed: 18', 'Unnamed: 19']
    
    usina_data = []
    
    # Processar dados para cada usina
    def processar_usina(colunas, usina_nome):
        usina_dados = []
        todas_linhas_vazias = True  # Flag para verificar se todas as linhas estão vazias
        for _, row in xlsm_data.iterrows():
            if row[colunas[2]] != '' or row[colunas[3]] != '':
                todas_linhas_vazias = False  # Se encontrar qualquer linha não vazia, definir como False
                data = row[colunas[0]]
                if isinstance(data, str):
                    try:
                        # Tenta converter a string para um objeto datetime e formata para mm/dd/aaaa
                        data = datetime.strptime(data, '%Y-%m-%d %H:%M:%S').strftime('%m/%d/%Y')
                    except ValueError:
                        # Se a conversão falhar, mantenha o valor original ou ajuste conforme necessário
                        data = data
                elif isinstance(data, pd.Timestamp):
                    # Se já for um Timestamp, formata para mm/dd/aaaa
                    data = data.strftime('%m/%d/%Y')
                hora = row[colunas[1]]
                if isinstance(hora, str) and 'h' not in hora:
                    hora = f"{hora}h"
                quantidade = int(float(row[colunas[2]]))
                justificativa = row[colunas[3]]
                if isinstance(justificativa, str):
                    justificativa = justificativa.replace("'", "")
                usina_dados.append({
                    'data': data,
                    'hora': hora,
                    'quantidade': quantidade,
                    'justificativa': justificativa
                })
        
        if todas_linhas_vazias:
            usina_dados = []
        
        usina_data.append({
            'usina': usina_nome,
            'total_produzido': 0,
            'eventos': usina_dados
        })

    processar_usina(colunas_usina_i, 'USINA I')
    processar_usina(colunas_usina_ii, 'USINA II')
    processar_usina(colunas_usina_iii, 'USINA III')
    processar_usina(colunas_usina_iv, 'USINA IV')
    processar_usina(colunas_usina_v, 'USINA V')
    
    resultado = {'regiao': 'Serra Norte',
                 'total_usinas': 0,
                 'usinas': usina_data}
     
    # Salvar o dicionário em um arquivo JSON
    with open(json_file_path, 'w', encoding='utf-8') as json_file:
        json.dump(resultado, json_file, indent=4, ensure_ascii=False, default=default_converter)
        
    print(f"Arquivo JSON salvo em: {json_file_path}")


# Exemplo de uso
xlsm_file_path = 'c:\\Users\\j.mendes.das.neves\\Desktop\\Studies\\Python\\xlsm to json\\excel\\15-08-2024-Informações COI - Usina.xlsm'
sheet_name = 'JUSTIFICATIVA DE USINA'
json_file_path = 'c:\\Users\\j.mendes.das.neves\\Desktop\\Studies\\Python\\xlsm to json\\Json\\15-08-2024-Informações COI - Usina.json'
xlsm_to_json(xlsm_file_path, sheet_name, json_file_path)