import requests
import json
import pandas as pd
import math
import concurrent.futures
from functools import partial
from tqdm import tqdm
import threading

# ==============================================================================
# --- CONFIGURAÇÃO  ---
# ==============================================================================

API_KEY = "" 
INPUT_EXCEL_FILE = "enderecos.xlsx"
OUTPUT_EXCEL_FILE = "resultados_rotas_ibge_session_detalhado.xlsx"
ORIGIN_ADDRESS = "Açailândia, Maranhão, Brazil"
MAX_WORKERS = 20
thread_local = threading.local()


# ==============================================================================
# --- FUNÇÕES AUXILIARES ---
# ==============================================================================

def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session

def get_city_name_from_ibge(ibge_code, session):
    ibge_api_url = f"https://servicodados.ibge.gov.br/api/v1/localidades/municipios/{ibge_code}"
    try:
        response = session.get(ibge_api_url, timeout=10)
        response.raise_for_status()
        data = response.json()
        city_name = data.get('nome')
        state_uf = data.get('microrregiao', {}).get('mesorregiao', {}).get('UF', {}).get('sigla')
        
        if city_name and state_uf:
            return f"{city_name}, {state_uf}, Brazil"
        else:
            return None
    except requests.exceptions.RequestException as e:
        print(f"ERRO DE CONEXÃO (IBGE) para '{ibge_code}': {e}")
        return None
    except json.JSONDecodeError:
        return None

def get_lat_lng(address, api_key, session):
    geocode_url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {"address": address, "key": api_key}
    
    try:
        response = session.get(geocode_url, params=params, timeout=10)
        response.raise_for_status()
        results = response.json().get('results', [])
        
        if results:
            location = results[0]['geometry']['location']
            return location['lat'], location['lng']
        else:
            return None, None
    except requests.exceptions.RequestException as e:
        print(f"ERRO DE CONEXÃO (Geocoding) para '{address}': {e}")
        return None, None

def format_duration(duration_str):
    if not duration_str: return "0h 0min"
    seconds = int(duration_str.replace('s', ''))
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    return f"{hours}h {minutes}min"

# ==============================================================================
# --- FUNÇÃO "WORKER"  ---
# ==============================================================================

def process_fiscal_code(fiscal_code, origin_lat, origin_lng, api_key):
    session = get_session()
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": "routes.distanceMeters,routes.duration,routes.travelAdvisory.tollInfo"
    }
    routes_url = "https://routes.googleapis.com/directions/v2:computeRoutes"
    
    try:
        parts = fiscal_code.strip().split()
        if len(parts) != 2: raise ValueError("Formato inválido")
        ibge_code = parts[1]
    except (ValueError, IndexError):
        return {"Domicilio Fiscal (Entrada)": fiscal_code, "Status": "Formato de entrada inválido"}

    resolved_address = get_city_name_from_ibge(ibge_code, session)
    if not resolved_address:
        return {"Domicilio Fiscal (Entrada)": fiscal_code, "Status": "Código IBGE não encontrado"}

    dest_lat, dest_lng = get_lat_lng(resolved_address, api_key, session)
    if not dest_lat or not dest_lng:
        return {"Domicilio Fiscal (Entrada)": fiscal_code, "Cidade Resolvida": resolved_address, "Status": "Erro na Geocodificação Google"}

    body = {
        "origin": {"location": {"latLng": {"latitude": origin_lat, "longitude": origin_lng}}},
        "destination": {"location": {"latLng": {"latitude": dest_lat, "longitude": dest_lng}}},
        "travelMode": "DRIVE",
        "routingPreference": "TRAFFIC_UNAWARE",
        "extraComputations": ["TOLLS"]
    }

    try:
        response = session.post(routes_url, headers=headers, json=body, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        route = data.get('routes', [{}])[0]
        result = {
            "Domicilio Fiscal (Entrada)": fiscal_code,
            "Cidade Resolvida": resolved_address.replace(", Brazil", ""),
            "Status": "Sucesso",
            "Distancia (km)": round(route.get('distanceMeters', 0) / 1000, 2),
            "Duracao Viagem": format_duration(route.get('duration')),
            "Custo Pedagio": float(route.get('travelAdvisory', {}).get('tollInfo', {}).get('estimatedPrice', [{}])[0].get('units', 0)),
            "Moeda Pedagio": route.get('travelAdvisory', {}).get('tollInfo', {}).get('estimatedPrice', [{}])[0].get('currencyCode', 'N/A')
        }
        return result

    except requests.exceptions.RequestException:
        return {"Domicilio Fiscal (Entrada)": fiscal_code, "Cidade Resolvida": resolved_address, "Status": "Erro de API de Rotas"}
    except (IndexError, KeyError):
        return {"Domicilio Fiscal (Entrada)": fiscal_code, "Cidade Resolvida": resolved_address, "Status": "Nenhuma rota encontrada"}

# ==============================================================================
# --- EXECUÇÃO PRINCIPAL  ---
# ==============================================================================

def main():
    print("Iniciando o processo de cálculo de rotas...")
    
    # --- ETAPA 1: Coordenadas da Origem ---
    print("\n[ETAPA 1/4] Buscando coordenadas da origem...")
    session = get_session()
    origin_lat, origin_lng = get_lat_lng(ORIGIN_ADDRESS, API_KEY, session)
    if not origin_lat or not origin_lng:
        print("ERRO FATAL: Não foi possível obter as coordenadas da origem. O script será encerrado.")
        return
    print(f"-> Coordenadas da origem encontradas: Lat={origin_lat}, Lng={origin_lng}")

    # --- ETAPA 2: Leitura do Arquivo ---
    print("\n[ETAPA 2/4] Lendo o arquivo de entrada...")
    try:
        df_destinations = pd.read_excel(INPUT_EXCEL_FILE, dtype=str)
        df_destinations.columns = df_destinations.columns.str.strip()
        
        if 'Domicilio Fiscal' not in df_destinations.columns:
            print(f"ERRO: O arquivo '{INPUT_EXCEL_FILE}' não contém uma coluna chamada 'Domicilio Fiscal'.")
            return
            
        destination_codes = df_destinations['Domicilio Fiscal'].dropna().tolist()
        print(f"-> {len(destination_codes)} domicílios fiscais carregados de '{INPUT_EXCEL_FILE}'.")
    
    except FileNotFoundError:
        print(f"ERRO FATAL: O arquivo de entrada '{INPUT_EXCEL_FILE}' não foi encontrado.")
        return
    except Exception as e:
        print(f"Ocorreu um erro inesperado ao ler o arquivo Excel: {e}")
        return

    # --- ETAPA 3: Processamento em Paralelo ---
    print("\n[ETAPA 3/4] Processando rotas em paralelo...")
    all_results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        worker_func = partial(process_fiscal_code, origin_lat=origin_lat, origin_lng=origin_lng, api_key=API_KEY)
        
        # Submete todas as tarefas e guarda os 'futures' em uma lista
        futures = [executor.submit(worker_func, code) for code in destination_codes]
        
        # Prepara a barra de progresso
        progress_bar = tqdm(total=len(destination_codes), desc="Iniciando...")
        
        # Processa os resultados à medida que ficam prontos (as_completed)
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            all_results.append(result)
            
            # Atualiza a descrição da barra com o item que acabou de ser processado
            processed_code = result.get("Domicilio Fiscal (Entrada)", "N/A")
            progress_bar.set_description(f"Processado: {processed_code}")
            
            # Atualiza o contador da barra
            progress_bar.update(1)
        
        progress_bar.close()

    # --- ETAPA 4: Salvando os Resultados ---
    if all_results:
        print("\n[ETAPA 4/4] Salvando resultados no arquivo Excel...")
        df_results = pd.DataFrame(all_results)
        df_results.to_excel(OUTPUT_EXCEL_FILE, index=False, engine='openpyxl')
        print(f"\n==========================================================")
        print(f"✅ Processo concluído! Os resultados foram salvos em '{OUTPUT_EXCEL_FILE}'.")
        print(f"==========================================================")
    else:
        print("\nNenhum domicílio foi processado.")

if __name__ == "__main__":
    main()