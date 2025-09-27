# ✈️ Air Traffic Data Pipeline & Dashboard

## Contexto e Motivação

Entre 2020 e 2022, a aviação mundial passou por um período atípico por causa da **pandemia de COVID-19**.  

Houve **queda drástica na quantidade de voos em 2020**, restrições de fronteiras, mudanças frequentes nas regras sanitárias e um processo de **retomada gradual** em 2021 e 2022.  
Para compreender o impacto dessas variações e apoiar análises de recuperação do setor, foi necessário trabalhar com uma base massiva de voos e aeroportos.

O conjunto de dados disponível possuía **milhões de registros**, mas estava **sujo e incompleto** para análise direta.  

O objetivo foi construir um **pipeline de transformação de dados**, capaz de gerar informações  para um **dashboard interativo em Streamlit**, permitindo entender padrões antes, durante e após a crise sanitária.

## Principais desafios encontrados

- **Dados brutos inconsistentes:** diferentes fontes usavam códigos ICAO sem padronização e nomes de empresas com problemas de acentuação (ex.: _Gol Transportes AŽreos_).
    
- **Impacto da pandemia:** quedas abruptas e retomadas exigiam datas confiáveis e comparáveis para análise de tendências.
    
- **Integração de aeroportos:** era preciso cruzar dados de voos com bases de aeroportos (nomes, países, municípios, coordenadas) sem duplicar colunas.
    
- **Volume de dados:** mais de 2 milhões de registros, tornando a representação no **StreamLit** dificultoso.
    
- **Fusos horários e horários de voos:** datas vinham como strings, sem timezone correto; análises de atrasos e sazonalidade dependiam de conversões confiáveis para **GMT-3 (São Paulo)**.

## Soluções adotadas

### 🚀 Utilização **Polars**

- O Polars substituiu o pandas, trazendo **processamento em paralelo e muito mais rápido**.
- Funções como `select`, `with_columns`, `rename`, `join` e `unique` foram amplamente usadas para manipular milhões de linhas.

### 🐍 Criação de uma de Transformação
```python
class Transformer:

    def __init__(self):
        pass

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        df = self._map_rows(df)
        df = self._remove_invalid_fligths(df)
        df = self._is_late(df)
        df = self._drop_unused_columns(df)
        df = self._normalize_dates(df)

        return df
```

### 💥 Remoção de Voos Inválidos

- Criado filtros de voos inválidos ou que não foram efetuados dentro de solo brasileiro (**outliers**), para manter o dataset limpo:
```python
def _remove_null_fligths(self, df: pl.DataFrame) -> pl.DataFrame:

	df = df.filter(
		(pl.col("Situação Voo").is_not_null()) &
		(pl.col("Situação Voo") == "REALIZADO")
		)
	df = df.filter([
		((pl.col("Partida Real").is_not_null()) &
		(pl.col("Chegada Real").is_not_null())),
		((pl.col("Partida Prevista").is_not_null()) &
		(pl.col("Chegada Prevista").is_not_null()))
		])

	return df
        
def _filter_brazil_only(self, df: pl.DataFrame) -> pl.DataFrame:
	df = df = df.filter(
		(pl.col("Destino País ISO").is_not_null()) &
		(pl.col("Destino País ISO") == "BR")
		)
	df = df.filter(
		(pl.col("Origem País ISO").is_not_null()) &
		(pl.col("Origem País ISO") == "BR")
		)

	return df

```

### 🛫 Tradução de códigos

- Criação de **JSONs atualizados** com códigos ICAO e nomes corretos de aeroportos e linhas aéreas:
```plaintext
|-📂json
	|-📃airline-types.json
	|-📃airlines-codes.json
	|-📃airport-codes.json
	|-📃justification-codes.json
```

- Juntamente criado **funções para mapeamento** dos mesmos:
```python
class Transformer:
	def _map_rows(self, df: pl.DataFrame) -> pl.DataFrame:
	        df = self._set_airports_names(df)
	        df = self._map_justification_codes(df)
	        df = self._map_airlines_types(df)
	        df = self._map_airlines_codes(df)
	        return df
```

### 🛬 Enriquecimento com dados de aeroportos

- Join com base de aeroportos para trazer **nome, município, país, continente, coordenadas e código GPS**:
```python
class Transformer:
	def _set_airports_names(self, df: pl.DataFrame) -> pl.DataFrame:
		airports = load_json_file("app/docs/json/airport-codes.json")
			df_airports = (
				pl.from_records(airports, infer_schema_length=10000)
				.select([
					pl.col("icao_code").cast(pl.Utf8),
					pl.col("name").cast(pl.Utf8),
					pl.col("continent").cast(pl.Utf8),
					pl.col("iso_country").cast(pl.Utf8),
					pl.col("municipality").cast(pl.Utf8),
					pl.col("gps_code").cast(pl.Utf8),
					pl.col("coordinates").cast(pl.Utf8),
					pl.col("type").cast(pl.Utf8),
				])
				.unique(subset=["icao_code"], keep="first")
			)
		
			# ---- Join para ORIGEM ----
			df = df.join(
				df_airports.rename({
					"icao_code": "ICAO Aeródromo Origem",
					"name": "Aeródromo Origem",
					"continent": "Origem Continente",
					"iso_country": "Origem País ISO",
					"municipality": "Origem Município",
					"gps_code": "Origem GPS",
					"coordinates": "Origem Coordenadas",
					"type": "Tamanho Origem"
				}),
				on="ICAO Aeródromo Origem",
				how="left",
			)
			
			# ---- Join para DESTINO ----
			df = df.join(
				df_airports.rename({
					"icao_code": "ICAO Aeródromo Destino",
					"name": "Aeródromo Destino",
					"continent": "Destino Continente",
					"iso_country": "Destino País ISO",
					"municipality": "Destino Município",
					"gps_code": "Destino GPS",
					"coordinates": "Destino Coordenadas",
					"type": "Tamanho Destino"
				}),
				on="ICAO Aeródromo Destino",
				how="left",
			)
```

### ⏱️ Conversão de datas para **GMT-3**

- Conversão de colunas de string para datetime com fuso horário correto:
```python
class Transformer:

	def _normalize_dates(self, df: pl.DataFrame) -> pl.DataFrame:
	
		date_cols = [c for c in df.columns if c.startswith(("Partida Prevista", "Partida Real", "Chegada Prevista", "Chegada Real"))]
	
		df = df.with_columns([
			pl.col(c)
			.str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S", strict=False)
			.dt.replace_time_zone("America/Sao_Paulo")
			for c in date_cols
		])
	
		return df
```
- Fundamental para comparar períodos de **restrição de voos (2020)** com a **recuperação em 2021–2022**.
    

### ⚡ Otimização para dashboards

- Transformações pesadas foram pré-processadas, evitando recálculos ao vivo.
- O Streamlit recebeu dados já limpos, permitindo filtros rápidos mesmo com grande volume.

### Impacto da pandemia no projeto

- **Queda de tráfego em 2020:** os dados mostraram um número muito menor de voos.
    
- **Retomada irregular em 2021 e 2022:** a necessidade de ter datas precisas e fusos corretos foi essencial para identificar a recuperação gradual das companhias e dos aeroportos.
    
- **Mudança de malha aérea:** algumas rotas e aeroportos foram suspensos ou retomaram em momentos diferentes.

### Resultados alcançados

- **Base padronizada**: companhias e aeroportos com nomes corretos, coordenadas e metadados completos.
- **Performance de nível empresarial**: processamento rápido mesmo com milhões de linhas, graças ao Polars.
- **Dashboard fluido e responsivo**: filtros e análises temporais funcionam em tempo real.
- **Insights claros sobre a pandemia**: comparação antes, durante e depois do COVID-19, evidenciando recuperação do setor aéreo.

### Lições aprendidas

- **Planejar tipos de dados e timezone é crítico** para análises temporais em crises globais como a pandemia.
- **Polars entrega performance e escalabilidade**, mas exige atenção a tipos de colunas e joins.
- Manter arquivos auxiliares de mapeamento (como o JSON de companhias) facilita manutenção contínua.
- **Pré-processar e limpar dados** antes de enviar para visualização garante **boa experiência para o usuário final**.


## 📖 Sobre o projeto

Este projeto realiza **extração, transformação e análise de dados de voos e aeroportos**, com foco em:

- **Padronização de companhias aéreas** via códigos ICAO.
- **Enriquecimento de dados de aeroportos** com nomes, coordenadas, município, país e continente.
- **Conversão de datas para fuso horário GMT-3 (America/Sao_Paulo)** para análises temporais confiáveis.
- **Visualização interativa** através de um dashboard em **Streamlit**.

O período analisado inclui os anos de **2020, 2021 e 2022**, destacando o impacto da **pandemia de COVID-19** sobre o tráfego aéreo e a recuperação gradual do setor.

---

## 🚀 Principais recursos

- ⚡ **Polars** para processamento rápido de milhões de linhas.
- 🛫 Mapeamento padronizado de companhias aéreas com suporte a acentuação correta.
- 🛬 Integração com base de aeroportos (ICAO, nome, país, município, coordenadas).
- ⏱️ Conversão de datas e horários para GMT-3.
- 📊 Dashboard interativo com filtros, gráficos e análise temporal.
- 🦠 Contexto COVID-19 incluído para análises antes, durante e depois da pandemia.

---

## 🗂️ Estrutura de diretórios
```
├── app/
│ ├── data/ # Dados brutos e transformados
│ ├── docs/json/ # Arquivo JSON com mapeamento ICAO de companhias
│ ├── dashboard/ # Código do dashboard Streamlit
│ ├── model/ # Transformações de dados em Polars
│ └── utils/ # Funções auxiliares (leitura, timezone, etc.)
├── requirements.txt
├── README.md
└── main.py # Script principal para execução local
```

---

## ⚙️ Instalação e execução

### 1. Instalar UV

https://docs.astral.sh/uv/getting-started/installation

```bash
#Windows (Powershell)
powershell -c "irm https://astral.sh/uv/install.ps1 | more"

#MacOs & Linux
curl -LsSf https://astral.sh/uv/install.sh | less
```

### 2. Clonar o repositório
```bash
git clone https://github.com/seu-usuario/air-traffic-dashboard.git
cd air-traffic-dashboard
```

### 3. Criar ambiente virtual
```bash
uv venv
source .venv/bin/activate   # Linux/Mac
# ou
.venv\Scripts\activate      # Windows
```

### 4. Instalar dependências
```bash
uv sync
```

### 5. Executar o pipeline de transformação
```bash
uv run main.py
```

### 6. Iniciar o dashboard interativo
```bash
#Caminho absoluto main.py
streamlit run {CAMINHO}\{DO}\{PROJETO}\airports-flights-analysis\main.py
```
O dashboard ficará disponível em "http://localhost:8501" 
