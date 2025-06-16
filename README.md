# 📦 Grain Logistic - Pipeline de Dados

Projeto de engenharia de dados desenvolvido para processar e transformar os dados logísticos da Grain Logistic. Utiliza o ecossistema Spark/Delta Lake em Databricks, aplicando boas práticas de ingestão, validação, modelagem dimensional e geração de features históricas.

---

## ℹ️ Disclaimer Inicial

Este projeto foi inicialmente desenvolvido e testado na **Databricks Community Edition**, o que impunha algumas limitações importantes. Alguns recursos que **não puderam ser utilizados ou foram simulados** incluem:

- ❌ **Unity Catalog**  
  Recursos de governança, ACLs (Access Control Lists) e centralização de permissões não estão disponíveis. Toda a gestão de permissões e catálogos foi omitida.

- ❌ **VACUUM com retenção mínima personalizada**  
  A execução de `VACUUM` com retenção inferior a 7 dias está bloqueada, dificultando a remoção imediata de arquivos obsoletos após sobrescritas.

- ❌ **Monitoramento de Jobs (Job UI)**  
  A criação de jobs agendados e monitoramento de execuções em painel dedicado não está disponível. A orquestração foi realizada por notebooks encadeados via `%run`.

- ❌ **Delta Live Tables (DLT)**  
  A criação de pipelines declarativos com validação contínua e versionamento automático de tabelas não está disponível.

- ❌ **Auto Loader (cloudFiles)**  
  A ingestão incremental automática de arquivos (sem necessidade de controle de checkpoint manual) foi substituída por `spark.read.csv`.

- ❌ **Dashboards interativos e widgets HTML**  
  A criação de dashboards e visualizações interativas diretamente nos notebooks foi evitada em razão das restrições de UI da Community Edition.

Apesar dessas limitações, o projeto foi estruturado de forma que possa ser facilmente adaptado para ambientes Databricks Premium ou Enterprise.

Posteriormente, o projeto foi adaptado para o Databricks Free, onde algumas dessas limitações foram superadas, permitindo uma execução mais robusta e eficiente. Arquivos com o pós-fixo `db_free`, na pasta de cada camada, indicam as adaptações feitas para essa versão.


---

## 📁 Estrutura do Projeto

```
.
├── bronze/
│   └── 01_ingestao_bronze.py
│   └── 01_ingestao_bronze_db_free.py
├── silver/
│   └── 02_transformacoes_silver.py
│   └── 02_transformacoes_silver_db_free.py
├── gold/
│   ├── 03_dim_uf.py
│   ├── 04_dim_calendario.py
│   ├── 05_dim_importancia.py
│   └── 06_fato_venda.py
│   ├── 03_dim_uf_db_free.py
│   ├── 04_dim_calendario_db_free.py
│   ├── 05_dim_importancia_db_free.py
│   └── 06_fato_venda_db_free.py
├── feature_store/
│   └── 07_fs_historico_envios_corredor.py
│   └── 07_fs_historico_envios_corredor_db_free.py
├── qualidade/
│   ├── define_schema.py
│   ├── documentacao_colunas.py
│   ├── valida_integridade.py
│   └── valida_presenca_coluna.py
├── testes/
│   ├── 00-run_all.py
│   ├── 01-teste_schemas.py
│   ├── 02-teste_integracao_fato.py
│   └── 03-teste_nulos_duplicados.py
├── 00-orquestrador.py
├── pipeline_db_free.yaml
├── relatorio_grain_logistics.pbix
├── LICENSE
└── README.md
```

---

## 🚀 Pipeline por Camadas

### 🔹 Bronze

- **Script:** `01_ingestao_bronze.py`
- Leitura do arquivo `grain_logistic_shipping.csv`
- Tipagem forçada para `StringType` (evitando perda de informação)
- Adição de metadados (usuário, modo de ingestão, timestamp)
- Escrita em Delta Lake particionada por `data_ingestao`
- Otimização com `OPTIMIZE` e `ZORDER BY`

---

### 🔸 Silver

- **Script:** `02_transformacoes_silver.py`
- Padronização de colunas
- Transformação em 3 tabelas:
  - `envios`: informações logísticas e temporais
  - `produto`: preço, peso, desconto
  - `cliente`: ligações, gênero, avaliações
- Aplicação de validações e definição de schema

---

### 🟡 Gold

- **Dimensões**
  - `dim_uf`: unidade federativa e sigla
  - `dim_calendario`: data completa, dia da semana, mês, ano
  - `dim_importancia`: grau de urgência (low, medium, high)
- **Fato**
  - `fato_venda`: métricas da venda (receita, tempo de entrega, peso total)
  - Particionada por `id_uf` e otimizada com `ZORDER`

---

### 🧠 Feature Store

- **Script:** `07_fs_historico_envios_corredor.py`
- Geração de histórico acumulado diário por corredor logístico
- Métricas: volume de vendas, tempo médio de entrega, avaliações, gênero
- Particionamento por `ds_corredor_de_armazenagem`
- Otimização com `ZORDER`

---

## ✅ Validações

- **Presença de colunas esperadas**: `valida_presenca_coluna.py`
- **Integridade de dados**: `valida_integridade.py`
- **Definição padronizada de schema**: `define_schema.py`

---

## 🧪 Testes

Localizados na pasta `testes/`:

- `01-teste_schemas.py`: Validação de schemas das tabelas
- `02-teste_integracao_fato.py`: Verificação de integridade na `fato_venda`
- `03-teste_nulos_duplicados.py`: Checagem de valores nulos e duplicados
- `00-run_all.py`: Execução centralizada dos testes

---

## 🧭 Orquestração

- **Script:** `00-orquestrador.py`
  - Executa todos os notebooks da pipeline com ordem de dependência entre camadas
- **YAML:** `pipeline_db_free.yaml`
  - Configuração para execução no Databricks Free
  - Define caminhos, tabelas e parâmetros de cada camada

---

## 📊 Relatório

Foi desenvolvido um dashboard em Power BI para visualização e análise dos dados processados pelo pipeline. O relatório consolida as principais métricas operacionais e comerciais da Grain Logistic, com filtros interativos por ano, UF e grau de importância da entrega.

Principais indicadores presentes no dashboard:

✅ Entrega no Prazo (%): Percentual de pacotes que chegaram dentro do prazo estipulado.
📍 Mapa de calor por UF: Visualiza a performance de entrega pontual por estado.
📈 Receita x Mês: Evolução mensal da receita, segmentada pelo nível de urgência do pedido.
🧭 Year to Date (YTD): Comparativo da receita acumulada no ano atual e no ano anterior.
⚖️ Peso Total por Ano: Volume total movimentado em toneladas, ano a ano.
⏱️ Média de Dias para Entrega: Tempo médio de entrega por estado (UF).

O arquivo `.pbix` se encontra no repositório com o nome `relatorio_grain_logistics.pbix`, podendo ser customizado ou conectado diretamente ao Lakehouse via Databricks Connector.

---

## 📄 Licença

Distribuído sob a licença MIT. Veja `LICENSE` para mais detalhes.

---

## ✍️ Autor

Renan Gonzales  
Engenheiro de Dados
