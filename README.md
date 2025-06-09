# 📦 Grain Logistic - Pipeline de Dados

Projeto de engenharia de dados desenvolvido para processar e transformar os dados logísticos da Grain Logistic. Utiliza o ecossistema Spark/Delta Lake em Databricks, aplicando boas práticas de ingestão, validação, modelagem dimensional e geração de features históricas.

---

## ℹ️ Disclaimer Inicial

Este projeto foi desenvolvido e testado na **Databricks Community Edition**, o que impõe algumas limitações importantes. Alguns recursos que **não puderam ser utilizados ou foram simulados** incluem:

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


---

## 📁 Estrutura do Projeto

```
.
├── bronze/
│   └── 01\_ingestao\_bronze.py
├── silver/
│   └── 02\_transformacoes\_silver.py
├── gold/
│   ├── 03\_dim\_uf.py
│   ├── 04\_dim\_calendario.py
│   ├── 05\_dim\_importancia.py
│   └── 06\_fato\_venda.py
├── feature\_store/
│   └── 07\_fs\_historico\_envios\_corredor.py
├── qualidade/
│   ├── define\_schema.py
│   ├── valida\_integridade.py
│   └── valida\_presenca\_coluna.py
├── testes/
│   ├── 00-run\_all.py
│   ├── 01-teste\_schemas.py
│   ├── 02-teste\_integracao\_fato.py
│   └── 03-teste\_nulos\_duplicados.py
├── 00-orquestrador.py
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

---

## 📄 Licença

Distribuído sob a licença MIT. Veja `LICENSE` para mais detalhes.

---

## ✍️ Autor

Renan Gonzales  
Engenheiro de Dados
