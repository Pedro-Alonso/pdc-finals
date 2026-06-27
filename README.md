# Memória Distribuída Compartilhada

Trabalho final da disciplina **Computação Distribuída e Paralela** (FCT Unesp).

- **Docente**: Prof. Dr. Ronaldo Toshiaki Oikawa
- **Discente**: Pedro Alonso Oliveira dos Santos
- **Curso**: Ciência da Computação — FCT Unesp

## Descrição

Sistema de memória compartilhada distribuída onde 3-4 processos Python se comunicam
via TCP sockets, compartilham registros armazenados em arquivos texto e implementam
algoritmos clássicos de coordenação, consenso, transações e controle de concorrência.

## Arquitetura

```
┌──────────┐    TCP/JSON    ┌──────────┐
│  Node 0  │◄──────────────►│  Node 1  │
│ :5000    │                │ :5001    │
└────┬─────┘                └────┬─────┘
     │                           │
     │    TCP/JSON    TCP/JSON   │
     │                           │
┌────┴─────┐                ┌────┴─────┐
│  Node 2  │◄──────────────►│  Node 3  │
│ :5002    │                │ :5003    │
└──────────┘                └──────────┘
```

Cada nó é um processo independente que:
- Escuta conexões TCP em sua porta designada
- Troca mensagens JSON com framing length-prefix (4 bytes big-endian)
- Mantém um relógio lógico de Lamport para ordenação de eventos
- Persiste registros em arquivos texto em `data/node_<id>/`

## Como Executar

### Pré-requisitos

- Python 3.10+

### Iniciar nós

Em terminais separados:

```bash
python -m src.node --id 0
python -m src.node --id 1
python -m src.node --id 2
python -m src.node --id 3
```

### Testes

```bash
python -m venv .venv
.venv/Scripts/activate   # Windows
# source .venv/bin/activate  # Linux/macOS
pip install -r requirements.txt
pytest tests/ -v
```

## Algoritmos Implementados

| Categoria | Algoritmo | Status |
|-----------|-----------|--------|
| Comunicação | Transporte TCP com protocolo JSON | Implementado |
| Relógios | Relógio lógico de Lamport | Implementado |
| Armazenamento | Memória compartilhada em arquivos texto | Implementado |
