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
- Mantém relógios lógicos (Lamport + Vetorial) para ordenação de eventos
- Persiste registros em arquivos texto em `data/node_<id>/`

## Algoritmos Implementados

| # | Categoria | Algoritmo |
|---|-----------|-----------|
| 1 | Eleição de Líder | Chang-Roberts (anel) |
| 2 | Eleição de Líder | Bully (prioridade) |
| 3 | Ordenação | FIFO ordering |
| 4 | Ordenação | Causal ordering (vector clocks) |
| 5 | Ordenação | Total ordering (sequenciador) |
| 6 | Exclusão Mútua | Ricart-Agrawala (permissões) |
| 7 | Exclusão Mútua | Maekawa (quóruns) |
| 8 | Consenso | Consenso baseado em líder |
| 9 | Tolerância a Falhas | Detector de falhas por heartbeat |
| 10 | Transações | Gerenciador de transações ACID |
| 11 | Concorrência | Strict Two-Phase Locking (S2PL) |
| 12 | Travas | Lock Manager (S/X locks, herança) |
| 13 | Deadlock | Detecção via grafo espera-por (WFG) |
| 14 | Recuperação | Write-Ahead Log (WAL) com redo/undo |
| 15 | Recuperação | Recovery Manager (ARIES simplificado) |
| 16 | Commit Distribuído | Two-Phase Commit (2PC) |

## Como Executar

### Pré-requisitos

- Python 3.10+
- pytest (apenas para testes)

### Instalação

```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
# Linux/macOS:
# source .venv/bin/activate
pip install -r requirements.txt
```

### Iniciar nós individualmente

Em terminais separados:

```bash
python -m src.node --id 0
python -m src.node --id 1
python -m src.node --id 2
python -m src.node --id 3
```

### Demo completo (todas as funcionalidades)

```bash
python -m src.demo.full_demo
```

Exercita as 10 partes do enunciado em sequência: inicialização, eleição, ordenação,
exclusão mútua, consenso, transações, deadlock, recuperação e 2PC.

### Demos individuais

```bash
python -m src.demo.demo_election       # Eleição de líder
python -m src.demo.demo_ordering        # Ordenação de mensagens
python -m src.demo.demo_mutex           # Exclusão mútua
python -m src.demo.demo_consensus       # Consenso e tolerância a falhas
python -m src.demo.demo_transactions    # Transações, concorrência, deadlock
python -m src.demo.demo_recovery        # Recuperação de falhas (WAL)
python -m src.demo.demo_2pc             # Two-Phase Commit
```

### Testes

```bash
pytest tests/ -v
```

## Estrutura do Projeto

```
src/
  node.py              — ponto de entrada de cada nó
  config.py            — configuração do cluster (IDs, portas, topologia)
  network/
    transport.py       — TCP connections, send/receive, framing
    message.py         — tipos de mensagem, serialização JSON
    clock.py           — Lamport clock + Vector clock
    ordering.py        — FIFO, Causal, Total ordering
  election/
    chang_roberts.py   — eleição em anel (Chang-Roberts)
    bully.py           — algoritmo Valentão (Bully)
  mutex/
    ricart_agrawala.py — exclusão mútua (Ricart-Agrawala)
    maekawa.py         — exclusão mútua com quóruns (Maekawa)
  consensus/
    consensus.py       — protocolo de consenso baseado em líder
    failure_detector.py — detecção de falhas por heartbeat
  storage/
    shared_memory.py   — leitura/escrita de registros em arquivos texto
    transaction.py     — gerenciador de transações (ACID)
    lock_manager.py    — travas compartilhadas (S) e exclusivas (X)
    concurrency.py     — controle de concorrência (Strict 2PL)
    deadlock.py        — grafo espera-por, detecção de ciclos
  recovery/
    wal.py             — write-ahead log persistente
    recovery.py        — recuperação redo/undo após falha
  commit/
    two_phase.py       — protocolo 2PC (coordenador/participante)
  demo/
    full_demo.py       — demo integrado (todas as funcionalidades)
    demo_election.py   — demo de eleição de líder
    demo_ordering.py   — demo de ordenação de mensagens
    demo_mutex.py      — demo de exclusão mútua
    demo_consensus.py  — demo de consenso
    demo_transactions.py — demo de transações e deadlock
    demo_recovery.py   — demo de recuperação (WAL)
    demo_2pc.py        — demo de Two-Phase Commit
tests/                 — testes unitários e de integração (pytest)
data/                  — criado em runtime, um subdiretório por nó
```
