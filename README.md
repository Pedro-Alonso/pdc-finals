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

## Demos

### Eleição de Líder

```bash
python -m src.demo.demo_election
```

Demonstra 4 cenários: Chang-Roberts com todos os nós vivos, Bully com todos vivos,
falha do líder com re-eleição, e entrada de nó com maior prioridade.

### Ordenação de Mensagens

```bash
python -m src.demo.demo_ordering
```

Demonstra 3 tipos de ordenação: FIFO (mensagens entregues na ordem de envio por sender),
Causal (dependências causa-efeito preservadas via vector clocks) e Total (todos os nós
entregam na mesma ordem global via sequenciador).

### Exclusão Mútua

```bash
python -m src.demo.demo_mutex
```

Demonstra 2 algoritmos: Ricart-Agrawala (baseado em permissões, 2(N-1) mensagens por
entrada na CS) e Maekawa (baseado em quóruns, ~3√N mensagens por entrada na CS).

### Consenso e Tolerância a Falhas

```bash
python -m src.demo.demo_consensus
```

Demonstra 4 cenários: consenso com todos os nós vivos, consenso com 1 nó morto (maioria
decide), falha do líder com re-eleição e re-proposta, e detecção de falha/recuperação
por heartbeat.

**Parâmetros configuráveis** em `src/config.py`:
- `HEARTBEAT_INTERVAL` — intervalo entre heartbeats (padrão: 1.0s)
- `HEARTBEAT_TIMEOUT` — tempo sem resposta para considerar nó morto (padrão: 3.0s)

### Transações Distribuídas

```bash
python -m src.demo.demo_transactions
```

Demonstra 6 cenários: transação simples (commit), abort com rollback, prevenção de
Lost Update via S2PL, prevenção de Dirty Read, detecção e resolução de deadlock via
grafo espera-por (WFG), e transações aninhadas com herança de travas.

**Componentes**:
- **Lock Manager** — travas compartilhadas (S) e exclusivas (X) com fila de espera
- **Deadlock Detector** — grafo espera-por com detecção de ciclos via DFS
- **Concurrency Control** — Strict Two-Phase Locking (S2PL)
- **Transaction Manager** — transações ACID com write buffer e read-your-writes

### Recuperação de Falhas (WAL)

```bash
python -m src.demo.demo_recovery
```

Demonstra 3 cenários: WAL com commit normal (verifica entries BEGIN/WRITE/COMMIT),
crash antes do commit com undo (restaura before_values), e crash após commit com
redo (re-aplica after_values a partir do log).

**Componentes**:
- **Write-Ahead Log** — log persistente com fsync, formato `LSN|txn_id|type|resource|before|after|timestamp`
- **Recovery Manager** — recuperação em 3 fases (análise, redo, undo) inspirada em ARIES

### Commit Distribuído (2PC)

```bash
python -m src.demo.demo_2pc
```

Demonstra 3 cenários: 2PC com todos votando commit (GLOBAL_COMMIT), um participante
votando abort (GLOBAL_ABORT), e timeout de participante (GLOBAL_ABORT automático).

**Componentes**:
- **Two-Phase Coordinator** — gerencia fase de prepare e decisão, com retry de ACKs
- **Two-Phase Participant** — vota commit/abort, aplica decisão global

## Algoritmos Implementados

| Categoria | Algoritmo |
|-----------|-----------|
| Comunicação | Transporte TCP com protocolo JSON |
| Relógios | Relógio lógico de Lamport |
| Relógios | Relógio vetorial (Vector Clock) |
| Armazenamento | Memória compartilhada em arquivos texto |
| Eleição de Líder | Chang-Roberts (anel) |
| Eleição de Líder | Bully (prioridade) |
| Ordenação | FIFO ordering |
| Ordenação | Causal ordering (vector clocks) |
| Ordenação | Total ordering (sequenciador) |
| Exclusão Mútua | Ricart-Agrawala (permissões) |
| Exclusão Mútua | Maekawa (quóruns) |
| Consenso | Consenso baseado em líder (flooding) |
| Tolerância a Falhas | Detector de falhas por heartbeat |
| Transações | Gerenciador de transações ACID |
| Concorrência | Strict Two-Phase Locking (S2PL) |
| Travas | Lock Manager (S/X locks, herança) |
| Deadlock | Detecção via grafo espera-por (WFG) |
| Recuperação | Write-Ahead Log (WAL) com redo/undo |
| Recuperação | Recovery Manager (ARIES simplificado) |
| Commit Distribuído | Two-Phase Commit (2PC) |
