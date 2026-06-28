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

## Algoritmos Implementados

| Categoria | Algoritmo | Status |
|-----------|-----------|--------|
| Comunicação | Transporte TCP com protocolo JSON | Implementado |
| Relógios | Relógio lógico de Lamport | Implementado |
| Relógios | Relógio vetorial (Vector Clock) | Implementado |
| Armazenamento | Memória compartilhada em arquivos texto | Implementado |
| Eleição de Líder | Chang-Roberts (anel) | Implementado |
| Eleição de Líder | Bully (prioridade) | Implementado |
| Ordenação | FIFO ordering | Implementado |
| Ordenação | Causal ordering (vector clocks) | Implementado |
| Ordenação | Total ordering (sequenciador) | Implementado |
| Exclusão Mútua | Ricart-Agrawala (permissões) | Implementado |
| Exclusão Mútua | Maekawa (quóruns) | Implementado |
| Consenso | Consenso baseado em líder (flooding) | Implementado |
| Tolerância a Falhas | Detector de falhas por heartbeat | Implementado |
| Transações | Gerenciador de transações ACID | Implementado |
| Concorrência | Strict Two-Phase Locking (S2PL) | Implementado |
| Travas | Lock Manager (S/X locks, herança) | Implementado |
| Deadlock | Detecção via grafo espera-por (WFG) | Implementado |
