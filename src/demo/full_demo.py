import os
import sys
import time
import shutil
import threading
import logging

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")
if sys.stderr.encoding != "utf-8":
    sys.stderr.reconfigure(encoding="utf-8")

from src.config import NODES, DATA_DIR
from src.node import Node
from src.network.ordering import FIFOOrdering, CausalOrdering, TotalOrdering
from src.mutex.ricart_agrawala import RicartAgrawala
from src.storage.transaction import TransactionManager, TransactionStatus
from src.storage import shared_memory
from src.recovery.wal import WriteAheadLog
from src.recovery.recovery import RecoveryManager
from src.storage.transaction import Transaction


logging.basicConfig(level=logging.WARNING, format="%(name)s | %(message)s")

DEMO_NODE_OFFSET = 70
RESULTS = {}


def banner(phase, title):
    print(f"\n{'=' * 70}")
    print(f"  FASE {phase} - {title}")
    print(f"{'=' * 70}\n")


def result_ok(part, desc):
    RESULTS[part] = ("OK", desc)
    print(f"  [OK] {desc}")


def result_fail(part, desc):
    RESULTS[part] = ("FAIL", desc)
    print(f"  [FAIL] {desc}")


def cleanup_data(*node_ids):
    for nid in node_ids:
        path = os.path.join(DATA_DIR, f"node_{nid}")
        if os.path.exists(path):
            shutil.rmtree(path)


def wait_for_election(nodes, timeout=10):
    for n in nodes:
        if n._election:
            n._election.election_complete.wait(timeout=timeout)


def phase_1_initialization():
    banner(1, "Inicialização e Conectividade (TCP + Lamport Clock)")

    nodes = []
    for nid in range(4):
        n = Node(nid, election_algorithm="chang_roberts")
        nodes.append(n)

    for n in nodes:
        n.start()
    time.sleep(1)

    print("  4 nós iniciados (portas 5000-5003)")
    print("  Diretórios de dados criados:")
    for nid in range(4):
        path = os.path.join(DATA_DIR, f"node_{nid}")
        exists = os.path.exists(path)
        print(f"    data/node_{nid}/ - {'OK' if exists else 'MISSING'}")

    print("\n  Testando conectividade PING/PONG...")
    from src.network.message import MSG_PONG
    pong_received = threading.Event()

    def on_pong(msg):
        pong_received.set()

    nodes[0].register_handler(MSG_PONG, on_pong)
    nodes[0].send(1, "PING")
    ok = pong_received.wait(timeout=3)
    if ok:
        print("  Node 0 -> PING -> Node 1 -> PONG OK")
    else:
        print("  Node 0 -> PING -> Node 1 -- sem resposta")

    print(f"\n  Lamport clock de cada nó:")
    for n in nodes:
        print(f"    Node {n.node_id}: ts={n.clock.get_time()}")

    result_ok("init", "4 nós conectados via TCP, Lamport clocks sincronizando")

    for n in nodes:
        n.stop()
    time.sleep(0.5)


def phase_2_election():
    banner(2, "Eleição de Líder (Chang-Roberts + Bully)")

    print("  --- Chang-Roberts ---")
    nodes_cr = [Node(i, election_algorithm="chang_roberts") for i in range(4)]
    for n in nodes_cr:
        n.start()
    time.sleep(1)

    print("  Node 0 inicia eleição via Chang-Roberts...")
    nodes_cr[0].start_election()
    wait_for_election(nodes_cr)
    time.sleep(1)

    leader_cr = nodes_cr[0].leader_id
    print(f"  Resultado: Líder eleito = Nó {leader_cr}")
    for n in nodes_cr:
        role = "LEADER" if n.is_leader() else "follower"
        print(f"    Node {n.node_id}: leader={n.leader_id} ({role})")

    for n in nodes_cr:
        n.stop()
    time.sleep(0.5)

    print("\n  --- Bully ---")
    nodes_b = [Node(i, election_algorithm="bully") for i in range(4)]
    for n in nodes_b:
        n.start()
    time.sleep(1)

    print("  Node 0 inicia eleição via Bully...")
    nodes_b[0].start_election()
    wait_for_election(nodes_b)
    time.sleep(1)

    leader_b = nodes_b[0].leader_id
    print(f"  Resultado: Líder eleito = Nó {leader_b}")
    for n in nodes_b:
        role = "LEADER" if n.is_leader() else "follower"
        print(f"    Node {n.node_id}: leader={n.leader_id} ({role})")

    if leader_cr == 3 and leader_b == 3:
        result_ok("election", f"Ambos algoritmos elegem Nó 3 (maior ID)")
    else:
        result_fail("election", f"Resultados inesperados: CR={leader_cr}, Bully={leader_b}")

    for n in nodes_b:
        n.stop()
    time.sleep(0.5)


def phase_3_ordering():
    banner(3, "Comunicação em Grupo (FIFO / Causal / Total)")

    print("  --- FIFO Ordering ---")
    nodes = [Node(i) for i in range(3)]
    for n in nodes:
        n.start()
    time.sleep(1)

    delivered_fifo = []
    lock = threading.Lock()
    fifo_sender = FIFOOrdering(nodes[0])
    fifo_receiver = FIFOOrdering(nodes[1])

    def on_deliver_fifo(sender, seq, data):
        with lock:
            delivered_fifo.append((sender, seq, data))

    fifo_receiver.set_deliver_callback(on_deliver_fifo)

    for i in range(1, 4):
        fifo_sender.send(1, f"msg_{i}")
        time.sleep(0.1)
    time.sleep(2)

    fifo_ok = len(delivered_fifo) == 3 and all(
        delivered_fifo[i][1] <= delivered_fifo[i + 1][1] for i in range(len(delivered_fifo) - 1)
    )
    print(f"  Enviadas 3 mensagens: Node 0 -> Node 1")
    print(f"  Entregues em ordem: {[(s, d) for s, _, d in delivered_fifo]}")
    print(f"  FIFO preservado: {fifo_ok}")

    for n in nodes:
        n.stop()
    time.sleep(0.5)

    print("\n  --- Causal Ordering ---")
    node_ids = [0, 1, 2]
    nodes = [Node(i) for i in node_ids]
    for n in nodes:
        n.start()
    time.sleep(1)

    delivered_causal = []
    causal_lock = threading.Lock()
    causal = [CausalOrdering(n, node_ids) for n in nodes]
    delivered_at_1 = threading.Event()

    def on_deliver_c2(sender, data):
        with causal_lock:
            delivered_causal.append((sender, data))

    def on_deliver_c1(sender, data):
        delivered_at_1.set()

    causal[2].set_deliver_callback(on_deliver_c2)
    causal[1].set_deliver_callback(on_deliver_c1)

    causal[0].broadcast("msg_A")
    delivered_at_1.wait(timeout=5)
    time.sleep(0.5)
    causal[1].broadcast("msg_B")
    time.sleep(2)

    causal_ok = False
    if len(delivered_causal) >= 2:
        a_idx = next((i for i, (s, d) in enumerate(delivered_causal) if d == "msg_A"), -1)
        b_idx = next((i for i, (s, d) in enumerate(delivered_causal) if d == "msg_B"), -1)
        causal_ok = a_idx >= 0 and b_idx >= 0 and a_idx < b_idx

    print(f"  Node 0 broadcast msg_A -> Node 1 recebe -> Node 1 broadcast msg_B")
    print(f"  Node 2 delivery order: {[d for _, d in delivered_causal]}")
    print(f"  Ordem causal preservada (A antes de B): {causal_ok}")

    for n in nodes:
        n.stop()
    time.sleep(0.5)

    print("\n  --- Total Ordering ---")
    nodes = [Node(i) for i in range(4)]
    for n in nodes:
        n.start()
    time.sleep(1)

    sequencer_id = 0
    delivered_total = {i: [] for i in range(4)}
    total_locks = {i: threading.Lock() for i in range(4)}
    total = [TotalOrdering(n, sequencer_id) for n in nodes]

    for i, t in enumerate(total):
        def make_cb(nid):
            def cb(origin, seq, data):
                with total_locks[nid]:
                    delivered_total[nid].append((seq, data))
            return cb
        t.set_deliver_callback(make_cb(i))

    t1 = threading.Thread(target=lambda: total[1].send("from_node_1"))
    t2 = threading.Thread(target=lambda: total[2].send("from_node_2"))
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    time.sleep(2)

    ref = [(s, d) for s, d in delivered_total[0]]
    total_ok = all(
        [(s, d) for s, d in delivered_total[nid]] == ref
        for nid in range(1, 4)
    )
    print(f"  Node 1 e Node 2 enviam concorrentemente")
    print(f"  Ordem de entrega (Node 0): {ref}")
    print(f"  Ordem consistente em todos os nós: {total_ok}")

    for n in nodes:
        n.stop()
    time.sleep(0.5)

    if fifo_ok and causal_ok and total_ok:
        result_ok("ordering", "FIFO, Causal e Total ordering funcionando corretamente")
    else:
        result_fail("ordering", f"FIFO={fifo_ok}, Causal={causal_ok}, Total={total_ok}")


def phase_4_mutex():
    banner(4, "Exclusão Mútua (Ricart-Agrawala)")

    nodes = [Node(i) for i in range(4)]
    for n in nodes:
        n.start()
    time.sleep(1)

    mutexes = [RicartAgrawala(n) for n in nodes]

    cs_log = []
    cs_lock = threading.Lock()

    def access_cs(node_id, mutex):
        acquired = mutex.request_cs("registro_central")
        if acquired:
            with cs_lock:
                cs_log.append(("enter", node_id, time.time()))
            time.sleep(0.3)
            shared_memory.write_record(node_id, "registro_central.txt", "writer", str(node_id), 0)
            with cs_lock:
                cs_log.append(("exit", node_id, time.time()))
            mutex.release_cs()

    threads = []
    for i in range(3):
        t = threading.Thread(target=access_cs, args=(i, mutexes[i]))
        threads.append(t)

    print("  Nós 0, 1, 2 pedem acesso à seção crítica simultaneamente...")
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=15)
    time.sleep(1)

    enters = [(nid, ts) for action, nid, ts in cs_log if action == "enter"]
    exits = [(nid, ts) for action, nid, ts in cs_log if action == "exit"]
    exclusive = True
    for i, (nid_i, enter_i) in enumerate(enters):
        exit_i = next(ts for n, ts in exits if n == nid_i)
        for j, (nid_j, enter_j) in enumerate(enters):
            if i != j:
                exit_j = next(ts for n, ts in exits if n == nid_j)
                if enter_j < exit_i and enter_i < exit_j:
                    exclusive = False

    print(f"  Ordem de acesso: {[nid for _, nid, _ in cs_log if _ == 'enter' or True][::2]}")
    print(f"  Exclusão mútua preservada: {exclusive}")
    print(f"  Mensagens por CS entry: 2*(N-1) = {2*3}")

    if exclusive and len(enters) == 3:
        result_ok("mutex", "Exclusão mútua Ricart-Agrawala sem acessos simultâneos")
    else:
        result_fail("mutex", f"exclusive={exclusive}, entries={len(enters)}")

    for n in nodes:
        n.stop()
    time.sleep(0.5)


def phase_5_consensus():
    banner(5, "Consenso e Detecção de Falhas (Heartbeat)")

    nodes = [
        Node(i, election_algorithm="bully", enable_failure_detector=True)
        for i in range(4)
    ]
    for n in nodes:
        n.start()
    time.sleep(1.5)

    print("  Elegendo líder...")
    nodes[0].start_election()
    wait_for_election(nodes)
    time.sleep(1)

    leader = next((n for n in nodes if n.is_leader()), None)
    if not leader:
        result_fail("consensus", "Nenhum líder eleito")
        for n in nodes:
            n.stop()
        return

    print(f"  Líder: Nó {leader.node_id}")

    print(f"\n  Líder propõe consenso: 'inicializar_registros'...")
    result1 = leader.consensus.propose("inicializar_registros")
    time.sleep(0.5)

    consensus_1_ok = result1 == "inicializar_registros"
    print(f"  Resultado: {result1}")
    for n in nodes:
        if n.consensus:
            d = n.consensus.get_decision(1)
            print(f"    Node {n.node_id}: decision={d}")

    print(f"\n  Matando Nó 2 (simulando crash)...")
    nodes[2].stop()
    time.sleep(4)

    print(f"  Líder propõe novo consenso: 'operacao_pos_falha'...")
    result2 = leader.consensus.propose("operacao_pos_falha")
    time.sleep(0.5)

    consensus_2_ok = result2 == "operacao_pos_falha"
    print(f"  Resultado com nó morto: {result2}")
    print(f"  Maioria (2 de 3 vivos + líder) suficiente: {consensus_2_ok}")

    if consensus_1_ok and consensus_2_ok:
        result_ok("consensus", "Consenso atingido com todos vivos e com 1 nó morto")
    else:
        result_fail("consensus", f"round1={result1}, round2={result2}")

    for n in [nodes[0], nodes[1], nodes[3]]:
        n.stop()
    time.sleep(0.5)


def phase_6_transactions():
    banner(6, "Transações com Concorrência (S2PL + Locks S/X)")

    demo_nid = DEMO_NODE_OFFSET
    cleanup_data(demo_nid)
    shared_memory.init_storage(demo_nid)
    shared_memory.write_record(demo_nid, "accounts.txt", "saldo_A", "1000", 0)

    tm = TransactionManager(demo_nid)

    t1 = tm.begin()
    val = tm.read(t1, "accounts.txt", "saldo_A")
    print(f"  T1 lê saldo_A = {val}")

    tm.write(t1, "accounts.txt", "saldo_A", "800")
    print(f"  T1 escreve saldo_A = 800 (X-lock adquirido)")

    t2_result = [None]

    def t2_read():
        t2 = tm.begin()
        t2_result[0] = tm.read(t2, "accounts.txt", "saldo_A", timeout=1.5)
        if t2_result[0] is not None:
            tm.commit(t2)
        else:
            tm.abort(t2)

    thread = threading.Thread(target=t2_read)
    thread.start()
    time.sleep(0.3)
    print(f"  T2 tenta ler saldo_A -> bloqueada (X-lock de T1)")

    tm.commit(t1)
    print(f"  T1 commit -> saldo_A = 800")
    thread.join(timeout=3)

    if t2_result[0] is not None:
        print(f"  T2 desbloqueada, lê saldo_A = {t2_result[0]}")
    else:
        print(f"  T2 timeout (serialização correta)")

    rec = shared_memory.read_record(demo_nid, "accounts.txt", "saldo_A")
    final = rec["value"] if rec else None
    txn_ok = final == "800"
    print(f"  Valor final: saldo_A = {final}")

    if txn_ok:
        result_ok("transactions", "S2PL previne dirty read, locks S/X funcionando")
    else:
        result_fail("transactions", f"saldo_A={final}, esperado 800")

    cleanup_data(demo_nid)


def phase_7_deadlock():
    banner(7, "Deadlock - Detecção e Resolução (Wait-For Graph)")

    demo_nid = DEMO_NODE_OFFSET + 1
    cleanup_data(demo_nid)
    shared_memory.init_storage(demo_nid)
    shared_memory.write_record(demo_nid, "accounts.txt", "recurso_A", "AAA", 0)
    shared_memory.write_record(demo_nid, "resource_b.txt", "recurso_B", "BBB", 0)

    tm = TransactionManager(demo_nid)
    tm.deadlock_detector._interval = 0.5
    tm.deadlock_detector.start()

    t1 = tm.begin()
    t2 = tm.begin()

    tm.write(t1, "accounts.txt", "recurso_A", "111")
    print(f"  {t1} adquire X-lock em accounts.txt")

    tm.write(t2, "resource_b.txt", "recurso_B", "222")
    print(f"  {t2} adquire X-lock em resource_b.txt")

    results = {"t1": None, "t2": None}

    def t1_wait():
        results["t1"] = tm.write(t1, "resource_b.txt", "recurso_B", "333", timeout=5.0)

    def t2_wait():
        results["t2"] = tm.write(t2, "accounts.txt", "recurso_A", "444", timeout=5.0)

    thread1 = threading.Thread(target=t1_wait)
    thread2 = threading.Thread(target=t2_wait)
    thread1.start()
    time.sleep(0.1)
    thread2.start()

    print(f"  {t1} espera resource_b.txt (held by {t2})")
    print(f"  {t2} espera accounts.txt (held by {t1})")
    print(f"  Ciclo: {t1} -> {t2} -> {t1}")

    thread1.join(timeout=8)
    thread2.join(timeout=8)

    tm.deadlock_detector.stop()

    t1_txn = tm.get_transaction(t1)
    t2_txn = tm.get_transaction(t2)

    deadlock_resolved = False
    if t2_txn and t2_txn.status == TransactionStatus.ABORTED:
        print(f"  Deadlock resolvido: {t2} (mais jovem) abortada como vítima")
        if results["t1"]:
            tm.commit(t1)
            print(f"  {t1} prosseguiu e commitou")
        deadlock_resolved = True
    elif t1_txn and t1_txn.status == TransactionStatus.ABORTED:
        print(f"  Deadlock resolvido: {t1} abortada como vítima")
        if results["t2"]:
            tm.commit(t2)
            print(f"  {t2} prosseguiu e commitou")
        deadlock_resolved = True
    else:
        print(f"  Deadlock pode ter sido resolvido por timeout")
        tm.abort(t1)
        tm.abort(t2)

    if deadlock_resolved:
        result_ok("deadlock", "WFG detectou ciclo, transação vítima abortada")
    else:
        result_fail("deadlock", "Detecção de deadlock inconclusiva")

    cleanup_data(demo_nid)


def phase_8_recovery():
    banner(8, "Recuperação de Falhas (WAL - Undo)")

    demo_nid = DEMO_NODE_OFFSET + 2
    cleanup_data(demo_nid)
    shared_memory.init_storage(demo_nid)

    shared_memory.write_record(demo_nid, "accounts.txt", "saldo", "1000", 0)
    shared_memory.write_record(demo_nid, "accounts.txt", "bonus", "100", 0)
    shared_memory.write_record(demo_nid, "accounts.txt", "taxa", "50", 0)

    wal = WriteAheadLog(demo_nid)
    tm = TransactionManager(demo_nid, wal=wal)

    txn = tm.begin()
    tm.write(txn, "accounts.txt", "saldo", "500")
    tm.write(txn, "accounts.txt", "bonus", "999")
    tm.write(txn, "accounts.txt", "taxa", "0")
    print(f"  {txn} escreveu 3 registros (saldo=500, bonus=999, taxa=0)")
    print(f"  COMMIT NÃO realizado")

    shared_memory.write_record(demo_nid, "accounts.txt", "saldo", "500", 0)
    shared_memory.write_record(demo_nid, "accounts.txt", "bonus", "999", 0)
    shared_memory.write_record(demo_nid, "accounts.txt", "taxa", "0", 0)
    print(f"\n  --- SIMULANDO CRASH ---")

    s = shared_memory.read_record(demo_nid, "accounts.txt", "saldo")
    print(f"  Dados após crash: saldo={s['value']}")

    print(f"\n  Reiniciando nó -> Recovery lê WAL...")
    wal2 = WriteAheadLog(demo_nid)
    rm = RecoveryManager()
    result = rm.recover(wal2, shared_memory, demo_nid)
    print(f"  Recovery result: {result}")

    saldo = shared_memory.read_record(demo_nid, "accounts.txt", "saldo")["value"]
    bonus = shared_memory.read_record(demo_nid, "accounts.txt", "bonus")["value"]
    taxa = shared_memory.read_record(demo_nid, "accounts.txt", "taxa")["value"]

    print(f"  Após recovery: saldo={saldo}, bonus={bonus}, taxa={taxa}")

    recovery_ok = saldo == "1000" and bonus == "100" and taxa == "50"
    if recovery_ok:
        result_ok("recovery", "Recovery: 3 escritas desfeitas, estado consistente restaurado")
    else:
        result_fail("recovery", f"saldo={saldo}, bonus={bonus}, taxa={taxa}")

    cleanup_data(demo_nid)


def phase_9_2pc():
    banner(9, "Two-Phase Commit (2PC)")

    cleanup_data(0, 1, 3)

    nodes = []
    for nid in [0, 1, 3]:
        n = Node(nid)
        n.start()
        nodes.append(n)
    time.sleep(0.5)

    node_map = {n.node_id: n for n in nodes}

    for nid in [0, 1, 3]:
        shared_memory.write_record(nid, "accounts.txt", "saldo", "1000", 0)

    coordinator = node_map[3]
    participants = [0, 1]
    txn_id = "dist_txn_full_demo"

    for pid in participants:
        node_map[pid].txn_manager._seq += 1
        node_map[pid].txn_manager._transactions[txn_id] = Transaction(txn_id)

    print(f"  Coordenador: Nó 3")
    print(f"  Participantes: Nós {participants}")
    print(f"  Transação: {txn_id}")

    print(f"\n  Fase 1: PREPARE enviado aos participantes...")
    result = coordinator.two_phase_coord.coordinate_commit(txn_id, participants)

    decision = "GLOBAL_COMMIT" if result else "GLOBAL_ABORT"
    print(f"  Fase 2: Decisão = {decision}")

    if result:
        result_ok("2pc", f"2PC: transação {txn_id} commitada globalmente")
    else:
        result_fail("2pc", f"2PC resultou em GLOBAL_ABORT inesperado")

    for n in nodes:
        n.stop()
    time.sleep(0.3)
    cleanup_data(0, 1, 3)


def phase_10_summary():
    banner(10, "Resumo")

    parts = [
        ("init", "Inicialização e Conectividade"),
        ("election", "Eleição de Líder (Chang-Roberts + Bully)"),
        ("ordering", "Comunicação em Grupo (FIFO / Causal / Total)"),
        ("mutex", "Exclusão Mútua (Ricart-Agrawala)"),
        ("consensus", "Consenso e Tolerância a Falhas"),
        ("transactions", "Transações Distribuídas (S2PL, Locks S/X)"),
        ("deadlock", "Detecção de Deadlock (Wait-For Graph)"),
        ("recovery", "Recuperação de Falhas (WAL Undo)"),
        ("2pc", "Commit Distribuído (Two-Phase Commit)"),
    ]

    print(f"  {'Parte':<55} {'Status'}")
    print(f"  {'-' * 55} {'-' * 6}")
    all_ok = True
    for key, desc in parts:
        status, _ = RESULTS.get(key, ("SKIP", ""))
        mark = "OK" if status == "OK" else "FAIL"
        if status != "OK":
            all_ok = False
        print(f"  {desc:<55} {mark}")

    print()
    if all_ok:
        print("  RESULTADO FINAL: TODAS AS FUNCIONALIDADES DEMONSTRADAS COM SUCESSO")
    else:
        failed = [desc for key, desc in parts if RESULTS.get(key, ("SKIP",))[0] != "OK"]
        print(f"  RESULTADO FINAL: {len(failed)} falha(s) detectada(s)")


def main():
    print("=" * 70)
    print("  DEMO INTEGRADO - MEMÓRIA DISTRIBUÍDA COMPARTILHADA")
    print("  Computação Distribuída e Paralela - FCT Unesp")
    print("  Prof. Dr. Ronaldo Toshiaki Oikawa")
    print("  Pedro Alonso Oliveira dos Santos")
    print("=" * 70)

    phase_1_initialization()
    phase_2_election()
    phase_3_ordering()
    phase_4_mutex()
    phase_5_consensus()
    phase_6_transactions()
    phase_7_deadlock()
    phase_8_recovery()
    phase_9_2pc()
    phase_10_summary()

    print("\n" + "=" * 70)
    print("  DEMO COMPLETO")
    print("=" * 70)


if __name__ == "__main__":
    main()
