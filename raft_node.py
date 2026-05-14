import Pyro5.api
import Pyro5.configure
import sys
import threading
import time
import random

Pyro5.configure.COMMTIMEOUT = 0.2

@Pyro5.api.expose
class RaftNode:
    def __init__(self, node_id):
        self.node_id = node_id
        self.my_uri = f"PYRO:node{node_id}@localhost:{9000 + node_id}"
        self.state = "follower"
        self.current_term = 0
        self.voted_for = None
        self.log = []
        self.commit_index = 0

        self.peers = {
            1: "PYRO:node1@localhost:9001",
            2: "PYRO:node2@localhost:9002",
            3: "PYRO:node3@localhost:9003",
            4: "PYRO:node4@localhost:9004",
        }
        del self.peers[node_id]

        self.last_heartbeat = time.time()
        self.heartbeat_timeout = random.uniform(1.5, 3.0)

        self.election_timeout = 1.0
        
        self.lock = threading.Lock()
        threading.Thread(target=self.election_timer, daemon=True).start()
        threading.Thread(target=self.heartbeat_timer, daemon=True).start()

    def election_timer(self):
        while True:
            time.sleep(0.1)
            with self.lock:
                if self.state != "leader":
                    delta_time = time.time() - self.last_heartbeat
                    if delta_time >= self.heartbeat_timeout:
                        print(f"\nLíder morreu, iniciando eleição...")
                        self.last_heartbeat = time.time()
                        threading.Thread(target=self.start_election, daemon=True).start() 
        
    def heartbeat_timer(self):
        while True:
            time.sleep(1)
            if self.state == "leader":
                self.broadcast_append_entries()

    def start_election(self):
        with self.lock:
            self.state = "candidate"
            self.current_term += 1
            self.voted_for = self.node_id
            self.last_heartbeat = time.time()
            self.election_timeout = 1.0 

            term = self.current_term
            print(f"Iniciando eleição para o {term}o termo como candidato {self.node_id}...")

        votes = 1
        for peer_uri in self.peers.values():
            try:
                with Pyro5.api.Proxy(peer_uri) as peer:
                    vote_granted = peer.request_vote(term, self.node_id)
                    if vote_granted:
                        votes += 1
            except Exception:
                pass
        with self.lock:
            if self.state == "candidate" and self.current_term == term and votes >= 3:
                print(f"\n ###### Venceu eleição com {votes} votos no {term}o termo ######")
                self.state = "leader"
                self.voted_for = None
                self.register_leader_ns()
                self.broadcast_append_entries() 

    def register_leader_ns(self):
        try:
            ns = Pyro5.api.locate_ns()
            ns.register("leader", self.my_uri)
            print("Atualizado registro de líderes no Servidor de Nomes")
        except Exception as e:
            print(f"Falha no servidor de nomes Erro: {e}")

    def request_vote(self, term, candidate_id):
        with self.lock:
            if term > self.current_term:
                self.current_term = term
                self.state = "follower"
                self.voted_for = None
            
            if term == self.current_term and (self.voted_for is None or self.voted_for == candidate_id):
                self.voted_for = candidate_id
                self.last_heartbeat = time.time()
                print(f"Votação:voto em nó_{candidate_id} para o {term}o termo")
                return True
            return False

    def append_entries(self, term, leader_id, entries, leader_commit):
        with self.lock:
            if term < self.current_term:
                return False
            
            self.current_term = term
            self.state = "follower"
            self.last_heartbeat = time.time()
            self.voted_for = None

            if entries:
                for entry in entries:
                    self.log.append(entry)
                    print(f"Replicação: entrada adicionada ao log: {entry}")

            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, len(self.log))
                print(f"índice commitado: {self.commit_index}")

            return True
    
    def broadcast_append_entries(self, new_entry=None):
        entry = [new_entry] if new_entry else []
        success_count = 1
        
        for peer_id, peer_uri in self.peers.items():
            try:
                with Pyro5.api.Proxy(peer_uri) as peer:
                    success = peer.append_entries(self.current_term, self.node_id, entry, self.commit_index)
                    if success and new_entry:
                        success_count += 1
            except Exception:
                continue

        if new_entry and success_count >= 3:
            with self.lock:
                self.commit_index += 1
                print(f"MAIORIA ALCANÇADA ({success_count}/4). Entrada commitada no índice {self.commit_index}")
            return True
        return False
    
    def receive_client_command(self, command):
        if self.state != "leader":
            return "Erro: não sou o líder"
        
        with self.lock:
            entry = {"termo": self.current_term, "commando": command}
            self.log.append(entry)
            print(f"\nComando ({command}) recebido do Cliente, iniciando replicação...")

        success = self.broadcast_append_entries(new_entry=entry)
        if success:
            return f"Sucesso, comando ({command}) commitado"
        else:
            return "Erro: não alcançou consenso dos nós."

def main():
    if len(sys.argv) != 2:
        print("python raft_node.py <node_id>")
        sys.exit(1)
    
    node_id = int(sys.argv[1])

    if node_id not in [1, 2, 3, 4]:
        print("Nó deve estar entre 1 e 4")
        sys.exit(1)

    port = 9000 + node_id
    object_id = f"node{node_id}"
    raft_node = RaftNode(node_id)
    daemon = Pyro5.api.Daemon(port=port)
    daemon.register(raft_node, objectId=object_id)

    print(f"Raft Node {node_id} rodando em {raft_node.my_uri}")
    print(f"Estado inicial: {raft_node.state}, termo atual: {raft_node.current_term}")
    print(f"aguardando conexões...")

    daemon.requestLoop()

if __name__ == "__main__":    main()