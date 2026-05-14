import Pyro5.api
import sys
import time

def get_leader_proxy():
    while True:
        try:
            ns = Pyro5.api.locate_ns()
            leader_uri = ns.lookup("leader")
            proxy = Pyro5.api.Proxy(leader_uri)
            proxy._pyroBind()
            return proxy
        except Exception:
            print("Aguardando eleição de um novo líder...")
            time.sleep(2)

def main():
    print("--- Cliente Raft Ativo ---")
    leader = get_leader_proxy()
    print(f"Conectado ao Líder.")

    while True:
        command = input("\nDigite o comando (ou 'sair'): ")
        if command.lower() == "sair": break

        try:
            response = leader.receive_client_command(command)
            print(f"Resposta: {response}")
        except Exception:
            print("Líder caiu! Buscando novo líder...")
            leader = get_leader_proxy()
            try:
                response = leader.receive_client_command(command)
                print(f"Resposta (novo líder): {response}")
            except Exception as e:
                print(f"Erro ao processar: {e}")

if __name__ == "__main__":
    main()