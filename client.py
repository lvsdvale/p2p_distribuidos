import Pyro5.api
import sys

def main():
    print("Cliente conectando ao servidor...")

    try:
        ns = Pyro5.api.locate_ns()
        leader_uri = ns.lookup("leader")
        print(f"Líder encontrado em {leader_uri}\n")

    except Pyro5.errors.NamingError:
        print("Líder não encontrado. Aguarde eleição")
        sys.exit(1)
    except Exception as e:
        print(f"Erro ao conectar ao servidor de nomes: {e}")
        sys.exit(1)

    with Pyro5.api.Proxy(leader_uri) as leader:
        while True:
            command = input("\n Digite um comando para o líder ('sair' para encerrar)")
            if command.lower() == "sair":
                print("Encerrando cliente...")
                break

            try:
                response = leader.receive_client_command(command)
                print(f"Resposta do líder: {response}")
            except Exception as e:
                print(f"Conexão perdida com o líder: {e}, reinicie o cliente")
                break

if __name__ == "__main__":
    main()