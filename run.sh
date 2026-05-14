#!/bin/bash

ENV_NAME="redes"
CONDA_PATH=$(conda info --base)
source "$CONDA_PATH/etc/profile.d/conda.sh"

echo "--- Inicializando Sistema Distribuído Raft ---"

conda activate $ENV_NAME


echo "Abrindo Name Server..."
gnome-terminal --title="Pyro5 Name Server" -- bash -c "source $CONDA_PATH/etc/profile.d/conda.sh; conda activate $ENV_NAME; python -m Pyro5.nameserver; exec bash"

sleep 2


for i in {1..4}
do
    echo "Lançando Nó $i..."
    gnome-terminal --title="Raft Node $i" -- bash -c "source $CONDA_PATH/etc/profile.d/conda.sh; conda activate $ENV_NAME; python raft_node.py $i; exec bash"
done

echo "Aguardando eleição inicial..."
sleep 5

echo "Iniciando Cliente interativo neste terminal..."
python client.py