#!/usr/bin/env bash
# Instala Docker Engine + Compose v2 no Debian 13 (trixie) a partir dos
# repositórios oficiais do Debian. Roda com sudo:
#   sudo bash scripts/install_docker.sh
set -euo pipefail

TARGET_USER="${SUDO_USER:-$(id -un)}"

echo ">> Atualizando índice de pacotes..."
apt-get update

echo ">> Instalando docker.io (engine + containerd + cli) e docker-compose (v2)..."
DEBIAN_FRONTEND=noninteractive apt-get install -y docker.io docker-compose

echo ">> Habilitando e iniciando o daemon do Docker..."
systemctl enable --now docker

echo ">> Adicionando '$TARGET_USER' ao grupo docker (uso sem sudo)..."
usermod -aG docker "$TARGET_USER"

echo
echo ">> Versões instaladas:"
docker --version
docker compose version || docker-compose --version

echo
echo "OK. Para usar 'docker' sem sudo nesta sessão, rode:  newgrp docker"
echo "(ou faça logout/login uma vez)."
