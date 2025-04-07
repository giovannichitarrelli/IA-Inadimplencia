#!/bin/bash

# Atualiza os pacotes e instala dependências necessárias
apt-get update

# Instala o msodbcsql18 aceitando o EULA automaticamente
ACCEPT_EULA=Y apt-get install -y msodbcsql18

# (Opcional) Instala outras dependências listadas em packages.txt, se houver
apt-get install -y unixodbc