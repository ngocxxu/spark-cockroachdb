# Spark + CockroachDB project

## Setup WSL

- Open powercell admin run `wsl --install`

## Setup environment Java

- Run `sudo apt update`
- Run `sudo apt install openjdk-11-jdk`

## Setup Python

- Run `sudo apt install python3`
- Run `sudo apt install python3-pip`

## Setup Apache Spark

- Run `pip3 install pyspark matplotlib faker pandas`

## Clone repository github into WSL

- Open WSL terminal and run `cd /home/[username]`
- Access and clone `https://github.com/ngocxxu/spark-cockroachdb`
- Open source code by VS Code
- Press [Ctrl + `] to open terminal

## Run Spark

- Run `spark-submit ./scripts/create_user_data.py`
- Run `spark-submit ./scripts/user_analysis.py`
