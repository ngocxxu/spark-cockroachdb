# Spark + CockroachDB project

## Setup WSL

- Open powercell admin run `wsl --install`
- Restart your computer
- Open powercell and run `wsl`
- Or you can download ubuntu from microsoft store to use it
  ![alt text](image.png)

* Simply install WSL via PowerShell and then install Ubuntu from the Microsoft Store. Once set up, you can start using Ubuntu in WSL.

## Setup environment Java

- Run `sudo apt update`

- Run `sudo apt install openjdk-11-jdk` for Window

## Clone repository github into WSL

- Open WSL terminal and run `cd /home/[username]`

- Access and clone `https://github.com/ngocxxu/spark-cockroachdb`

- Open source code by VS Code

- Press [Ctrl + `] to open terminal

## Setup Apache Spark

- Run `sudo apt install python3` (if you don't have python3)

- Run `sudo apt install pipenv` (if you don't have pipenv)

- Run `pipenv install`

- Run `pipenv install setuptools` (if error)

## Create ENV file

- Run `touch .env`

- Copy and paste into `.env` file

## Run Spark

- Run `pipenv run spark-submit ./scripts/create_user_data.py`

- Run `pipenv run spark-submit --master 'local[*]' --conf spark.ui.port=4043 ./scripts/user_analysis.py`

- Access `http://localhost:4043` to see the result

## Run CockroachDB

# Additional with Jupiter Notebook - You can skip this step

## Setup docker

- Download docker desktop from [here](https://www.docker.com/products/docker-desktop/) and open it

- Run `docker-compose up -d` in your project if you want to open Jupiter Notebook at `http://localhost:8888`
