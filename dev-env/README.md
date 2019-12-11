# Local development environment

The files in this directory help creating a local environment based on Docker for development purposes (only).

# Steps to create and run environment

## 1) Preparation

```
# Create and edit configuration (use unique S3 SDS prefix to avoid conflicts)
cp ./dev-env/configuration.sample.py ./configuration.py
vim ./configuration.py

# Download SDS sample data
mkdir -p dev-env/data/sds/
aws s3 cp s3://seismo-test-sds-samples/sds-sample-data.tar dev-env/data/sds-sample-data.tar --profile sandbox
tar xvf dev-env/data/sds-sample-data.tar -C dev-env/data/sds/

# Create Docker images of other services (use a separate local dir)
git clone git@gitlab.com:KNMI/RDSA/ppsd-webservice.git <PPSD_WS_DIR>
cd <PPSD_WS_DIR>
docker build -t ppsd_webservice .

# Put AWS credentials in env variables to be used by containers
# (it assumes your credentials are stored in a profile with your user name)
export AWS_ACCESS_KEY_ID=$(aws --profile $(id -un) configure get aws_access_key_id)
export AWS_SECRET_ACCESS_KEY=$(aws --profile $(id -un) configure get aws_secret_access_key)
```

## 2 - Option 1) Complete environment with docker-compose

```
# (Re-)Build custom container images
docker-compose -f dev-env/docker-compose.yml --project-directory ./ -p devenv build

# Bring containers up
docker-compose -f dev-env/docker-compose.yml --project-directory ./ -p devenv up

# Run rulemanager container interactively
docker-compose -f dev-env/docker-compose.yml --project-directory ./ -p devenv run rulemanager bash

# Bring everything down
docker-compose -f dev-env/docker-compose.yml --project-directory ./ -p devenv down
```

## 2 - Option 2) Rule Manager container only

```
# Build container image
docker build -t rulemanager -f ./dev-env/Dockerfile .

# Run container interactively
docker run -v $(pwd):/home/rulemanager -v $HOME/tmp/sds_1:/data/temp_archive -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY -it rulemanager
```

## 3) Commands inside Rule Manager container

```
# Execute rule manager (inside container)
./sdsmanager.py -h
./sdsmanager.py --dir /data/temp_archive --rulemap ppsd_rules.json --ruleseq sequences/ppsd_seq.json --collect_wildcards "NL.HGN.02.BHZ.D.2019.335"

# List S3 bucket (inside container)
aws s3 ls seismo-test-sds --profile sandbox
```

## 4) Remove all Docker containers / images
```
# Remove all Docker containers
docker rm $(docker ps -a -q)

# Remove all Docker images
docker rmi $(docker images -q)
```

