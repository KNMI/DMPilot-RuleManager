# Local development environment

The files in this directory help creating a local environment based on Docker for development purposes (only).

# Steps to create and run environment

## 1) Preparation

```
# Create and edit configuration (use unique S3 SDS prefix to avoid conflicts)
cp ./docker/dev/configuration.sample.py ./configuration.py
vim ./configuration.py

# Clone PPSD webservice code (in a separate dir), export its directory and customise config
git clone git@gitlab.com:KNMI/RDSA/ppsd-webservice.git <PPSD_WS_DIR>
export PPSD_WS_DIR=<PPSD_WS_DIR>
cd <PPSD_WS_DIR>
cp config.json.sample config.json

# Clone WFCatalog webservice code (in a separate dir), export its directory and customise config
git clone git@gitlab.com:KNMI/RDSA/webservices/wfcatalog-webservice.git <WFCAT_WS_DIR>
export WFCAT_WS_DIR=<PPSD_WS_DIR>
cd <WFCAT_WS_DIR>
cp configuration.json.sample configuration.json

# (Optional) Get temporary AWS credentials and put them in env variables to be used by containers
aws-session knmi-sandbox
export AWS_ACCESS_KEY_ID=$(aws --profile knmi-saml-session-credentials configure get aws_access_key_id)
export AWS_SECRET_ACCESS_KEY=$(aws --profile knmi-saml-session-credentials configure get aws_secret_access_key)
export AWS_SESSION_TOKEN=$(aws --profile knmi-saml-session-credentials configure get aws_session_token)
export ADFS_USER=<YOUR_ADFS_USER>

# (Optional) Download SDS sample data
mkdir -p docker/dev/data/sds/
aws s3 cp s3://seismo-test-sds-samples/sds-sample-data.tar docker/dev/data/sds-sample-data.tar --profile knmi-sandbox-saml
tar xvf docker/dev/data/sds-sample-data.tar -C docker/dev/data/sds/

# Tip: use a .env file to save the env variables PPSD_WS_DIR, WFCAT_WS_DIR and ADFS_USER for next runs
cp ./docker/dev/.env.sample .env
vim .env
```

## 2) Create environment with docker-compose

```
# (Re-)Build custom container images
docker-compose -f docker/dev/docker-compose.yml --project-directory ./ -p devenv build

# Bring environment up (mongodb's and web services)
docker-compose -f docker/dev/docker-compose.yml --project-directory ./ -p devenv up -d

# Check logs mongodb's and web services
docker logs ppsd_webservice --tail="10" -t -f
docker logs ppsd_mongo --tail="10" -t -f
docker logs wfcatalog_service --tail="10" -t -f
docker logs wfcatalog_mongo --tail="10" -t -f

# Run rulemanager container interactively
docker-compose -f docker/dev/docker-compose.yml --project-directory ./ -p devenv run rulemanager bash
```

## 3) Commands inside Rule Manager container

```
# Execute rule manager (inside container)
./sdsmanager.py -h
./sdsmanager.py --ruleseq rule_sequences/ppsd_seq.json --collect_wildcards "NL.HGN.02.BHZ.D.2019.335"

# List S3 bucket (inside container)
aws s3 ls seismo-test-sds --profile knmi-sandbox-saml
```

## 4) Stop and clean-up
```
# Bring environment down
docker-compose -f docker/dev/docker-compose.yml --project-directory ./ -p devenv down

# (Optional) Remove Docker volumes (data)
docker volume rm devenv_wfcat_mongodb
docker volume rm devenv_ppsd_mongodb

# (Optional) Remove all Docker images
docker rmi $(docker images -q)
```

