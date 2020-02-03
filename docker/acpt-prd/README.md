# Acceptance/Production environtment

The files in this directory help deploying the system in an acceptance or
production environtment based on Docker.

## Deployment

# Preparation

```
# Clone code from repository
git clone git@gitlab.com:KNMI/RDSA/DMPilot/rulemanager.git

# Write initial configuration file
cp docker/acpt-prd/configuration.sample.py ./configuration.py
vim ./configuration.py

# Create var data dir to be mounted as volume (for i.e. deletion.db file)
mkdir -p /data/seismo/rulemanager
```

```
# Build custom container image(s)
docker-compose -f docker/acpt-prd/docker-compose.yml --project-directory ./ build
```

# Run with docker-compose

```
# Run rulemanager's sdsmanager with PPSD sequence for 1 file
docker-compose -f docker/acpt-prd/docker-compose.yml --project-directory ./ run --rm rulemanager ./sdsmanager.py --ruleseq rule_sequences/ppsd_seq.json --collect_wildcards NL.NH010..HGZ.D.2020.001

# Run rulemanager container interactively for testing
docker-compose -f docker/acpt-prd/docker-compose.yml --project-directory ./ run --rm rulemanager /bin/bash
```

# Update code

 1. Update code (`git pull -v`)
 2. Re-build container image(s) (see _Preparation_)
 3. Re-run container

# Update configuration

 1. Update config (`vim ./configuration.py`)
 2. Re-run container
