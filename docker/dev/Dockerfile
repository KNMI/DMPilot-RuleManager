# Dockerfile for creating local development environment only

FROM ubuntu:bionic

# update packages
RUN apt-get update && apt-get upgrade -y

# install basics
RUN apt-get update && apt-get install -y gnupg2 wget unzip

# add obspy repository
RUN echo 'deb http://deb.obspy.org bionic main' >> /etc/apt/sources.list
RUN wget --quiet -O - https://raw.github.com/obspy/obspy/master/misc/debian/public.key | apt-key add -

# install obspy and dependencies
RUN apt-get update && apt-get -y install python3-obspy

# install pip
RUN apt-get update && apt-get -y install python3-pip

# Install data tools from IRIS
RUN mkdir -p /home/iris-lib/ /home/bin/
#
# libmseed
WORKDIR /home/iris-lib/
RUN wget --quiet https://github.com/iris-edu/libmseed/archive/v2.19.6.zip
RUN unzip v2.19.6.zip
WORKDIR /home/iris-lib/libmseed-2.19.6/
RUN make
WORKDIR /home/iris-lib/libmseed-2.19.6/example/
ENV CFLAGS="$CFLAGS -no-pie"
RUN make
RUN cp msrepack /home/bin/
#
# dataselect
WORKDIR /home/iris-lib/
RUN wget --quiet https://github.com/iris-edu/dataselect/archive/v3.22.zip
RUN unzip v3.22.zip
WORKDIR /home/iris-lib/dataselect-3.22/
RUN make
RUN cp dataselect /home/bin/
#
# msi
WORKDIR /home/iris-lib/
RUN wget --quiet https://github.com/iris-edu/msi/archive/v3.8.zip
RUN unzip v3.8.zip
WORKDIR /home/iris-lib/msi-3.8/
RUN make
RUN cp msi /home/bin/
#
WORKDIR /home/rulemanager/
RUN rm -Rv /home/iris-lib
ENV PATH="$PATH:/home/bin"

# Declare volume for temporary archive
VOLUME /data/temp_archive

# Declare volume for quarantined data
VOLUME /data/quarantine

# Declare volume to put Rule Manager code
VOLUME /home/rulemanager

# Install python requirements
COPY ./requirements.txt /home/rulemanager-requirements.txt
RUN pip3 install -r /home/rulemanager-requirements.txt
RUN rm -v /home/rulemanager-requirements.txt

# Install AWS CLI to be able to make S3 queries
RUN pip3 install awscli

# AWS credentials
ARG ADFS_USER
COPY ./docker/dev/aws-config-sandbox-role /home/aws-config
RUN sed -i "s/USER/${ADFS_USER}/g" /home/aws-config
ENV AWS_CONFIG_FILE="/home/aws-config"
