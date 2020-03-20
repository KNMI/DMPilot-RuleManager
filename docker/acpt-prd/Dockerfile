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
RUN mkdir -p /home/iris-lib/ /home/bin/ /home/rulemanager/
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

# Declare volume for var files (i.e.: deletion.db)
VOLUME /var/rulemanager

# Declare volume to write app logs
VOLUME /tmp/rulemanager/logs

# Copy Rule Manager code
COPY . .

# Install python requirements
RUN pip3 install -r ./requirements.txt

# Configure user
RUN groupadd -g 23011 sysop && \
    useradd -u 23011 -g 23011 sysop
RUN chown -R sysop:sysop /home/rulemanager
USER sysop
