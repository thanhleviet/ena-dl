FROM ubuntu:16.04
MAINTAINER robbie.petit@gmail.com

# Aspera Connect
RUN apt-get update && \
    apt-get install --no-install-recommends -y \
        python3-dev \
        python3-pip \
        wget && \
    pip3 install requests

RUN mkdir /aspera /data && \
    cd /tmp && \
    wget --quiet http://download.asperasoft.com/download/sw/connect/3.7.4/aspera-connect-3.7.4.147727-linux-64.tar.gz && \
    tar -xzf aspera-connect-3.7.4.147727-linux-64.tar.gz && \
    sed -i 's=INSTALL_DIR\=~/.aspera/connect=INSTALL_DIR\=/aspera=' aspera-connect-3.7.4.147727-linux-64.sh && \
    bash aspera-connect-3.7.4.147727-linux-64.sh

ENV ASCP /aspera/bin/ascp
ENV ASCP_KEY /aspera/etc/asperaweb_id_dsa.openssh

COPY ena-dl.py /usr/local/bin/ena-dl
RUN chmod 755 /usr/local/bin/ena-dl

# Add user biodocker with password biodocker
# https://github.com/BioContainers/containers/blob/master/biodocker/Dockerfile
RUN groupadd fuse \
    && useradd --create-home --shell /bin/bash --user-group --uid 1000 --groups sudo,fuse biodocker && \
    echo `echo "biodocker\nbiodocker\n" | passwd biodocker`

# Change user
USER biodocker

WORKDIR /data

CMD ["ena-dl", "--help"]
