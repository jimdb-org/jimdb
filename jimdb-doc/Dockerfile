FROM is.harbor.svc.m.n.jd.local/jdos_base/jd-new-centos7.2v0105:latest
ADD . /opt/src
RUN yum install wget -y && yum clean all && wget http://storage.jd.local/ui-work/node-v8.12.0-linux-x64.tar.xz -O /opt/src/node-v8.12.0-linux-x64.tar.xz && cd /opt/src && xz -d node-v8.12.0-linux-x64.tar.xz && tar -xvf node-v8.12.0-linux-x64.tar && /bin/rm node-v8.12.0-linux-x64.tar && chown -R root:root /opt/src
ENV PATH /opt/src/node-v8.12.0-linux-x64/bin:$PATH
ENV NODE_HOME /opt/src/node-v8.12.0-linux-x64/
ENV VERSION 8.12.0
RUN cd /opt/src && npm install gitbook-cli -g  && npm install -g pm2 && gitbook install && gitbook build
WORKDIR /opt/src
ENTRYPOINT /usr/sbin/sshd && pm2 startOrRestart /opt/src/bin/start.json && sleep 9999999d
