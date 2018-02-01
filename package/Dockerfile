from ubuntu:artful

MAINTAINER Michael Spector <michael@viyadb.com>
LABEL Description="ViyaDB is in-memory analytical data store"

ENV TERM=xterm

RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl1.0.0 \
    curl \
    bsdmainutils \
    awscli \
    g++-7 \
  && update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-7 60 --slave /usr/bin/g++ g++ /usr/bin/g++-7 \
  && apt-get purge -y \
    manpages-dev \
  && apt-get autoremove -y \
  && apt-get clean \
  && strip $(gcc -print-prog-name=cc1plus) \
  && strip $(gcc -print-prog-name=lto1) \
  && find /usr/lib | grep -E "(__pycache__|\.pyc|\.pyo$)" | xargs rm -rf \
  && rm -rf \
    /var/lib/apt \
    /var/lib/cache \
    /var/lib/log \
    /var/tmp/* \
    /tmp/* \
    /usr/share/doc \
    /usr/share/man \
    /usr/share/locale \
    /usr/share/dh-python \
    $(gcc -print-prog-name=cc1)

ARG tarball
ADD $tarball /opt/
RUN mv /opt/viyadb* /opt/viyadb \
  && strip /opt/viyadb/bin/viyad

RUN mkdir /opt/viyadb/conf
ADD store.json /opt/viyadb/conf/store.json

EXPOSE 5000-5555

WORKDIR "/opt/viyadb"
CMD ["/opt/viyadb/bin/viyad", "/opt/viyadb/conf/store.json"]

