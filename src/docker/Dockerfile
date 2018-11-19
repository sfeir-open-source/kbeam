FROM adoptopenjdk/openjdk8-openj9

ARG MAVEN_VERSION=3.6.0
ARG USER_HOME_DIR="/root"
ARG SHA=fae9c12b570c3ba18116a4e26ea524b29f7279c17cbaadc3326ca72927368924d9131d11b9e851b8dc9162228b6fdea955446be41207a5cfc61283dd8a561d2f
ARG BASE_URL=https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries

RUN apt-get update -qqy && apt-get install -qqy curl \
  && mkdir -p /usr/share/maven /usr/share/maven/ref \
  && curl -fsSL -o /tmp/apache-maven.tar.gz ${BASE_URL}/apache-maven-$MAVEN_VERSION-bin.tar.gz \
  && echo "${SHA}  /tmp/apache-maven.tar.gz" | sha512sum -c - \
  && tar -xzf /tmp/apache-maven.tar.gz -C /usr/share/maven --strip-components=1 \
  && rm -f /tmp/apache-maven.tar.gz \
  && ln -s /usr/share/maven/bin/mvn /usr/bin/mvn \
  # clean up build packages
  && apt-get remove -qqy --purge curl \
  && rm /var/lib/apt/lists/*_*

RUN cd ~ && git clone https://github.com/sfeir-open-source/kbeam.git && cd ~/kbeam && mvn build && cd ~/ && rm -rf ~/kbeam

ENV M2_HOME /usr/share/maven

# transitively resolve all dependencies
#ADD deps.txt /builder/deps.txt
#ADD resolve-deps.sh /builder/resolve-deps.sh

#RUN /builder/resolve-deps.sh

ENTRYPOINT ["mvn"]

