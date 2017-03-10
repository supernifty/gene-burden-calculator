FROM ubuntu:16.04
MAINTAINER Peter Georgeson "peter.georgeson@gmail.com"

# install base requirements
RUN apt-get update -y
RUN apt-get install -y \
  perl \
  python-pip \
  python-dev \
  build-essential \
  git \
  libz-dev \
  curl \
  wget \
  libdbi-perl \
  unzip \
  python3 \
  default-jre \
  tabix

RUN pip install --upgrade pip

# install tools

# vep
RUN export PERL_MM_USE_DEFAULT=1 && cpan App::cpanminus && \
  cpanm HTTP::Tiny && \
  cpanm LWP::Simple && \
  cpanm Archive::Extract && \
  cpanm Module::Build && \
  cpanm File::Copy::Recursive && \
  cpanm CGI

# old vep
RUN mkdir -p /app/tools && \
  cd /app/tools && \
  wget https://github.com/Ensembl/ensembl-tools/archive/release/87.zip && \
  unzip 87.zip && \
  cd ./ensembl-tools-release-87/scripts/variant_effect_predictor/ && \
  perl ./INSTALL.pl

# new vep
#RUN mkdir -p /app/tools && \
#  cd /app/tools && \
#  git clone https://github.com/Ensembl/ensembl-vep.git && \
#  cd ensembl-vep && \
#  perl INSTALL.pl

# vt
RUN mkdir -p /app/tools && \
  cd /app/tools && \
  git clone https://github.com/atks/vt.git && \
  cd vt && \
  make && \
  make test

# vcfanno - binary is in tools
# snpeff - assets
# vep cache - assets

# copy app files 
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt


EXPOSE 5000

# run command
CMD ["/bin/bash", "run_all.sh"]
