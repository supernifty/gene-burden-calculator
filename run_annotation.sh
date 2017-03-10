#!/bin/bash

INPUT_FILE=$1
OUTPUT_FILE=$2
SETTINGS=$3

# sort INPUT_FILE > OUTPUT_FILE.temp

/app/tools/vt/vt decompose -s $INPUT_FILE | /app/tools/vt/vt normalize -r /app/assets/genome.fa - |
    perl ./tools/ensembl-tools-release-87/scripts/variant_effect_predictor/variant_effect_predictor.pl --cache \
        --format vcf -o stdout --force_overwrite --vcf --offline --no_progress \
        --sift b --polyphen b -symbol --canonical \
        --dir /app/assets/ \
        -custom /app/assets/ExAC.r0.3.nonTCGA.sites.vep.pass.vt.vcf.gz,ExAC,vcf,exact,0,AF,AC,AC_AFR,AC_AMR,AC_Adj,AC_EAS,AC_FIN,AC_Het,AC_Hom,AC_NFE,AC_OTH,AC_SAS,AF,AN,AN_AFR,AN_AMR,AN_Adj,AN_EAS,AN_FIN,AN_NFE,AN_OTH,AN_SAS \
    | /app/tools/vcfanno /app/tools/anno.conf /dev/stdin \
    | java -jar /app/assets/snpEff/SnpSift.jar annotate -a /app/assets/fannsdb.small.vcf.gz \
    | python /app/tools/vcf2tab.py > $OUTPUT_FILE

