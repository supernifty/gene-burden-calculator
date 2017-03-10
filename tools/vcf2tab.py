'''
burgene
khalid.mahmood@unimelb.edu.au
'''

#!/usr/bin/python

# from utils import findlist
# from annotations import getTabixVal,getTabixValCondel,getTabixBool
# from annotations import getfathmm,adjust_scores

import sys
import os
import argparse
import getopt
import vcf
import re
import array
#import pandas as pd

def cal_maf(info, pops):
    af_list = [None] * len(pops)
    # for all pops
    for i, val in enumerate(pops):
        ac_string = 'ExAC_AC_' + str(val)
        an_string = 'ExAC_AN_' + str(val)
        
        if (ac_string in info and an_string in info):
            ac = float(info['ExAC_AC_' + val][0])
            an = float(info['ExAC_AN_' + val][0])
            af_all = str(ac) + "\t" + str(an)
            try:
                af = str(float(ac/an))
                af_list[i] = str(af_all + "\t" + af)
            except ZeroDivisionError as detail:
                af_list[i] = str(af_all + "\t" + "0.0")
        else:
            #af_list[i] = str('0.0')
            af_list[i] = "0.0\t0.0\t0.0"

    return af_list

# MAIN

def main(argv):

    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--vcf", type=str, dest="vcf", help="Input variant file (vcf)", required=False)
    #parser.add_argument("-o", "--output", type=str, dest="out", help="Output file (tabular)", required=False)
    parser.add_argument("-v", "--verbosity", action="count", default=0)

    args = parser.parse_args()
    #outputfile = open(args.out, "w")
    outputfile = sys.stdout

    if args.verbosity >= 2:
        print("{} to the power {} equals {}".format(args.v, args.o, answer))
    elif args.verbosity >= 1:
        print("{}^{} == {}".format(args.x, args.y, answer))
    
    #gene #chr #pos #id
    #impact #impact_type #allele_count #total_alleles
    #exac_all_pop #exac_african #exac_latino #exac_east_asian
    #exac_fin #exac_nonfin_eur #exac_south_asian #exac_other
    #cadd #condel_score #sift #polyphen

    pops = ['Adj', 'AFR', 'AMR', 'EAS', 'FIN', 'NFE', 'SAS', 'OTH']

    # Output header
    #outputfile.write("chr\tpos\tid\tref\talt\tgene\timpact\timpact_type\tconsequence\n")
    top_header = "gene\tchrom\tstart\tend\tref\talt\timpact\timpact_type\tallele_count\t" \
        + "allele_number\t" \
        + "exac_all_pop_ac\texac_all_pop_an\texac_all_pop\t" \
        + "exac_african_ac\texac_african_an\texac_african\t" \
        + "exac_latino_ac\texac_latino_an\texac_latino\t" \
        + "exac_east_asian_ac\texac_east_asian_an\texac_east_asian\t" \
        + "exac_fin_ac\texac_fin_an\texac_fin\t" \
        + "exac_nonfin_eur_ac\texac_nonfin_eur_an\texac_nonfin_eur\t" \
        + "exac_south_asian_ac\texac_south_asian_an\texac_south_asian\t" \
        + "exac_other_ac\texac_other_ac\texac_other\t" \
        + "sift\tpolyphen\tcondel_score\tcadd\n"

    #series index array
    series_index = top_header.split('\t')
    #burden_df = pd.DataFrame(columns=series_index) 

    vcf_row = {}

    vcf_reader = vcf.Reader(sys.stdin)
    number_samples = len(vcf_reader.samples)
    outputfile.write(str(number_samples) + "\n")
    outputfile.write(top_header)
  
    #vcf_reader = vcf.Reader(open(args.vcf, 'r'))
    for record in vcf_reader:
        current_chr = record.CHROM #chrom
        current_id = record.ID
        current_start = record.POS - 1 #end
        current_end = record.POS #start
        current_ref = record.REF #ref
        current_alt = ','.join(str(v) for v in record.ALT) #alt
        current_ac = ','.join(str(v) for v in record.INFO['AC']) #allele_count
        current_an = record.INFO['AN'] #allele_number
        
        current_sift, current_polyphen, current_condel, current_cadd = '','','',''
        current_gene, current_feature = '',''
        current_impact, current_impact_type = '',''
        try:
            current_cadd = str(record.INFO['CADD'][0])
        except KeyError:
            current_cadd = ''

        try:
            current_condel = str(record.INFO['CONDEL'][0])
        except KeyError:
            current_condel = ''

        #print current_chr + ":" + str(current_start) + ":" + str(current_end) + ":" + current_ref + ":" + current_alt + ":" + current_cadd + ":" + current_condel        
        current_exac = '\t'.join(cal_maf(record.INFO, pops)) #exac...

        # CHECK INDEL AND MNP
        indel = True if ((len(current_ref) > 1 or len(current_alt) > 1) and \
                ("," not in current_ref and "," not in current_alt)) else False
        mnp = True if len(record.ALT) > 1 else False
        mnpflag = "%s" % mnp

        if "CSQ" in record.INFO:
            csq = record.INFO['CSQ']

            # BELOW: THERE ARE A COUPLE OF OPTIONS TO PROCEED
            # For going through annotations for all transcript
            for current_csq_element in csq:
                current_csq = current_csq_element.split('|')
                current_gene = current_csq[3] #gene
                current_impact = current_csq[2] #impact
                current_impact_type = current_csq[1] #impact_type
                current_sift_ = current_csq[24]
                current_sift = current_sift_[current_sift_.find("(")+1:current_sift_.find(")")] #sift
                current_polyphen_ = current_csq[25]
                current_polyphen = current_polyphen_[current_polyphen_.find("(")+1:current_polyphen_.find(")")] #polyphen
                #current_sift = current_csq[24][current_csq[24].find("(")+1:current_csq.find(")")] 
                #current_polyphen = current_csq[25][current_csq[25].find("(")+1:current_csq.find(")")]                 

                out_str = [ current_gene, current_chr, current_start, current_end, current_ref, current_alt,
                    current_impact, current_impact_type,
                    current_ac, current_an, current_exac, current_sift, current_polyphen, current_condel, current_cadd]
                #
                out_str = [str(x) or 'None' for x in out_str]
                outputfile.write("\t".join(out_str))
                outputfile.write("\n")
                #current_series = pd.Series(out_str, index=series_index)
                #burden_df=burden_df.append(current_series,ignore_index=True)
                break

        else:
            current_gene, current_feature = '',''
            out_str = [ current_gene, current_chr, current_start, current_end, current_ref, current_alt,
                current_impact, current_impact_type,
                current_ac, current_an, current_exac, current_sift, current_polyphen, current_condel, current_cadd]
            #
            out_str = [str(x) or 'None' for x in out_str]
            outputfile.write("\t".join(out_str))
            outputfile.write("\n")
            #current_series = pd.Series(out_str, index=series_index)
            #burden_df=burden_df.append(current_series,ignore_index=True)

            


    outputfile.close()

if __name__ == "__main__":
    main(sys.argv)
