#!/usr/bin/python
#
#Author: Julio Alvarez, Alvaro Mate
#
#Date: 23/02/2018
#
#New tariff addons massive activation EMA SQLs and SDP batches script generator
#
#

import sys
import time
import os
from optparse import OptionParser

def create_execution_file():
        f=open("execute_sql.sh", "w")
        f.write ("(($# < 1)) && echo \"Use: execute_sql.sh <SQL commands file>\" && exit\n\n")
        f.write ("printf \"Start Time: %s `date +%d%m%Y_%H%M%S`\\n\" >> $1.log\n")
        f.write ("sqlplus  jzaddon_ema/jzaddon_ema@emadb @$1 >> $1.log\n")
        f.write ("printf \"Finish Time: %s `date +%d%m%Y_%H%M%S`\\n\" >> $1.log\n")
        f.close()
        #os.chmod("execute_sql.sh", 0755)

        os.system ("cp %s %s" % ("execute_sql.sh", udir))
        os.system ("cp %s %s" % ("execute_sql.sh", rdir))

        os.remove("execute_sql.sh")

def close_file(fs, sfs):
        #Close file one
        fs.write("\ncommit;\nquit\n")
        fs.close()
        print (" Closing output file: " + sfs)

def close_batch (path, fs, sfs):
        #Close file one
        fs.close()
        print (" Closing output file: " + sfs)

        #Write batch header file
        pfile = open(str(path) + "/" + str(sfs),"r")
        text = pfile.read();
        pfile.close()

        lines = text.count("\n"); #Number of commands in file
        pfile = open(str(path) + "/" + str(sfs),"w")
        pfile.write ("53," + str(lines) + "\n")
        pfile.write (text)
        pfile.close()

def insert(msisdn, addon):
        return "insert into COMMERCIAL_ADDONS (ADDONID, MSISDN, STATUS, ACTIVATION_DATE_TIME, EXTRA_DATA) values (" + addon + ",'" + msisdn + "','Activate',sysdate,'');\n"

def delete(msisdn, addon):
        return "delete from COMMERCIAL_ADDONS where MSISDN='" + msisdn + "' and ADDONID=" + addon + ";\n"

def update(msisdn, addon_old, addon_new):
        return "update COMMERCIAL_ADDONS set ADDONID=" + addon_new + ",ACTIVATION_DATE_TIME=sysdate where MSISDN='" + msisdn + "' and ADDONID=" + addon_old + ";\n"

def select(msisdn, addon):
        return "SELECT ADDONID||'|'||MSISDN||'|'||to_char(ACTIVATION_DATE_TIME,'yyyymmddhh24miss')||'|'||EXTRA_DATA FROM COMMERCIAL_ADDONS WHERE MSISDN='"+ msisdn + "' AND ADDONID=" + addon + ";\n"

def selectMultiSIM(msisdn):
        return "select msisdn||'|'||subs_type||'|'||MASTER_MSISDN from COMMERCIAL_MULTISIM where MSISDN='"+ msisdn + "' or MASTER_MSISDN='" + msisdn +"';\n"


def addSDP(msisdn, addon):
        return addonCreation[int(addon)].replace("msisdn",msisdn)

def delSDP(msisdn, addon):
        return addonRemove[int(addon)].replace("msisdn",msisdn)

def hasSameAddonInDeleteAndInsert(addonRem, addonAdd):
    for addon in addonAdd:
        if addon in addonRem:
            return True
    return False


#SDP batch commands
addonCreation = {165 : "UA_INSTALL,msisdn,1,165,512000,\nGENERAL,msisdn,128,448,,,,,,,,\n",
                 127 : "UA_INSTALL,msisdn,1,127,12582912,\nOFFER_INSTALL,msisdn,127,,,,,,,\n",
                 126 : "UA_INSTALL,msisdn,1,126,10485760,\nOFFER_INSTALL,msisdn,126,,,,,,,\n",
                 125 : "UA_INSTALL,msisdn,1,125,15000,\nOFFER_INSTALL,msisdn,125,,,,,,,\n",
                 124 : "UA_INSTALL,msisdn,1,124,11534336,\nOFFER_INSTALL,msisdn,124,,,,,,,\n",
                 121 : "UA_INSTALL,msisdn,1,121,6291456,\nOFFER_INSTALL,msisdn,121,,,,,,,\n",
                 120 : "UA_INSTALL,msisdn,1,120,8388608,\nOFFER_INSTALL,msisdn,120,,,,,,,\n",
                 119 : "UA_INSTALL,msisdn,1,119,7340032,\nOFFER_INSTALL,msisdn,119,,,,,,,\n",
                 118 : "UA_INSTALL,msisdn,1,118,3145728,\nOFFER_INSTALL,msisdn,118,,,,,,,\n",
                 117 : "UA_INSTALL,msisdn,1,117,1258291,\nOFFER_INSTALL,msisdn,117,,,,,,,\n",
                 116 : "UA_INSTALL,msisdn,1,116,2621440,\nOFFER_INSTALL,msisdn,116,,,,,,,\n",
                 114 : "UA_INSTALL,msisdn,1,114,6000,\nGENERAL,msisdn,1024,1024,,,,,,,,\n",
                 108 : "UA_INSTALL,msisdn,1,108,1048576,\nOFFER_INSTALL,msisdn,108,,,,,,,\n",
                 106 : "UA_INSTALL,msisdn,1,106,1572864,\nOFFER_INSTALL,msisdn,106,,,,,,,\n",
                 104 : "UA_INSTALL,msisdn,1,104,4194304,\nOFFER_INSTALL,msisdn,104,,,,,,,\n",
                 99  : "UA_INSTALL,msisdn,1,99,2097152,\nOFFER_INSTALL,msisdn,99,,,,,,,\n",
                 97  : "UA_INSTALL,msisdn,1,97,1153434,\nOFFER_INSTALL,msisdn,97,,,,,,,\n",
                 96  : "UA_INSTALL,msisdn,1,96,204800,\nOFFER_INSTALL,msisdn,96,,,,,,,\n",
                 94  : "UA_INSTALL,msisdn,1,94,102400,\nGENERAL,msisdn,2048,2048,,,,,,,,\n",
                 93  : "UA_INSTALL,msisdn,1,93,5242880,\nGENERAL,msisdn,256,448,,,,,,,,\n",
                 92  : "UA_INSTALL,msisdn,1,92,1048576,\nGENERAL,msisdn,192,448,,,,,,,,\n",
                 71  : "UA_INSTALL,msisdn,1,71,12000,\nOFFER_INSTALL,msisdn,71,,,,,,,\n",
                 70  : "UA_INSTALL,msisdn,1,70,6000,\nGENERAL,msisdn,2,15,,,,,,,,\n",
                 69  : "UA_INSTALL,msisdn,1,20,16,\nUA_SET,msisdn,1,20,16,\n"
}
addonRemove = { 165  : "UA_DELETE,msisdn,1,165\nGENERAL,msisdn,0,448,,,,,,,,\n",
                1165 : "UA_DELETE,msisdn,1,165\n",
                127  : "UA_DELETE,msisdn,1,127\nOFFER_DELETE,msisdn,127\n",
                126  : "UA_DELETE,msisdn,1,126\nOFFER_DELETE,msisdn,126\n",
                125  : "UA_DELETE,msisdn,1,125\nOFFER_DELETE,msisdn,125\n",
                124  : "UA_DELETE,msisdn,1,124\nOFFER_DELETE,msisdn,124\n",
                121  : "UA_DELETE,msisdn,1,121\nOFFER_DELETE,msisdn,121\n",
                120  : "UA_DELETE,msisdn,1,120\nOFFER_DELETE,msisdn,120\n",
                119  : "UA_DELETE,msisdn,1,119\nOFFER_DELETE,msisdn,119\n",
                118  : "UA_DELETE,msisdn,1,118\nOFFER_DELETE,msisdn,118\n",
                117  : "UA_DELETE,msisdn,1,117\nOFFER_DELETE,msisdn,117\n",
                116  : "UA_DELETE,msisdn,1,116\nOFFER_DELETE,msisdn,116\n",
                114  : "UA_DELETE,msisdn,1,114\nGENERAL,msisdn,0,1024,,,,,,,,\n",
                108  : "UA_DELETE,msisdn,1,108\nOFFER_DELETE,msisdn,108\n",
                106  : "UA_DELETE,msisdn,1,106\nOFFER_DELETE,msisdn,106\n",
                104  : "UA_DELETE,msisdn,1,104\nOFFER_DELETE,msisdn,104\n",
                99   : "UA_DELETE,msisdn,1,99\nOFFER_DELETE,msisdn,99\n",
                97   : "UA_DELETE,msisdn,1,97\nOFFER_DELETE,msisdn,97\n",
                96   : "UA_DELETE,msisdn,1,96\nOFFER_DELETE,msisdn,96\n",
                94   : "UA_DELETE,msisdn,1,94\nGENERAL,msisdn,0,2048,,,,,,,,\n",
                93   : "UA_DELETE,msisdn,1,93\nGENERAL,msisdn,0,448,,,,,,,,\n",
                92   : "UA_DELETE,msisdn,1,92\nGENERAL,msisdn,0,448,,,,,,,,\n",
                1092 : "UA_DELETE,msisdn,1,92\n",
                71   : "UA_DELETE,msisdn,1,71\nOFFER_DELETE,msisdn,71\n",
                70   : "UA_DELETE,msisdn,1,70\nGENERAL,msisdn,0,15,,,,,,,,\n",
                69   : "UA_DELETE,msisdn,1,20\n"
}

print (sys.argv[0])

parser = OptionParser(usage="usage: %prog [options] <inputFile>",
                      version="%prog v1.1 - 2018-02-23 Addon Massive Migration script")
parser.add_option("-a", "--add",
                  action="store",
                  type="string",
                  dest="createAddons",
                  help="New addons to be added to the subscriber. Comma separated")

parser.add_option("-r", "--remove",
                  action="store",
                  type="string",
                  dest="removeAddons",
                  help="Current addons to be removed from the subscriber. Comma separated")
(options, args) = parser.parse_args()

if len(args) != 1:
        parser.error(" Wrong number of arguments")

if not options.createAddons and not options.removeAddons:
        print ("\n ERROR: At least one addon to remove/add is needed. !!!!!!!!")
        sys.exit(1)

print ("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
print ("Start >> Addon massive migration batch generation")
print (">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

#Addon validation
addonAddList = []
if options.createAddons:
        print ("\n This is the list of addons to create: " + options.createAddons)
        addonAddList = options.createAddons.split(',')
        for addon in addonAddList:
                if int(addon) not in addonCreation:
                        print ("\n ERROR: Addon " + addon + " not supported. !!!!!!!!")
                        sys.exit(2)

addonRemList = []
if options.removeAddons:
        print ("\n This is the list of addons to remove: " + options.removeAddons)
        addonRemList = options.removeAddons.split(',')
        for addon in addonRemList:
                if int(addon) not in addonRemove:
                        print ("\n ERROR: Addon " + addon + " not supported. !!!!!!!!")
                        sys.exit(2)

fmsisdn = open(args[0],"r")
batch_name = fmsisdn.readline().strip('\n') # First line of the file with batch information. It can end with ";" but we dont use this variable
fmsisdn.close();

print ("\n This is the migration file name: " + batch_name + "\n")
#time.sleep(1)

#Create output directories
directory = time.strftime("%Y%m%d_%H%M%S_Migration_file_" + args[0])
udir = directory + "/update";
rdir = directory + "/rollback";
#cdir = directory + "/check";
batchdir = directory + "/sdp_batch_change_Migration_file_" + args[0]
batchrdir = directory + "/sdp_batch_rollback_Migration_file_" + args[0]

#General directory
if not os.path.exists(directory):
        os.makedirs(directory)
        print (" General output directory --> " + directory )
#changes directory
if not os.path.exists(udir):
        os.makedirs(udir)
        print (" EMA Update output directory --> " + udir );
#Rollback directory
if not os.path.exists(rdir):
        os.makedirs(rdir)
        print (" EMA Rollback output directory --> " + rdir );
#CS batch directory
if not os.path.exists(batchdir):
        os.makedirs(batchdir)
        print (" SDP BATCH output directory --> " + batchdir );
#CS batch rollback directory
if not os.path.exists(batchrdir):
        os.makedirs(batchrdir)
        print (" SDP BATCH Rollback output directory --> " + batchrdir );

#Create script for EMA sql execution
create_execution_file()

multifsout = args[0] + ".multisim.sql"
sfucout    = args[0] + ".post_updated_check.sql"
sfuout     = args[0] + ".changes.sql"
sfrout     = args[0] + ".rollback.sql"
sfrcout    = args[0] + ".post_rollback_check.sql"
sdpfout  = time.strftime("SUBSCRPT_DATA_%Y%m%d_change_Migration_file_" + args[0]+ ".DAT")
sdpfrout = time.strftime("SUBSCRPT_DATA_%Y%m%d_rollback_Migration_file_" + args[0]+ ".DAT")

#Create insert/delete files
print ("\n Creating insert/update/delete output file: " + sfuout)
fuout = open(udir + "/" + sfuout,"w")

# Create multisim files
print (" Creating select multisim output file: " + multifsout)
fmultiout = open(udir + "/" + multifsout,"w")

#Create updated check files
print (" Creating check output file: " + sfucout)
fcout = open(udir + "/" + sfucout,"w")

#Create rollback files
print (" Creating rollback output file: " + sfrout)
frout = open(rdir + "/" + sfrout,"w")

#Create rollback check file
print (" Creating rollback check output file: " + sfrcout)
fcrout = open(rdir + "/" + sfrcout,"w")

#Create SDP BATCH files
print (" Creating SDP BATCH output file: " + sdpfout)
fsdpfout = open(batchdir + "/" + sdpfout,"w")

#Create SDP BATCH Rollback files
print (" Creating SDP BATCH Rollback output file: " + sdpfrout)
fsdpfrout = open(batchrdir + "/" + sdpfrout,"w")

# Set header
header= "SET ECHO ON\nSET HEADING OFF\n"  # By default: ECHO OFF (do not print commands), HEADING ON (print columns headings)
fmultiout.write(header)

for i in (1,2): # 1: Calculate going forward, 2: Calculate rollback like going forward reversing -r and -a
        if i == 1:
                fout = fuout
                fsout = fcout
                fsdp = fsdpfout
                addonAdd = addonAddList
                addonRem = addonRemList
        else:
                fout = frout
                fsout = fcrout
                fsdp = fsdpfrout
                addonAdd = addonRemList
                addonRem = addonAddList

        fout.write(header)
        fsout.write(header) #ECHO ON, to find MSISDN in selects that returns no row

        fmsisdn = open(args[0],"r")
        fmsisdn.readline() #Discard first line
        line = 0
        for subs in fmsisdn:
                #Format MSISDN
                msisdn = subs.rstrip().split(';')[0]
                msisdn = msisdn.strip()
                if msisdn == "":
                        continue
                msisdn = "34" + msisdn
                line += 1

                #For simple cases in which we only replace one addon for other, is faster to do an update than to do an insert + delete.
                #Advantages: Faster and no duplicates generated.
                #Disadvantages: If the old addon does not exists, the new addon is not inserted. You have to check this later, parsing the update log.

                # WARNING: For more than one addon deleted and added: PUT CORRESPONDING ADDONS IN SAME POSITIONS IN DELETE AND ADD.
                #   For example 71 -> 125 (VOZ), 116 -> 104 (DATOS)
                #     OK: -r 71,116 -a 125,104
                #     KO: -r 116,71 -a 125,104
                #     KO: -r 71,116 -a 104,125

                #if ( len(addonAdd) == 1 and len(addonRem) == 1 and addonAdd[0] != addonRem[0] and
                #                not ((addonAdd[0] == "92" and addonRem[0] == "165") or (addonAdd[0] == "165" and addonRem[0] == "92")) ):
                if ( len(addonAdd) == len(addonRem) and not hasSameAddonInDeleteAndInsert(addonRem, addonAdd) and
                                "92"  not in addonAdd and "92"  not in addonRem and
                                "165" not in addonAdd and "165" not in addonRem ):
                        for j in range(0, len(addonAdd)):
                                addon_old = addonRem[j]
                                addon_new = addonAdd[j]

                                #SDP
                                fsdp.write(addSDP(msisdn, addon_new)) #Write SDP batch
                                fsdp.write(delSDP(msisdn, addon_old)) #Write SDP batch

                                #EMA
                                fout.write(update(msisdn, addon_old, addon_new)) #EMA update
                                fsout.write(select(msisdn, addon_new)) #EMA check new inserted addon
                else:
                        for addon in addonAdd:
                                # Check if addon to create is also in the remove list. This will be studied in remove part
                                if addon not in addonRem:
                                        #SDP
                                        fsdp.write(addSDP(msisdn, addon)) #Write SDP batch

                                        #EMA
                                        fout.write(insert(msisdn, addon))  #EMA insert
                                        fsout.write(select(msisdn, addon)) #EMA check new inserted addon

                        for addon in addonRem:
                                addon_del = addon
                                if (addon == "92" and "165" in addonAdd) or (addon == "165" and "92" in addonAdd):
                                        addon_del = str(int(addon) + 1000) #addonSOB

                                # Check if addon to remove was in the list of addons to create. In this case remove must be done first
                                if addon in addonAdd:
                                        #SDP
                                        fsdp.write(delSDP(msisdn, addon_del)) #Write SDP batch
                                        fsdp.write(addSDP(msisdn, addon))     #Write SDP batch

                                        #EMA
                                        fsout.write(select(msisdn, addon)) #Just check EMA addon. It was supposed to be created
                                else:
                                        #SDP
                                        fsdp.write(delSDP(msisdn, addon_del)) #Write SDP batch

                                        #EMA
                                        fout.write(delete(msisdn, addon)) #EMA delete addon

                #Write multisim select commands
                if i == 1:
                        fmultiout.write(selectMultiSIM(msisdn))

                #Add a commit command every 5000 MSISDNs (about 10K INSERT/DELETE)
                if line % 5000 == 0:
                        fout.write("commit;\n")

        fmsisdn.close();
        print ("\n MSISDNs processed " + ("(forward)" if i==1 else "(rollback)") + " %d" % line)


print("")
#Close output split files at the end of the script
close_batch(batchdir, fsdpfout, sdpfout)
close_batch(batchrdir, fsdpfrout, sdpfrout)

#Close EMA files that are not split
close_file(fuout,sfuout) # Closing select file
close_file(fcout,sfucout) # Closing update check file
close_file(fcrout,sfrcout) # Closing rollback check file
close_file(frout,sfrout) # Closing rollback file
close_file(fmultiout,multifsout) # Closing multisim select file

print ("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
print ("End >> Addon massive migration batch generation")
print (">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
