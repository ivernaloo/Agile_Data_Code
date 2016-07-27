#!/usr/bin/python
#coding:utf-8
# This is a command line utility for slurping emails from gmail and storing them as avro documents.
# I uses the GmailSlurper class, which in turn uses email utils.
import os, sys, getopt,imaplib, signal
import lepl, re
import email
import inspect, pprint
import time
from lepl.apps.rfc3696 import Email
from avro import schema, datafile, io
from email_utils import EmailUtils

def usage(context):
  print """Usage: mail.py -u <username@gmail.com> -p <password>"""

def does_exist(path_string, name):
  if(os.path.exists(path_string)):
    pass
  else:
    print "Error: " + name + ": " + path_string + " does not exist."
    sys.exit(2)

def main():
  try:
    opts, args = getopt.getopt(sys.argv[1:], 'm:u:p:s:f:o:i:') # 拿到参数
  except getopt.GetoptError, err:
    # print help information and exit:
    print "Error:" + str(err) # will print something like "option -a not recognized"
    usage("getopt")
    sys.exit(2)

  mode = None
  username = None
  password = None
  schema_path = './email.avro.schema' #'../avro/email.schema'
  imap_folder = None #'[Gmail]/All Mail'
  output_path = 'gmail_mbox'
  single_id = None
  arg_check = dict() # mapping object

  # 处理传入的参数
  for o, a in opts:
    print "Opts : " + a
    if o in ("-u"):
      username = a
      arg_check[o] = 1
    elif o in ("-p"):
      password = a
      arg_check[o] = 1
    else:
      assert False, "unhandled option"
  writer, writertmp = init_avro(output_path, 1, schema_path) # 初始化avro
  imap = init_imap(username, password)
  init_folder(imap, 'INBOX/aloo', writer, writertmp)

def init_folder(imap, folder, writer, writertmp):
 status, count = imap.select(folder)
 print "Folder '" + str(folder) + "' SELECT status: " + status

 if(status == 'OK'):
   count = int(count[0])
   ids = range(1,count)
   ids.reverse()
   #self.id_list = ids
   print "Folder '" + str(folder) + " has " + str(count) + "' emails...\n"
   print "Connected to folder " + folder + " and downloading " + str(count) + " emails...\n"
   slurp(imap, folder, ids, writer, writertmp) # ?
    #shutdown() #?
 else:
    print "Problem initializing imap connection."

def init_imap(username, password):
    try:
      imap.shutdown()
    except:
      pass
    try:
        print username
        print password
        imap = imaplib.IMAP4_SSL('imap.gmail.com', 993)
        imap.login(username, password)

        print "Login Success"
        return imap
    except:
        print "Init imap error"
        pass

def init_directory(directory):
    if os.path.exists(directory):
      print 'Warning: %(directory)s already exists:' % {"directory":directory}
    else:
      os.makedirs(directory)
    return directory

# part_id will be helpful one we're splitting files among multiple slurpers
def init_avro(output_path, part_id, schema_path):
  print("************* init_avro ***************")
  output_dir = None
  output_dirtmp = None	# Handle Avro Write Error
  if(type(output_path) is str):
    output_dir = init_directory(output_path)
    output_dirtmp = init_directory(output_path + 'tmp') # Handle Avro Write Error
  out_filename = '%(output_dir)s/part-%(part_id)s.avro' % \
    {"output_dir": output_dir, "part_id": str(part_id)}
  out_filenametmp = '%(output_dirtmp)s/part-%(part_id)s.avro' % \
    {"output_dirtmp": output_dirtmp, "part_id": str(part_id)}  # Handle Avro Write Error
  schemas = open(schema_path, 'r').read()
  email_schema = schema.parse(schemas)

  rec_writer = io.DatumWriter(email_schema)
  avro_writer = datafile.DataFileWriter(
    open(out_filename, 'a+b'),
    rec_writer,
    email_schema
  )
  # CREATE A TEMP AvroWriter that can be used to workaround the UnicodeDecodeError when writing into AvroStorage
  avro_writertmp = datafile.DataFileWriter(
      open(out_filenametmp, 'wb'),
    rec_writer,
    email_schema
  )
  return avro_writer, avro_writertmp
  print("*************end init_avro ***************")

def slurp(imap, folder, ids, writer, writertmp):
  print "*******************slurp**************************"
  if(imap and folder):
    for email_id in ids:
      (status, email_hash, charset) = fetch_email(imap, email_id)

      if(status == 'OK' and charset and 'thread_id' in email_hash and 'from' in email_hash):
        print email_id, charset, email_hash['thread_id']
        write(email_hash, writer, writertmp)
        if((int(email_id) % 1000) == 0):
          flush()
      elif(status == 'ERROR' or status == 'PARSE' or status == 'UNICODE' or status == 'CHARSET' or status =='FROM'):
        sys.stderr.write("Problem fetching email id " + str(email_id) + ": " + status + "\n")
        continue
      elif (status == 'ABORT' or status == 'TIMEOUT'):
        sys.stderr.write("resetting imap for " + status + "\n")
        stat, c = self.reset()
        sys.stderr.write("IMAP RESET: " + str(stat) + " " + str(c) + "\n")
      else:
        sys.stderr.write("ERROR IN PARSING EMAIL, SKIPPED ONE\n")
        continue
  print "*******************slurp end**************************"

def write(record, writer, writertmp):
  print "*******************write**************************"
  #self.avro_writer.append(record)
  # BEGIN - Handle errors when writing into Avro storage
  try:
      print record
      writer.append(record)
      writertmp.append(record)

  except UnicodeDecodeError:
      sys.stderr.write("ERROR IN Writing EMAIL to Avro for UnicodeDecode issue, SKIPPED ONE\n")
      pass

  except Exception,e:
      print Exception, " : ", e
      pass
  # END - Handle errors when writing into Avro storage
  print "*******************write end**************************"

def flush():
  avro_writer.flush()
  avro_writertmp.flush()	# Handle Avro write errors
  print "Flushed avro writer..."

def fetch_email(imap, email_id):
  print "*******************fetch_email**************************"
  # def timeout_handler(signum, frame):
  #   raise self.TimeoutException()
  #
  # signal.signal(signal.SIGALRM, timeout_handler)
  # signal.alarm(30) # triger alarm in 30 seconds
  #
  # avro_record = dict()
  # status = 'FAIL'
  utils = EmailUtils()

  try:
    status, data = imap.fetch(str(email_id), '(X-GM-THRID RFC822)') # Gmail's X-GM-THRID will get the thread of the message
  except TimeoutException:
    return 'TIMEOUT', {}, None
  except:
    return 'ABORT', {}, None

  charset = None
  if status != 'OK':
    return 'ERROR', {}, None
  else:
    raw_thread_id = data[0][0]
    encoded_email = data[0][1]

  try:
    charset = utils.get_charset(encoded_email)

    # RFC2822 says default charset is us-ascii, which often saves us when no charset is specified
    if(charset):
      pass
    else:
      charset = 'us-ascii'

    if(charset): # redundant, but saves our ass if we edit above
      #raw_email = encoded_email.decode(charset)
      thread_id = utils.get_thread_id(raw_thread_id)
    #   print "CHARSET: " + charset
      avro_record, charset = utils.process_email(encoded_email, thread_id)
    else:
      return 'UNICODE', {}, charset
  except UnicodeDecodeError:
    return 'UNICODE', {}, charset
  except:
    return 'ERROR', {}, None

  # Without a charset we pass bad chars to avro, and it dies. See AVRO-565.
  if charset:
    return status, avro_record, charset
  else:
    return 'CHARSET', {}, charset
  print "*******************fetch_email end**************************"

if __name__ == "__main__":
  main()
