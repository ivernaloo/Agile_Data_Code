#!/usr/bin/env python

#
# derived from helpful example at http://www.harshj.com/2010/04/25/writing-and-reading-avro-data-files-using-python/
#
from avro import schema, datafile, io
import pprint
import json
# Test writing avros
OUTFILE_NAME = 'qq.avro'
schema_path  = './qq.avro.schema'

schemas = open(schema_path, 'r').read()
SCHEMA = schema.parse(schemas)

# Create a 'record' (datum) writer
rec_writer = io.DatumWriter(SCHEMA)

# Create a 'data file' (avro file) writer
df_writer = datafile.DataFileWriter(
  open(OUTFILE_NAME, 'wb'),
  rec_writer,
  writers_schema = SCHEMA
)
df_writer.append({'body': u'Generic Email - 2\r\nhttp://view.exacttarget.com/?j=fe54107971650c7a7d16&m=fe6a15707267067f7710&ls=fde01d79776d0d757c117076&l=fe661771746504797d13&s=fdf315707763027b7413757c&jb=ff5e137877&ju= \r\nView this email in your browser .\r\n\r\nDear ivern,\r\n\r\n\r\n\r\nHave you explored your genetics lately? Discover something new about your ancestry with one of the site features below!\r\n\r\n- The 23andMe Team\r\n\r\n\r\n \r\nDNA Relatives\r\nBrowse the 23andMe genealogical database for your close relatives and find distant cousins when segments of your DNA match identically with other people.\r\nhttp://www.23andme.com/you/relfinder/\r\n\r\n\r\nAncestry Composition\r\nUse the information from your DNA to help you discover your ancestral origins and trace your lineage around the world.\r\nhttps://www.23andme.com/you/ancestry/composition/ \r\n\r\n\r\nNeanderthal Ancestry\r\nSee what percentage of your DNA stems from Neanderthals, and compare it to your family and friends. \r\nhttps://www.23andme.com/you/labs/neanderthal/ \r\n\r\n\r\nMaternal Line\r\nTrace what part of your DNA you got from your mother, via her mother and so on for the last 10,000 years and discover over 750 maternal lines.\r\nhttps://www.23andme.com/you/haplogroup/maternal/ \r\n\r\n\r\n---------------------------------\r\n\r\nYou are receiving this email because you are a customer of 23andMe.\r\n\r\n23andMe, Inc. uses the clinical laboratory services of\r\n\r\nNational Genetics Institute, a subsidiary of Laboratory Corporation,\r\n\r\n2440 S Sepulveda Blvd., Ste #130, Los Angeles, CA 90064.\r\n\r\n(c) 2007-2015 23andMe, Inc.\r\n \r\n23andMe, Inc.\r\n899 W. Evelyn Avenue\r\nMountain View, CA 94041\r\nUSA\r\n\r\nhttp://cl.exct.net/unsub_center.aspx?s=fdf315707763027b7413757c&j=fe54107971650c7a7d16&mid=fe6a15707267067f7710&lid=fe661771746504797d13&jb=ff5e137877&ju= \r\nunsubscribe from 23andMe news and updates emails \r\n\r\n', 'from': {'address': 'lists@23andme.com', 'real_name': '23andMe'}, 'tos': [{'address': 'aloocs@gmail.com', 'real_name': None}], 'reply_tos': None, 'ccs': None, 'thread_id': '1502455980357624956', 'bccs': None, 'date': '2015-05-28T15:41:32', 'in_reply_to': 'None', 'message_id': '2aca2b75-6dab-4ae1-bfff-a57e72a83761@xtinmta4100.xt.local', 'subject': u'Revisit your genetic ancestry'})
df_writer.close()

# Test reading avros
rec_reader = io.DatumReader()

# Create a 'data file' (avro file) reader
df_reader = datafile.DataFileReader(
  open(OUTFILE_NAME),
  rec_reader
)

# Read all records stored inside
pp = pprint.PrettyPrinter()
for record in df_reader:
  pp.pprint(record)
