#!/usr/bin/env python

#
# derived from helpful example at http://www.harshj.com/2010/04/25/writing-and-reading-avro-data-files-using-python/
#
from avro import schema, datafile, io
import pprint

# Test writing avros
OUTFILE_NAME = 'messages.avro'

SCHEMA_STR = """{
   "type":"record",
   "name":"Email",
   "fields":[
      {
         "name":"message_id",
         "type":[
            "null",
            "string"
         ]
      },
      {
         "name":"thread_id",
         "type":[
            "null",
            "string"
         ],
         "doc":""
      },
      {
         "name":"in_reply_to",
         "type":[
            "string",
            "null"
         ]
      },
      {
         "name":"subject",
         "type":[
            "string",
            "null"
         ]
      },
      {
         "name":"body",
         "type":[
            "string",
            "null"
         ]
      },
      {
         "name":"date",
         "type":[
            "string",
            "null"
         ]
      },
      {
         "name":"from",
         "type":
         {
           "type":"record",
           "name":"from",
           "fields":[
              {
                 "name":"real_name",
                 "type":[
                    "null",
                    "string"
                 ],
                 "doc":""
              },
              {
                 "name":"address",
                 "type":[
                    "null",
                    "string"
                 ],
                 "doc":""
               }
            ]
         }
      },
      {
         "name":"tos",
         "type":[
            "null",
            {
               "type":"array",
               "items":[
                  "null",
                  {
                     "type":"record",
                     "name":"to",
                     "fields":[
                        {
                           "name":"real_name",
                           "type":[
                              "null",
                              "string"
                           ],
                           "doc":""
                        },
                        {
                           "name":"address",
                           "type":[
                              "null",
                              "string"
                           ],
                           "doc":""
                        }
                     ]
                  }
               ]
            }
         ],
         "doc":""
      },
      {
         "name":"ccs",
         "type":[
            "null",
            {
               "type":"array",
               "items":[
                  "null",
                  {
                     "type":"record",
                     "name":"cc",
                     "fields":[
                        {
                           "name":"real_name",
                           "type":[
                              "null",
                              "string"
                           ],
                           "doc":""
                        },
                        {
                           "name":"address",
                           "type":[
                              "null",
                              "string"
                           ],
                           "doc":""
                        }
                     ]
                  }
               ]
            }
         ],
         "doc":""
      },
      {
         "name":"bccs",
         "type":[
            "null",
            {
               "type":"array",
               "items":[
                  "null",
                  {
                     "type":"record",
                     "name":"bcc",
                     "fields":[
                        {
                           "name":"real_name",
                           "type":[
                              "null",
                              "string"
                           ],
                           "doc":""
                        },
                        {
                           "name":"address",
                           "type":[
                              "null",
                              "string"
                           ],
                           "doc":""
                        }
                     ]
                  }
               ]
            }
         ],
         "doc":""
      }
   ]
}
"""

SCHEMA = schema.parse(SCHEMA_STR)

# Create a 'record' (datum) writer
rec_writer = io.DatumWriter(SCHEMA)

# Create a 'data file' (avro file) writer
df_writer = datafile.DataFileWriter(
  open(OUTFILE_NAME, 'wb'),
  rec_writer,
  writers_schema = SCHEMA
)

df_writer.append( {'body': '', 'from': {'address': 'mail.other4@dnspod.cn', 'real_name': 'DNSPod'}, 'tos': [{'address': 'aloocs@gmail.com', 'real_name': 'None'}], 'reply_tos': 'None', 'ccs': 'None', 'thread_id': '1501568107948821503', 'bccs': 'None', 'date': '2015-05-18T20:42:18', 'in_reply_to': 'None', 'message_id': '555ab11a.a86d460a.3a84.78d8SMTPIN_ADDED_MISSING@mx.google.com', 'subject': '=?utf-8?b?RE5TUG9k5o+Q6YaS77ya5oKo55qE5biQ5oi3YWxvb2NzQGdtYWlsLmNvbQ==?=\r\n =?utf-8?b?5oiQ5Yqf55m76ZmGRE5TUG9k?='} )
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
