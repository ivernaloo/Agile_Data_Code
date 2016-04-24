# Test writing avros
from avro import schema, datafile, io
import pprint

OUTFILE_NAME = '/tmp/messages.avro'
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
