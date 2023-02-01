
import smtplib
from datetime import datetime

csvs = []
csv = []
keys = sdc.records[0].value.keys()
for key in keys:
  csv.append(key)
csvs.append(', '.join([str(x) for x in csv]))
csvs.append('---')
for record in sdc.records:
  try:
    sdc.output.write(record)
    csv = []
    for key in keys:
      csv.append(record.value[key])

    line = ', '.join([str(x) for x in csv])
    csvs.append(line)

  except Exception as e:
    # Send record to error
    sdc.error.write(record, str(e))

formatted_csv = '\n'.join([str(x) for x in csvs])

pipeline_parameters = sdc.pipelineParameters()

server = "smtp.gmail.com"
from_address = pipeline_parameters['from_address']
to_address = [pipeline_parameters['to_address']] # must be a list
subject = "{} Query Summary: ${job:name()}".format(datetime.date(datetime.now()))
message = 'Subject: {}\n\n{}'.format(subject, formatted_csv)

mailserver = smtplib.SMTP('smtp.gmail.com',587)
# identify ourselves to smtp gmail client
mailserver.ehlo()
# secure our email with tls encryption
mailserver.starttls()
# re-identify ourselves as an encrypted connection
mailserver.ehlo()
mailserver.login(from_address, pipeline_parameters['password'])

mailserver.sendmail(from_address, to_address, message)

mailserver.quit()