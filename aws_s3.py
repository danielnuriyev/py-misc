import boto3

s3 = boto3.resource('s3')

bucket = s3.Bucket('is2-custom-ui-components')

# list operation:
print('list:')
for object in bucket.objects.all():
    print(object)

# put:
data = b'Here we have some data'
object = s3.Object('is2-custom-ui-components', 'test.txt')
object.put(Body=data)
print('put test.txt:')
for object in bucket.objects.all():
    print(object)
# overwrite:
data = b'Here we have some data 2'
object.put(Body=data)
overwrite = object.get()['Body'].read().decode('utf8')
print(overwrite)

#delete:
object.delete()
print('after deleting:')
for object in bucket.objects.all():
    print(object)
