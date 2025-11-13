
Log in as UMBMIAWS-CMS-Staging-Policy
```
aws sts get-caller-identity
```


Make s3 bucket
```
aws s3 ls

aws s3 mb s3://gpc-cistem2-srtr --region us-east-2
```

Upload a single file

```
# Navigate to the parent folder where the target file is located
ls

aws s3 cp pubsaf2409.exe s3://gpc-cistem2-srtr

aws s3 cp ./pubsaf2409/ s3://gpc-cistem2-srtr
```

Upload all files wihint a target folder
```
# Navigate to the parent folder where the target folder is located
ls 

aws s3 sync pubsaf2409/ s3://gpc-cistem2-srtr/

```
