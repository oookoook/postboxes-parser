# How to deploy

<https://docs.aws.amazon.com/AmazonECS/latest/userguide/docker-basics.html>

Replace the 

```cmd
docker build -t postobxes-parser .

docker tag postboxes-parser aws_account_id.dkr.ecr.region.amazonaws.com/postboxes-parser
```

Then:

```cmd
aws ecr get-login-password | docker login --username AWS --password-stdin aws_account_id.dkr.ecr.region.amazonaws.com
```

I don't have the AWS in Windows, so I have to run the first part `aws ecr get-login-password` in Ubuntu and then copy and paste to the Windows to login docker:

```cmd
echo "password" | docker login --username AWS --password-stdin aws_account_id.dkr.ecr.region.amazonaws.com/postboxes-parser
```

And the last step:

```cmd
docker push aws_account_id.dkr.ecr.region.amazonaws.com/postboxes-parser
```
