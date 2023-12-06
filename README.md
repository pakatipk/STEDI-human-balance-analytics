# STEDI-human-balance-analytics

STEDI Step Trainer is a hardware developed to:
  - trains the user to do a STEDI balance exercise;
  - collects data to train a machine-learning algorithm to detect steps;
  - and collects customer data using mobile app and interacts with the device sensors.

Several customers have already received their Step Trainers, installed the mobile application, and begun using them together to test their balance. The Step Trainer is just a motion sensor that records the distance of the object detected. The app uses a mobile phone accelerometer to detect motion in the X, Y, and Z directions.

The STEDI team wants to use the motion sensor data to train a machine learning model to detect steps accurately in real-time. Privacy will be a primary consideration in deciding what data can be used.

Some of the early adopters have agreed to share their data for research purposes. Only these customers’ Step Trainer and accelerometer data should be used in the training data for the machine learning model.

This project is using AWS Glue to generate Python scripts to build a lakehouse solution in that satisfies these requirements from the data scientist team.

## Data Sources 
- customer
- step_trainer
- accelerometer

## Configuration
1. Create S3 bucket 
```bash
  aws s3 mb s3://your-bucket-name
```

2. Create an S3 Gateway Endpoint
```bash
  aws ec2 create-vpc-endpoint --vpc-id your-vpc-id --service-name com.amazonaws.us-east-1.s3 --route-table-ids your-route-table-ids
```
Note: use AWS CLI `aws ec2 describe-vpcs` and `aws ec2 describe-route-tables` to get both of the ids

3. Create IAM role
```bash
  aws iam create-role --role-name my-glue-service-role --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "glue.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}'
```

4. Grant Glue Privileges on the S3 Bucket
```bash
aws iam put-role-policy --role-name my-glue-service-role --policy-name S3Access --policy-document 
'{ 
    "Version": "2012-10-17", 
    "Statement": [ 
        { 
            "Sid": "ListObjectsInBucket", 
            "Effect": "Allow", 
            "Action": [ "s3:ListBucket" ],
            "Resource": [ "arn:aws:s3:::your-bucket-name" ] 
        }, 
        { 
            "Sid": "AllObjectActions", 
            "Effect": "Allow", 
            "Action": "s3:*Object", 
            "Resource": [ "arn:aws:s3:::your-bucket-name/*" ] 
        } 
    ] 
}'
```

5. Give Glue access to data in S3 buckets
```bash
aws iam put-role-policy --role-name my-glue-service-role --policy-name GlueAccess --policy-document '{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "glue:*",
                "s3:GetBucketLocation",
                "s3:ListBucket",
                "s3:ListAllMyBuckets",
                "s3:GetBucketAcl",
                "ec2:DescribeVpcEndpoints",
                "ec2:DescribeRouteTables",
                "ec2:CreateNetworkInterface",
                "ec2:DeleteNetworkInterface",
                "ec2:DescribeNetworkInterfaces",
                "ec2:DescribeSecurityGroups",
                "ec2:DescribeSubnets",
                "ec2:DescribeVpcAttribute",
                "iam:ListRolePolicies",
                "iam:GetRole",
                "iam:GetRolePolicy",
                "cloudwatch:PutMetricData"
            ],
            "Resource": [
                "*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:CreateBucket",
                "s3:PutBucketPublicAccessBlock"
            ],
            "Resource": [
                "arn:aws:s3:::aws-glue-*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::aws-glue-*/*",
                "arn:aws:s3:::*/*aws-glue-*/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject"
            ],
            "Resource": [
                "arn:aws:s3:::crawler-public*",
                "arn:aws:s3:::aws-glue-*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents",
                "logs:AssociateKmsKey"
            ],
            "Resource": [
                "arn:aws:logs:*:*:/aws-glue/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "ec2:CreateTags",
                "ec2:DeleteTags"
            ],
            "Condition": {
                "ForAllValues:StringEquals": {
                    "aws:TagKeys": [
                        "aws-glue-service-resource"
                    ]
                }
            },
            "Resource": [
                "arn:aws:ec2:*:*:network-interface/*",
                "arn:aws:ec2:*:*:security-group/*",
                "arn:aws:ec2:*:*:instance/*"
            ]
        }
    ]
}'
```
    
## How to reproduce

1. Create Glue Table `customer_landing` from customer data from the Website (Landing Zone)
   
2. Create a Glue Table `customer_trusted` by filtering out Customer Records who did not agree to share their data for research purposes
   ```bash
     SELECT * FROM customer_landing WHERE sharewithresearchasofdate IS NOT NULL;
   ```
3. Create a Glue Table `accelerometer_landing` from the accelerometer data from the Mobile App
   
4. Create a Glue Table `accelerometer_trusted` which stores only the `accelerometer` readings from customers who agreed to share their data for research purposes (Trusted Zone)
   ```bash
     SELECT user, timestamp, x, y, z FROM customer_trusted c \
            JOIN accelerometer_landing a ON c.email = a.user;
   ```
   
5. Create a Glue Table `customer_curated` that only includes customers who have accelerometer data and have agreed to share their data for research

6. Create a Glue Table `steptrainer_landing` from the Step Trainer data

7. Create a Glue Table `steptrainer_trusted` that contains the Step Trainer Records data for customers who have accelerometer data and have agreed to share their data for research (customers_curated).
   ```bash
     SELECT serialnumber, sensorreadingtime, distancefromobject FROM customer_curated c \
            JOIN steptrainer_landing s a ON c.serialnumber = s.s_serialnumber;
   ```
Note that serialnumber field from `steptrainer_landing` is renamed to s_serialnumber to prevent inconsistency of outputs

8. Create Glue Table `machine_learning_curated` that has each of the Step Trainer Readings, and the associated accelerometer reading data for the same timestamp, but only for customers who have agreed to share their data

  ```bash
    SELECT DISDINCT * FROM customer_curated c \
           JOIN accelerometer_trusted a ON a.sensorreadings = c.timestamps
  ```


## Link to program

[Data Engineering with AWS Nanodegree Program](https://www.udacity.com/course/data-engineer-nanodegree--nd027)

