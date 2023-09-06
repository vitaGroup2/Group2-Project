#------ Creating Bucket ------------#

resource "aws_s3_bucket" "bucket1" {
  bucket = "terraform-tejas758-prac"
  tags = {
    Name = "My bucket"
  }
}

resource "aws_s3_bucket_ownership_controls" "bucket1" {
  bucket = aws_s3_bucket.bucket1.id
  rule {
    object_ownership = "BucketOwnerPreferred"
  }
}


resource "aws_s3_bucket_public_access_block" "bucket1" {
  bucket = aws_s3_bucket.bucket1.id

  block_public_acls       = false
  block_public_policy     = false
  ignore_public_acls      = false
  restrict_public_buckets = false
}

resource "aws_s3_bucket_acl" "bucket1" {
  depends_on = [
    aws_s3_bucket_ownership_controls.bucket1,
    aws_s3_bucket_public_access_block.bucket1,
  ]

  bucket = aws_s3_bucket.bucket1.id
  acl    = "public-read"
}


#--------------- Uploading Pyscript on S3 ------------------#

resource "aws_s3_object" "upload-glue-script-1" {
  bucket = aws_s3_bucket.bucket1.id
  key    = "first_job.py"
  source = "./first_job.py"
}

resource "aws_s3_object" "upload-glue-script-2" {
  bucket = aws_s3_bucket.bucket1.id
  key    = "second_job.py"
  source = "./second_job.py"
}

#------ Creating Bucket 2 output ------------#

resource "aws_s3_bucket" "bucket2" {
  bucket = "terraform-nikhil-prac"
  tags = {
    Name = "My bucket"
  }
}

resource "aws_s3_bucket_ownership_controls" "bucket2" {
  bucket = aws_s3_bucket.bucket2.id
  rule {
    object_ownership = "BucketOwnerPreferred"
  }
}


resource "aws_s3_bucket_public_access_block" "bucket2" {
  bucket = aws_s3_bucket.bucket2.id

  block_public_acls       = false
  block_public_policy     = false
  ignore_public_acls      = false
  restrict_public_buckets = false
}

resource "aws_s3_bucket_acl" "bucket2" {
  depends_on = [
    aws_s3_bucket_ownership_controls.bucket2,
    aws_s3_bucket_public_access_block.bucket2,
  ]

  bucket = aws_s3_bucket.bucket2.id
  acl    = "public-read-write"
}

output "aws_s3_bucket_uri" {
  value = aws_s3_bucket.bucket2.bucket_domain_name
}


#------------------ Redshift Resource ----------------#

# AWS REDSHIFT CLUSTER 


# resource "aws_redshift_cluster" "redshiftCluster1" {
#   cluster_identifier = "tf-redshift-cluster"
#   database_name      = "dev"
#   master_username    = "nikhil"
#   master_password    = "#Nikhil33"
#   node_type          = "dc2.large"
#   cluster_type       = "single-node"
# }

# resource "aws_redshift_cluster_iam_roles" "redshiftCluster1" {
#   cluster_identifier = aws_redshift_cluster.redshiftCluster1.cluster_identifier
#   iam_role_arns      = ["arn:aws:iam::300758866129:role/LabRole"]
# }

# output "redshift_cluster_endpoint" {
#   value = aws_redshift_cluster.redshiftCluster1.endpoint
# }

# # create Lambda Function

# # 1st create lambda function and copy the lambda ARN and paste in cloudwatch block 

# resource "aws_lambda_function" "glue_job_trigger_lambda" {
#  filename                       = "${path.module}/lm.zip"
#  function_name                  = "Jhooq-Lambda-Function"
#  role                           = "arn:aws:iam::300758866129:role/LabRole"
#  handler                        = "lm.py"
#  runtime                        = "python3.8"
# }

#-------------------- GLUE JOB 1 - RDS + S3 -------------------------#

resource "aws_glue_job" "glue_job_1" {
  name = "Data-Merging"
  role_arn = "arn:aws:iam::300758866129:role/LabRole"
  description = "Combining Data from RDS and S3"
  max_retries = "0"
  timeout = 60
  number_of_workers = 3
  worker_type = "Standard"
  command {
    script_location = "s3://terraform-tejas758-prac/first_job.py"
    python_version = "3"
  }
  glue_version = "4.0"
}


#---------------------- GLUE JOB 2 - Transforming ---------------------------------#

resource "aws_glue_job" "glue_job_2" {
  name = "Data-Transformation"
  role_arn = "arn:aws:iam::300758866129:role/LabRole"
  description = "Cleaning and Modifying the Table"
  max_retries = "0"
  timeout = 60
  number_of_workers = 3
  worker_type = "Standard"
  command {
    script_location = "s3://terraform-tejas758-prac/second_job.py"
    python_version = "3"
  }
  glue_version = "4.0"
}

#------------------- STEP FUNCTION TO TRIGGER GLUE JOB ---------------#
#  Define an SNS topic :

resource "aws_sns_topic" "glue_job_notification" {
  name = "glue-job-notification-topic"
}

#---------- STEP FUNCTION TO TRIGGER GLUE JOB AND NOTIFY---------------#
resource "aws_sfn_state_machine" "glue_job_trigger" {
  name     = "glue-job-trigger"
  role_arn = "arn:aws:iam::300758866129:role/LabRole"

  definition = <<EOF
{
  "Comment": "Firstly, Combining Data and Secondly Cleaning and modifying the data",
  "StartAt": "GlueJob1",
  "States": {
    "GlueJob1": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {
        "JobName": "${aws_glue_job.glue_job_1.name}"
      },
      "Next": "SNSPublish1"
    },
    "SNSPublish1": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "${aws_sns_topic.glue_job_notification.arn}",
        "Message": "Greetings Group 2,\n\nYour Glue Job 1 is completed successfully."
      },
      "Next": "GlueJob2"
    },
    "GlueJob2": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {
        "JobName": "${aws_glue_job.glue_job_2.name}"
      },
      "Next": "SNSPublish2"
    },
    "SNSPublish2": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "${aws_sns_topic.glue_job_notification.arn}",
        "Message": "Greetings Group 2,\n\nYour Glue Job 2 is completed successfully\n\nThe Data is Loaded to Redshift Successfully."
      },
      "End": true
    }
  }
}
EOF
}



