variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-west-2"
}

variable "bucket_name" {
  description = "S3 bucket name for data storage"
  type        = string
  default     = "automated-trading-data-bucket"
}