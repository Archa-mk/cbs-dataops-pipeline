variable "aws_region" {
  default = "eu-north-1"
}

variable "source_bucket" {
  default = "financial-demo-source-data"
}

variable "sink_bucket" {
  default = "financial-demo-sink-data"
}

variable "script_bucket" {
  default = "financial-glue-scripts"
}