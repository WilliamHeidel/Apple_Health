variable "S3_BUCKET_NAME" {
  description = "The S3 bucket where webhook data will be stored"
  type        = string
}

variable "EXPECTED_API_KEY" {
  description = "API key expected from Health Auto Export"
  type        = string
}
