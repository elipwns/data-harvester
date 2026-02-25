# Terraform Resource Tagging Standard

## Why Tag Resources?

Tagging all Terraform-managed AWS resources helps:
- **Track what's managed** across multiple repos
- **Find unmanaged resources** in your AWS account
- **Cost allocation** by project
- **Compliance** and auditing

## Required Tags

Add these default tags to your AWS provider:

```hcl
# terraform/provider.tf
provider "aws" {
  region = var.aws_region
  
  default_tags {
    tags = {
      ManagedBy     = "Terraform"
      Project       = "YOUR_PROJECT_NAME"        # e.g., "my-app"
      TerraformRepo = "github.com/USER/REPO"     # e.g., "github.com/elipwns/my-app"
    }
  }
}
```

## How to Apply

1. Add the `default_tags` block to your provider
2. Update `Project` and `TerraformRepo` values
3. Run `terraform plan` to see what will be tagged
4. Run `terraform apply` to apply tags to all resources

**Note:** This only tags resources created/managed by Terraform. Existing resources will be tagged on next `terraform apply`.

## Finding Unmanaged Resources

### Option 1: AWS CLI
```bash
# Find resources WITHOUT ManagedBy tag
aws resourcegroupstaggingapi get-resources \
  --tag-filters Key=ManagedBy,Values='' \
  --resource-type-filters lambda dynamodb apigateway sns iam
```

### Option 2: AWS Resource Explorer
1. Enable in AWS Console → Resource Explorer
2. Query: `NOT tag.key:ManagedBy`
3. Shows all untagged resources

### Option 3: List by repo
```bash
# Find resources managed by specific repo
aws resourcegroupstaggingapi get-resources \
  --tag-filters Key=TerraformRepo,Values=github.com/elipwns/timing-system
```

## Tag Standards

| Tag | Required | Example | Purpose |
|-----|----------|---------|---------|
| `ManagedBy` | Yes | `Terraform` | Identifies Terraform-managed resources |
| `Project` | Yes | `timing-system` | Groups resources by project |
| `TerraformRepo` | Yes | `github.com/elipwns/timing-system` | Links to source repo |
| `Environment` | Optional | `prod`, `dev`, `staging` | Separates environments |
| `Owner` | Optional | `team-name` or `email` | Identifies responsible party |

## Multi-Repo Strategy

When managing AWS resources across multiple repos:

1. **Each repo tags with its own name**
   ```hcl
   TerraformRepo = "github.com/elipwns/repo-a"  # in repo A
   TerraformRepo = "github.com/elipwns/repo-b"  # in repo B
   ```

2. **Query all Terraform-managed resources**
   ```bash
   aws resourcegroupstaggingapi get-resources \
     --tag-filters Key=ManagedBy,Values=Terraform
   ```

3. **Find resources NOT managed by any repo**
   ```bash
   aws resourcegroupstaggingapi get-resources \
     --tag-filters Key=ManagedBy,Values='' \
     --resource-type-filters lambda dynamodb
   ```

## Troubleshooting

**Q: Tags not showing up after apply?**
- Some resources don't support tags (e.g., Lambda permissions)
- Check AWS docs for resource-specific tag support

**Q: Can I tag existing resources?**
- Yes! Terraform will tag them on next `apply` after adding `default_tags`

**Q: What if I have resources in multiple AWS accounts?**
- Add `Account` tag with account ID or alias
- Use separate provider configs per account

## Example: Complete Provider Config

```hcl
terraform {
  required_version = ">= 1.5.0"
  
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
  
  default_tags {
    tags = {
      ManagedBy     = "Terraform"
      Project       = "my-project"
      TerraformRepo = "github.com/myorg/my-repo"
      Environment   = var.environment  # optional
      Owner         = "platform-team"  # optional
    }
  }
}
```

## References

- [AWS Provider Default Tags](https://registry.terraform.io/providers/hashicorp/aws/latest/docs#default_tags)
- [AWS Resource Groups Tagging API](https://docs.aws.amazon.com/resourcegroupstagging/latest/APIReference/Welcome.html)
- [AWS Resource Explorer](https://docs.aws.amazon.com/resource-explorer/latest/userguide/welcome.html)
