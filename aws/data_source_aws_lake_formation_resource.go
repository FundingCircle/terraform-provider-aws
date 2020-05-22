package aws

import (
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/service/lakeformation"
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
)

func dataSourceAwsLakeFormationResource() *schema.Resource {
	return &schema.Resource{
		Read: dataSourceAwsLakeFormationResourceRead,

		Schema: map[string]*schema.Schema{
			"arn": {
				Type:     schema.TypeString,
				Required: true,
			},
			"last_modified": {
				Type:     schema.TypeString,
				Computed: true,
			},
			"role_arn": {
				Type:     schema.TypeString,
				Computed: true,
			},
		},
	}
}

func dataSourceAwsLakeFormationResourceRead(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*AWSClient).lakeformationconn

	arn := d.Get("arn").(string)
	req := &lakeformation.DescribeResourceInput{
		ResourceArn: &arn,
	}
	log.Printf("[DEBUG] Reading Lake Formation Resource: %s", req)
	res, err := client.DescribeResource(req)
	if err != nil {
		return fmt.Errorf("error getting resource: %s", err)
	}

	d.Set("last_modified", res.ResourceInfo.LastModified.Format(time.RFC1123))
	d.Set("role_arn", res.ResourceInfo.RoleArn)

	return nil
}
