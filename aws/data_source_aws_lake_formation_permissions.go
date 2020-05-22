package aws

import (
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/service/lakeformation"
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
)

func dataSourceAwsLakeFormationPermissions() *schema.Resource {
	return &schema.Resource{
		Read: dataSourceAwsLakeFormationPermissionsRead,

		Schema: map[string]*schema.Schema{
			"resource_arn": {
				Type:     schema.TypeString,
				Required: true,
			},
			"principal_resource_permissions": {
				Type:     schema.TypeList,
				Computed: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"permissions": {
							Type:     schema.TypeList,
							Computed: true,
							Elem:     &schema.Schema{Type: schema.TypeString},
						},
						"permissions_with_grant_option": {
							Type:     schema.TypeList,
							Computed: true,
							Elem:     &schema.Schema{Type: schema.TypeString},
						},
						"principal": {
							Type:     schema.TypeString,
							Computed: true,
						},
						"resource": {
							Type:     schema.TypeString,
							Computed: true,
						},
					},
				},
			},
		},
	}
}

func dataSourceAwsLakeFormationPermissionsRead(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*AWSClient).lakeformationconn
	accountId := meta.(*AWSClient).accountid

	arn := d.Get("resource_arn").(string)
	req := &lakeformation.GetEffectivePermissionsForPathInput{
		CatalogId:   &accountId,
		ResourceArn: &arn,
	}
	log.Printf("[DEBUG] Reading Lake Formation Permissions: %s", req)

	prp := []*lakeformation.PrincipalResourcePermissions{}
	err := client.GetEffectivePermissionsForPathPages(req, func(resp *lakeformation.GetEffectivePermissionsForPathOutput, isLast bool) bool {
		prp = append(prp, resp.Permissions...)
		return !isLast
	})
	if err != nil {
		return fmt.Errorf("error getting permissions: %s", err)
	}

	permissions := flattenLakeFormationPrincipalResourcePermissions(prp)

	d.Set("principal_resource_permissions", permissions)

	return nil
}
