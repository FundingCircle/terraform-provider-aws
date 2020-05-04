package aws

import (
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/lakeformation"
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
)

func dataSourceAwsLakeFormationDataLakeSettings() *schema.Resource {
	return &schema.Resource{
		Read: dataSourceAwsLakeFormationDataLakeSettingsRead,

		Schema: map[string]*schema.Schema{
			"create_database_default_permissions": {
				Type:     schema.TypeList,
				Computed: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"permissions": {
							Type:     schema.TypeList,
							Computed: true,
							Elem: &schema.Schema{Type: schema.TypeString},
						},
						"principal": {
							Type:     schema.TypeString,
							Computed: true,
						},
					},
				},
			},
			"create_table_default_permissions": {
				Type:     schema.TypeList,
				Computed: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"permissions": {
							Type:     schema.TypeList,
							Computed: true,
							Elem: &schema.Schema{Type: schema.TypeString},
						},
						"principal": {
							Type:     schema.TypeString,
							Computed: true,
						},
					},
				},
			},
			"data_lake_admins": {
				Type:     schema.TypeList,
				Computed: true,
				Elem: &schema.Schema{Type: schema.TypeString},
			},
		},
	}
}

func dataSourceAwsLakeFormationDataLakeSettingsRead(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*AWSClient).lakeformationconn

	log.Printf("[DEBUG] Reading Lake Formation Data Lake Settings")
	
	accountId := meta.(*AWSClient).accountid
	req := &lakeformation.GetDataLakeSettingsInput{
		CatalogId: accountId
	}
	res, err := client.GetDataLakeSettings(req)
	if err != nil {
		return fmt.Errorf("error getting data lake settings: %s", err)
	}

	cddpList := flattenLakeFormationPrincipalPermissions(res.DataLakeSettings.CreateDatabaseDefaultPermissions)
	ctdpList := flattenLakeFormationPrincipalPermissions(res.DataLakeSettings.CreateTableDefaultPermissions)

	if err := d.Set("create_database_default_permissions", cddpList); err != nil {
		return fmt.Errorf("error setting create_database_default_permissions: %s", err)
	}

	if err := d.Set("create_table_default_permissions", ctdpList); err != nil {
		return fmt.Errorf("error setting create_table_default_permissions: %s", err)
	}

	admins := make([string], 0, len(settings.DataLakeAdmins))
	for _, admObj := range settings.DataLakeAdmins {
		admins = append(admins, admObj.DataLakePrincipalIdentifier)
	}

	if err := d.Set("data_lake_admins", admins); err != nil {
		return fmt.Errorf("error setting data_lake_admins: %s", err)
	}

	return nil
}
