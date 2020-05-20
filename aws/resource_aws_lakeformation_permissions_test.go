package aws

import (
	"fmt"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/helper/acctest"
	"github.com/hashicorp/terraform-plugin-sdk/helper/resource"
)

func TestAccAWSLakeFormationPermissions_full(t *testing.T) {
	rName := acctest.RandomWithPrefix("lakeformation-test-bucket")
	dName := acctest.RandomWithPrefix("lakeformation-test-db")
	tName := acctest.RandomWithPrefix("lakeformation-test-table")

	callerIdentityName := "data.aws_caller_identity.current"
	roleName := "data.aws_iam_role.test"
	resourceName := "aws_lakeformation_permissions.test"
	bucketName := "aws_s3_bucket.test"
	dbName := "aws_glue_catalog_database.test"
	tableName := "aws_glue_catalog_table.test"

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:  func() { testAccPreCheck(t) },
		Providers: testAccProviders,
		// TODO: CheckDestroy
		Steps: []resource.TestStep{
			{
				Config: testAccAWSLakeFormationPermissionsConfig_catalog(),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckResourceAttrAccountID(resourceName, "catalog_id"),
					resource.TestCheckResourceAttrPair(callerIdentityName, "account_id", resourceName, "catalog_id"),
					resource.TestCheckResourceAttrPair(roleName, "arn", resourceName, "principal"),
					resource.TestCheckResourceAttr(resourceName, "permissions.#", "1"),
					resource.TestCheckResourceAttr(resourceName, "permissions.0", "CREATE_DATABASE"),
				),
			},
			{
				Config: testAccAWSLakeFormationPermissionsConfig_location(rName),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckResourceAttrAccountID(resourceName, "catalog_id"),
					resource.TestCheckResourceAttrPair(callerIdentityName, "account_id", resourceName, "catalog_id"),
					resource.TestCheckResourceAttrPair(roleName, "arn", resourceName, "principal"),
					resource.TestCheckResourceAttrPair(bucketName, "arn", resourceName, "location"),
					resource.TestCheckResourceAttr(resourceName, "permissions.#", "1"),
					resource.TestCheckResourceAttr(resourceName, "permissions.0", "DATA_LOCATION_ACCESS"),
				),
			},
			{
				Config: testAccAWSLakeFormationPermissionsConfig_database(rName, dName),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckResourceAttrAccountID(resourceName, "catalog_id"),
					resource.TestCheckResourceAttrPair(callerIdentityName, "account_id", resourceName, "catalog_id"),
					resource.TestCheckResourceAttrPair(roleName, "arn", resourceName, "principal"),
					resource.TestCheckResourceAttrPair(dbName, "name", resourceName, "database"),
					resource.TestCheckResourceAttr(resourceName, "permissions.#", "3"),
					resource.TestCheckResourceAttr(resourceName, "permissions.0", "ALTER"),
					resource.TestCheckResourceAttr(resourceName, "permissions.1", "CREATE_TABLE"),
					resource.TestCheckResourceAttr(resourceName, "permissions.2", "DROP"),
					resource.TestCheckResourceAttr(resourceName, "permissions_with_grant_option.#", "1"),
					resource.TestCheckResourceAttr(resourceName, "permissions_with_grant_option.0", "CREATE_TABLE"),
				),
			},
			{
				Config: testAccAWSLakeFormationPermissionsConfig_table(rName, dName, tName, "\"ALL\""),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckResourceAttrAccountID(resourceName, "catalog_id"),
					resource.TestCheckResourceAttrPair(callerIdentityName, "account_id", resourceName, "catalog_id"),
					resource.TestCheckResourceAttrPair(roleName, "arn", resourceName, "principal"),
					resource.TestCheckResourceAttr(resourceName, "table.#", "1"),
					resource.TestCheckResourceAttrPair(tableName, "database_name", resourceName, "table.0.database"),
					resource.TestCheckResourceAttrPair(tableName, "name", resourceName, "table.0.name"),
					resource.TestCheckResourceAttr(resourceName, "permissions.#", "1"),
					resource.TestCheckResourceAttr(resourceName, "permissions.0", "ALL"),
				),
			},
			// FIXME: more than one permission in API read result (in acceptance tests setup)
			{
				Config: testAccAWSLakeFormationPermissionsConfig_table(rName, dName, tName, "\"ALL\", \"SELECT\""),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckResourceAttrAccountID(resourceName, "catalog_id"),
					resource.TestCheckResourceAttrPair(callerIdentityName, "account_id", resourceName, "catalog_id"),
					resource.TestCheckResourceAttrPair(roleName, "arn", resourceName, "principal"),
					resource.TestCheckResourceAttr(resourceName, "table.#", "1"),
					resource.TestCheckResourceAttrPair(tableName, "database_name", resourceName, "table.0.database"),
					resource.TestCheckResourceAttrPair(tableName, "name", resourceName, "table.0.name"),
					resource.TestCheckResourceAttr(resourceName, "permissions.#", "2"),
					resource.TestCheckResourceAttr(resourceName, "permissions.0", "ALL"),
					resource.TestCheckResourceAttr(resourceName, "permissions.1", "SELECT"),
				),
			},
			// FIXME: WIP
			// {
			// 	Config: testAccAWSLakeFormationPermissionsConfig_tableWithColumns(rName, dName, tName),
			// 	Check: resource.ComposeTestCheckFunc(
			// 		testAccCheckResourceAttrAccountID(resourceName, "catalog_id"),
			// 		resource.TestCheckResourceAttrPair(callerIdentityName, "account_id", resourceName, "catalog_id"),
			// 		resource.TestCheckResourceAttrPair(roleName, "arn", resourceName, "principal"),
			// 		resource.TestCheckResourceAttr(resourceName, "table.#", "1"),
			// 		resource.TestCheckResourceAttrPair(tableName, "database_name", resourceName, "table.0.database"),
			// 		resource.TestCheckResourceAttrPair(tableName, "name", resourceName, "table.0.name"),
			// 		resource.TestCheckResourceAttr(resourceName, "table.0.column_names.#", "2"),
			// 		resource.TestCheckResourceAttr(resourceName, "table.0.column_names.0", "event"),
			// 		resource.TestCheckResourceAttr(resourceName, "table.0.column_names.1", "timestamp"),
			// 		resource.TestCheckResourceAttr(resourceName, "permissions.#", "1"),
			// 		resource.TestCheckResourceAttr(resourceName, "permissions.0", "SELECT"),
			// 	),
			// },
		},
	})
}

func testAccAWSLakeFormationPermissionsConfig_catalog() string {
	return `
data "aws_caller_identity" "current" {}

data "aws_iam_role" "test" {
  name = "AWSServiceRoleForLakeFormationDataAccess"
}

resource "aws_lakeformation_datalake_settings" "test" {
  admins = [
    data.aws_caller_identity.current.arn
  ]
}

resource "aws_lakeformation_permissions" "test" {
  permissions = ["CREATE_DATABASE"]
  principal   = data.aws_iam_role.test.arn
}
`
}

func testAccAWSLakeFormationPermissionsConfig_location(rName string) string {
	return fmt.Sprintf(`
data "aws_caller_identity" "current" {}

data "aws_iam_role" "test" {
  name = "AWSServiceRoleForLakeFormationDataAccess"
}

resource "aws_s3_bucket" "test" {
  bucket = %[1]q
}

resource "aws_lakeformation_datalake_settings" "test" {
  admins = [
    data.aws_caller_identity.current.arn
  ]
}

resource "aws_lakeformation_resource" "test" {
  resource_arn            = aws_s3_bucket.test.arn
  use_service_linked_role = true
}

resource "aws_lakeformation_permissions" "test" {
  permissions = ["DATA_LOCATION_ACCESS"]
  principal   = data.aws_iam_role.test.arn

  location = aws_lakeformation_resource.test.resource_arn
}
`, rName)
}

func testAccAWSLakeFormationPermissionsConfig_database(rName, dName string) string {
	return fmt.Sprintf(`
data "aws_caller_identity" "current" {}

data "aws_iam_role" "test" {
  name = "AWSServiceRoleForLakeFormationDataAccess"
}

resource "aws_s3_bucket" "test" {
  bucket = %[1]q
}

resource "aws_glue_catalog_database" "test" {
  name = %[2]q
}

resource "aws_lakeformation_datalake_settings" "test" {
  admins = [
    data.aws_caller_identity.current.arn
  ]
}

resource "aws_lakeformation_permissions" "test" {
  permissions = ["ALTER", "CREATE_TABLE", "DROP"]
  permissions_with_grant_option = ["CREATE_TABLE"]
  principal   = data.aws_iam_role.test.arn

  database = aws_glue_catalog_database.test.name
}
`, rName, dName)
}

func testAccAWSLakeFormationPermissionsConfig_table(rName, dName, tName, permissions string) string {
	return fmt.Sprintf(`
data "aws_caller_identity" "current" {}

data "aws_iam_role" "test" {
  name = "AWSServiceRoleForLakeFormationDataAccess"
}

resource "aws_s3_bucket" "test" {
  bucket = %[1]q
}

resource "aws_glue_catalog_database" "test" {
  name = %[2]q
}

resource "aws_glue_catalog_table" "test" {
  name          = %[3]q
  database_name = aws_glue_catalog_database.test.name
}

resource "aws_lakeformation_datalake_settings" "test" {
  admins = [
    data.aws_caller_identity.current.arn
  ]
}

resource "aws_lakeformation_permissions" "test" {
  permissions = [%s]
  principal   = data.aws_iam_role.test.arn

  table {
  	database = aws_glue_catalog_table.test.database_name
  	name = aws_glue_catalog_table.test.name
  }
}
`, rName, dName, tName, permissions)
}

/* func testAccAWSLakeFormationPermissionsConfig_tableWithColumns(rName, dName, tName string) string {
	return fmt.Sprintf(`
data "aws_caller_identity" "current" {}

data "aws_iam_role" "test" {
  name = "AWSServiceRoleForLakeFormationDataAccess"
}

resource "aws_s3_bucket" "test" {
  bucket = %[1]q
}

resource "aws_glue_catalog_database" "test" {
  name = %[2]q
}

resource "aws_glue_catalog_table" "test" {
  name          = %[3]q
  database_name = aws_glue_catalog_database.test.name

  storage_descriptor {
    columns {
      name = "event"
      type = "string"
    }
    columns {
      name = "timestamp"
      type = "date"
    }
    columns {
      name = "value"
      type = "double"
    }
  }
}

resource "aws_lakeformation_datalake_settings" "test" {
  admins = [
    data.aws_caller_identity.current.arn
  ]
}

resource "aws_lakeformation_permissions" "test" {
  permissions = ["SELECT"]
  principal   = data.aws_iam_role.test.arn

  table {
  	database = aws_glue_catalog_table.test.database_name
  	name = aws_glue_catalog_table.test.name
  	column_names = ["event", "timestamp"]
  }
}
`, rName, dName, tName)
} */
