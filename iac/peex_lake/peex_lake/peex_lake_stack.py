"""Lake Formation stack."""

from aws_cdk import CfnOutput, RemovalPolicy, Stack
from aws_cdk import aws_glue as glue
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lakeformation as lakeformation
from aws_cdk import aws_s3 as s3
from constructs import Construct


class PeexLakeStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create S3 Bucket
        bronze_layer_bucket = s3.Bucket(
            self,
            "BronzeLayerBucket",
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
        )

        # Create IAM role for Lake Formation
        lake_formation_role = iam.Role(
            self,
            "LakeFormationServiceRole",
            assumed_by=iam.ServicePrincipal("lakeformation.amazonaws.com"),
            description="Role used by AWS Lake Formation to access data lake resources",
        )

        # Grant Lake Formation role access to the S3 bucket
        bronze_layer_bucket.grant_read_write(lake_formation_role)

        # Add additional required policies for Lake Formation
        lake_formation_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "glue:GetDatabase",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:CreateTable",
                    "glue:UpdateTable",
                    "glue:DeleteTable",
                    "glue:GetUserDefinedFunctions",
                ],
                resources=["*"],  # For POC. In production, you should restrict this
            )
        )

        # Set up Lake Formation Admin settings with IAM user
        cli_user_arn = f"arn:aws:iam::{Stack.of(self).account}:user/cli"

        lakeformation.CfnDataLakeSettings(
            self,
            "DataLakeSettings",
            admins=[
                lakeformation.CfnDataLakeSettings.DataLakePrincipalProperty(data_lake_principal_identifier=cli_user_arn)
            ],
        )

        # Register the S3 bucket as a Lake Formation location
        lakeformation.CfnResource(
            self, "RegisterS3Location", resource_arn=bronze_layer_bucket.bucket_arn, use_service_linked_role=True
        )

        # Create Lake Formation Database
        bronze_db = glue.CfnDatabase(
            self,
            "BronzeDatabase",
            catalog_id=Stack.of(self).account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="bronze_db",
                description="Bronze layer database for raw data",
                location_uri=f"s3://{bronze_layer_bucket.bucket_name}/",
            ),
        )

        # Grant Lake Formation permissions for the database to the admin user
        lakeformation.CfnPermissions(
            self,
            "BronzeDatabasePermissions",
            data_lake_principal=lakeformation.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=cli_user_arn
            ),
            resource=lakeformation.CfnPermissions.ResourceProperty(
                database_resource=lakeformation.CfnPermissions.DatabaseResourceProperty(name=bronze_db.ref)
            ),
            permissions=["ALL"],
            permissions_with_grant_option=["ALL"],
        )

        # Create IAM role for Glue Crawler
        crawler_role = iam.Role(
            self,
            "GlueCrawlerRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            description="Role used by AWS Glue Crawler to access data lake resources",
        )

        # Add required policies for the Glue Crawler role
        crawler_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
        )

        # Grant S3 bucket access to the Crawler role
        bronze_layer_bucket.grant_read(crawler_role)

        # Add Lake Formation permissions for the Crawler role
        lakeformation.CfnPermissions(
            self,
            "CrawlerDatabasePermissions",
            data_lake_principal=lakeformation.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=crawler_role.role_arn
            ),
            resource=lakeformation.CfnPermissions.ResourceProperty(
                database_resource=lakeformation.CfnPermissions.DatabaseResourceProperty(name=bronze_db.ref)
            ),
            permissions=["CREATE_TABLE", "ALTER", "DROP"],
        )

        # Create Glue Crawler
        glue.CfnCrawler(
            self,
            "BronzeLayerCrawler",
            name="bronze-layer-crawler",
            role=crawler_role.role_arn,
            database_name=bronze_db.ref,
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[glue.CfnCrawler.S3TargetProperty(path=f"s3://{bronze_layer_bucket.bucket_name}/")]
            ),
        )

        CfnOutput(
            self,
            "BronzeBucketName",
            value=bronze_layer_bucket.bucket_name,
            description="Name of the bronze layer S3 bucket",
        )
