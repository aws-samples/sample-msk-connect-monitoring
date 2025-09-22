# -*- coding: utf-8 -*-
"""
Created on Mon June 02 12:30:22 2025

@authors: ayanmaju, swapnaba 
"""

import yaml
import json
import boto3
import sys

###############################################  Input  Parameters ########################################
"""
ConnectorName     = 'ConnectorName'                              #-- your MSK Connector name
MSKConnectRegion  = 'us-east-1'                                  #-- your MSK Connector region
DashboardName     = 'AWS_MSKConnect_Metrics_Dashboard'           #-- Name of the CloudWach dashboard
Purpose           = 'Generates a CloudWatch dashboard YAML template for monitoring Amazon MSK Connect metrics'
"""
###########################################################################################################



# Get user inputs with validation
ConnectorName = input('MSK Connector name: ')
if not ConnectorName:
    print("Error: Connector name cannot be empty")
    exit(1)

MSKRegion = input('MSK Connector region: ')
if not MSKRegion:
    print("Error: Region cannot be empty")
    exit(1)

DashboardName = input('CloudWatch dashboard name: ')
if not DashboardName:
    print("Error: Dashboard name cannot be empty")
    sys.exit(1)

deploy_choice = input('Deploy to AWS automatically? (y/n): ').lower()

print("Executing the script...")

# Define dashboard body as a JSON string
DashboardBody     = '''
                        {
                            "variables": [
                                {
                                    "type": "property",
                                    "property": "Connector Name",
                                    "inputType": "input",
                                    "id": "Connector",
                                    "label": "Connector Name",
                                    "defaultValue": "''' + ConnectorName + '''",
                                    "visible": true
                                },
                                {
                                    "type": "property",
                                    "property": "region",
                                    "inputType": "input",
                                    "id": "region",
                                    "label": "region",
                                    "defaultValue": "''' + MSKRegion + '''",
                                    "visible": true
                                }
                            ],
                            "widgets": [
                                {
                                    "height": 6,
                                    "width": 6,
                                    "y": 0,
                                    "x": 0,
                                    "type": "metric",
                                    "properties": {
                                        "metrics": [
                                            [ "AWS/KafkaConnect", "BytesInPerSec", "ConnectorName", "''' + ConnectorName + '''", { "region": "''' + MSKRegion + '''", "color": "#1f77b4" } ]
                                        ],
                                        "view": "timeSeries",
                                        "stacked": false,
                                        "region": "''' + MSKRegion + '''",
                                        "stat": "Maximum",
                                        "period": 300,
                                        "title": "Bytes In Per Sec"
                                    }
                                },
                                {
                                "height": 6,
                                "width": 6,
                                "y": 0,
                                "x": 6,
                                "type": "metric",
                                "properties": {
                                    "metrics": [
                                        [ "AWS/KafkaConnect", "BytesOutPerSec", "ConnectorName", "''' + ConnectorName + '''", { "region": "''' + MSKRegion + '''", "color": "#ff7f0e" } ]
                                    ],
                                    "view": "timeSeries",
                                    "stacked": false,
                                    "region": "''' + MSKRegion + '''",
                                    "stat": "Maximum",
                                    "period": 300,
                                    "title": "Bytes Out Per Sec"
                                    }
                                },
                                {
                                "height": 6,
                                "width": 6,
                                "y": 0,
                                "x": 12,
                                "type": "metric",
                                "properties": {
                                    "metrics": [
                                        [ "AWS/KafkaConnect", "CpuUtilization", "ConnectorName", "''' + ConnectorName + '''", { "color": "#1f77b4", "region": "''' + MSKRegion + '''" } ]
                                    ],
                                    "view": "timeSeries",
                                    "stacked": false,
                                    "region": "''' + MSKRegion + '''",
                                    "stat": "Maximum",
                                    "period": 300,
                                    "title": "CPU Utilization"
                                    }
                                },
                                {
                                "height": 6,
                                "width": 6,
                                "y": 0,
                                "x": 18,
                                "type": "metric",
                                "properties": {
                                    "metrics": [
                                        [ "AWS/KafkaConnect", "MemoryUtilization", "ConnectorName", "''' + ConnectorName + '''", { "color": "#1f77b4", "region": "''' + MSKRegion + '''" } ]
                                    ],
                                    "view": "timeSeries",
                                    "stacked": false,
                                    "region": "''' + MSKRegion + '''",
                                    "stat": "Maximum",
                                    "period": 300,
                                    "title": "Memory Used"
                                    }
                                },
                                {
                                "height": 6,
                                "width": 6,
                                "y": 6,
                                "x": 0,
                                "type": "metric",
                                "properties": {
                                    "metrics": [
                                        [ "AWS/KafkaConnect", "ErroredTaskCount", "ConnectorName", "''' + ConnectorName + '''", { "region": "''' + MSKRegion + '''", "color": "#d62728" } ]
                                    ],
                                    "view": "timeSeries",
                                    "stacked": false,
                                    "region": "''' + MSKRegion + '''",
                                    "stat": "Maximum",
                                    "period": 300,
                                    "title": "Number of Errored Task"
                                    }
                                },
                                {
                                "height": 6,
                                "width": 6,
                                "y": 6,
                                "x": 6,
                                "type": "metric",
                                "properties": {
                                    "metrics": [
                                        [ "AWS/KafkaConnect", "RunningTaskCount", "ConnectorName", "''' + ConnectorName + '''", { "region": "''' + MSKRegion + '''", "color": "#1f77b4" } ]
                                    ],
                                    "view": "timeSeries",
                                    "stacked": false,
                                    "region": "''' + MSKRegion + '''",
                                    "stat": "Maximum",
                                    "period": 300,
                                    "title": "Number of Running Task"
                                    }
                                },
                                {
                                "height": 6,
                                "width": 6,
                                "y": 6,
                                "x": 12,
                                "type": "metric",
                                "properties": {
                                    "metrics": [
                                        [ "AWS/KafkaConnect", "WorkerCount", "ConnectorName", "''' + ConnectorName + '''", { "color": "#1f77b4", "region": "''' + MSKRegion + '''" } ]
                                    ],
                                    "view": "timeSeries",
                                    "stacked": false,
                                    "region": "''' + MSKRegion + '''",
                                    "stat": "Maximum",
                                    "period": 300,
                                    "title": "Worker Count"
                                    }
                                }
                            ]
                        }'''

# Create CloudFormation template
cf_template = {
    "AWSTemplateFormatVersion": "2010-09-09",
    "Description": "CloudWatch Dashboard for MSK Connect Metrics",
    "Resources": {
        "MSKConnectDashboard": {
            "Type": "AWS::CloudWatch::Dashboard",
            "Properties": {
                "DashboardName": DashboardName,
                "DashboardBody": DashboardBody
            }
        }
    },
    "Outputs": {
        "DashboardURL": {
            "Description": "CloudWatch Dashboard URL",
            "Value": {
                "Fn::Sub": f"https://{MSKRegion}.console.aws.amazon.com/cloudwatch/home?region={MSKRegion}#dashboards:name={DashboardName}"
            }
        }
    }
}

# Save YAML file
yaml_filename = f"{DashboardName}.yaml"
with open(yaml_filename, 'w') as f:
    yaml.dump(cf_template, f, default_flow_style=False)

print(f"CloudFormation template saved as: {yaml_filename}")

# Deploy to AWS if requested
if deploy_choice == 'y':
    try:
        cf_client = boto3.client('cloudformation', region_name=MSKRegion)
        
        with open(yaml_filename, 'r') as f:
            template_body = f.read()
        
        stack_name = f"{DashboardName}-stack"
        
        print(f"Deploying stack: {stack_name}")
        response = cf_client.create_stack(
            StackName=stack_name,
            TemplateBody=template_body
        )
        
        print(f"Stack deployment initiated. Stack ID: {response['StackId']}")
        
        # Wait for stack creation and get outputs
        print("Waiting for stack creation to complete...")
        waiter = cf_client.get_waiter('stack_create_complete')
        waiter.wait(StackName=stack_name)
        
        # Get stack outputs
        stack_info = cf_client.describe_stacks(StackName=stack_name)
        outputs = stack_info['Stacks'][0].get('Outputs', [])
        
        print("\n✅ Stack deployed successfully!")
        for output in outputs:
            if output['OutputKey'] == 'DashboardURL':
                dashboard_url = output['OutputValue']
                print(f"\n🔗 Dashboard URL: {dashboard_url}")
                print("Click the link above to view your CloudWatch dashboard")
        
    except Exception as e:
        print(f"Deployment failed: {str(e)}")
        dashboard_url = f"https://{MSKRegion}.console.aws.amazon.com/cloudwatch/home?region={MSKRegion}#dashboards:name={DashboardName}"
        print(f"Manual dashboard URL: {dashboard_url}")

else:
    dashboard_url = f"https://{MSKRegion}.console.aws.amazon.com/cloudwatch/home?region={MSKRegion}#dashboards:name={DashboardName}"
    print(f"\n📋 After manual deployment, access dashboard at: {dashboard_url}")

print("\nScript completed successfully!")