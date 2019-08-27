import pandas as pd
import boto3
import json
import configparser

def delete_cluster(DWH_CLUSTER_IDENTIFIER,KEY,SECRET):
    redshift = boto3.client('redshift',
                       region_name='us-west-2',
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET)
    redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)

def remove_resources(DWH_IAM_ROLE_NAME,KEY,SECRET):
    iam = boto3.client('iam',
                       region_name='us-west-2',
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET)
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)   

def main():
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    
    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    
    DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")

    KEY = config.get('AWS','KEY')
    SECRET = config.get('AWS','SECRET')
    
    delete_cluster(DWH_CLUSTER_IDENTIFIER,KEY,SECRET)
    
    remove_resources(DWH_IAM_ROLE_NAME,KEY,SECRET)
    print('Deleted Cluster and Removed Resources')

if __name__ == '__main__':
    main()