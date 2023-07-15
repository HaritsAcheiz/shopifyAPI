import httpx
from dataclasses import dataclass
import creds
import json
import os
import pandas as pd
from urllib.parse import urljoin

@dataclass
class ShopifyApp:
    store_name: str = creds.store_name
    access_token: str = creds.access_token


    def create_session(self):
        print("Creating session...")
        client = httpx.Client()
        headers = {
            'X-Shopify-Access-Token': self.access_token,
            'Content-Type': 'application/json'
        }
        client.headers.update(headers)
        return client

    def query_shop(self, client):
        print("Fetching shop data...")
        query = '''
                {
                    shop{
                        name
                    }
                }
                '''

        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2023-07/graphql.json',
                               json={"query": query})
        print(response)
        print(response.json())
        print('')

    def query_product(self, client):
        print("Fetching product data...")
        query = '''
                {
                    products(first: 3) {
                        edges {
                            node {
                                id
                                title
                            }
                        }
                    }
                }
                '''

        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2023-07/graphql.json',
                               json={"query": query})
        print(response)
        print(response.json())
        print('')

    def create_product(self, client):
        print("Creating product...")
        mutation = '''
                    mutation {
                        productCreate(
                            input: {
                                handle: "BAB063"
                                title: "Xmas Rocks Beavis And Butt-Head Shirt",
                                productType: "Shirts",
                                vendor: "MyStore"
                                variants: [
                                    {
                                        title: "Default",
                                        price: "79.99",
                                        inventoryManagement: SHOPIFY,
                                        inventoryPolicy: DENY
                                    }
                                ]
                            }
                            media: {
                                originalSource: "https://80steess3.imgix.net/production/products/BAB061/xmas-rocks-beavis-and-butt-head-hoodie.master.png",
                                mediaContentType: IMAGE
                            }    
                        )
                        {
                            product {
                                id
                            }
                        }
                    }
                    '''

        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2023-07/graphql.json',
                               json={"query": mutation})
        print(response)
        print(response.json())
        print('')

    def generate_staged_target(self, client):
        print("Creating stage upload...")
        mutation = '''
                    mutation {
                        stagedUploadsCreate(
                            input:{
                                resource: BULK_MUTATION_VARIABLES,
                                filename: "bulk_op_vars.jsonl",
                                mimeType: "text/jsonl",
                                httpMethod: POST
                            }
                        )
                        {
                            userErrors{
                                field,
                                message
                            }
                            stagedTargets{
                                url,
                                resourceUrl,
                                parameters {
                                    name,
                                    value
                                }    
                            }
                        }
                    }
                    '''

        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2023-07/graphql.json',
                               json={"query": mutation})
        print(response)
        print(response.json())
        print('')
        return response.json()

    def create_products(self, client, staged_target):
        mutation = '''
                    mutation {
                        bulkOperationRunMutation(
                            mutation: "mutation call($input: ProductInput!) { productCreate(input: $input) { product {id title variants(first: 10) {edges {node {id title inventoryQuantity }}}} userErrors { message field } } }",
                            stagedUploadPath: $stagedUploadPath
                        )
                        {
                            bulkOperation {
                                id
                                url
                                status
                            }
                            userErrors {
                                message
                                field
                            }
                        }
                    }
                    '''

        variables = {
            "stagedUploadPath": staged_target['data']['stagedUploadsCreate']['stagedTargets'][0]['parameters'][4]['value']
        }

        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2023-07/graphql.json',
                               json={"query": mutation, "variables": variables})
        print(response)
        print(response.json())
        print('')

    def csv_to_jsonl(self, csv_filename, jsonl_filename):
        print("Converting csv to jsonl file...")
        csvfile = pd.read_csv(os.path.join(os.getcwd(), csv_filename), encoding='utf-16')

        jsonfile = open(os.path.join(os.getcwd(), jsonl_filename), 'w')
        print(csvfile.to_json(orient='records', lines=True), file=jsonfile, flush=False)
        jsonfile.close()
        print('')

    def upload_jsonl(self, staged_target, jsonl_path):
        print("Uploading jsonl file to staged path...")
        url = staged_target['data']['stagedUploadsCreate']['stagedTargets'][0]['url']
        parameters = staged_target['data']['stagedUploadsCreate']['stagedTargets'][0]['parameters']
        files = dict()
        for parameter in parameters:
            files[f"{parameter['name']}"] = (None, parameter['value'])
        files['file'] = open(jsonl_path, 'rb')

        with httpx.Client() as sess:
            response = sess.post(url, files=files)

        print(response)
        print(response.content)
        print('')

    def import_bulk_data(self, client, csv_filename, jsonl_filename):
        self.csv_to_jsonl(csv_filename=csv_filename, jsonl_filename=jsonl_filename)
        staged_target = self.generate_staged_target(client)
        self.upload_jsonl(staged_target=staged_target, jsonl_path=jsonl_filename)
        self.create_products(client, staged_target=staged_target)

if __name__ == '__main__':
    s = ShopifyApp()
    client = s.create_session()
    s.query_shop(client)
    s.query_product(client)
    s.create_product(client)
    s.csv_to_jsonl(csv_filename='result.csv', jsonl_filename='test2.jsonl')
    staged_target = s.generate_staged_target(client)
    s.upload_jsonl(staged_target=staged_target, jsonl_path="D:/Naru/shopifyAPI/bulk_op_vars.jsonl")
