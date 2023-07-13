import httpx
from dataclasses import dataclass
import creds
import json
import os
import pandas as pd

@dataclass
class ShopifyApp:
    store_name: str = creds.store_name
    access_token: str = creds.access_token


    def create_session(self):
        client = httpx.Client()
        headers = {
            'X-Shopify-Access-Token': self.access_token,
            'Content-Type': 'application/json'
        }
        client.headers.update(headers)
        return client

    def query_shop(self, client):
        data = {

            "query": '''{
                shop{
                    name
                }
            }'''
        }

        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2023-07/graphql.json', json=data)
        print(response)
        print(response.json())

    def query_product(self, client):
        data = {
            "query": '''{
                products(first: 3) {
                    edges {
                        node {
                            id
                            title
                        }
                    }
                }
            }'''
        }

        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2023-07/graphql.json', json=data)
        print(response)
        print(response.json())

    def create_product(self, client):
        data = {
            "query": '''
                mutation {
                    productCreate(
                        input: {
                            handle: "BAB061"
                            title: "Xmas Rocks Beavis And Butt-Head Hoodie",
                            productType: "Hoodies",
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
        }

        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2023-07/graphql.json', json=data)
        print(response)
        print(response.json())

    def generate_upload_path(self, client):
        data = {"query": '''
                    mutation {
                        stagedUploadsCreate(
                            input:{
                                resource: BULK_MUTATION_VARIABLES,
                                filename: "bulk_op_vars",
                                mimeType: "text/jsonl",
                                httpMethod: POST
                            }
                        )
                    {
                    userErrors{
                        field,
                        message
                    },
                    stagedTargets{
                        url,
                        resourceUrl,
                        parameters {
                            name,
                            value
                        }
                    }
                '''
                }

        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2023-07/graphql.json')
        print(response)
        print(response.json())

    def create_products(self, client):
        data = {
            "query": '''
                mutation {
                    bulkOperationMutation(
                        input: {
                            clientIdentifier: ""
                            mutation: "",
                            stagedUploadPath: "",    
                        }    
                    )
                }
            '''
        }

        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2023-07/graphql.json', json=data)
        print(response)
        print(response.json())

    def csv_to_jsonl(self, input_filename, output_filename):
        csvfile = pd.read_csv(os.path.join(os.getcwd(), input_filename), encoding='utf-16')

        jsonfile = open(os.path.join(os.getcwd(), output_filename), 'w')
        print(csvfile.to_json(orient='records', lines=True), file=jsonfile, flush=False)
        jsonfile.close()


if __name__ == '__main__':
    s = ShopifyApp()
    client = s.create_session()
    # s.query_shop(client)
    # s.query_product(client)
    # s.create_product(client)
    s.csv_to_jsonl(input_filename='result.csv', output_filename='result.jsonl')
    # s.generate_upload_path(client)
