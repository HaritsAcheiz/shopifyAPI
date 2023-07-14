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
        client = httpx.Client()
        headers = {
            'X-Shopify-Access-Token': self.access_token,
            'Content-Type': 'application/json'
        }
        client.headers.update(headers)
        return client

    def query_shop(self, client):
        data = {"query":
                '''
                {
                    shop{
                        name
                    }
                }
                '''
        }

        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2023-07/graphql.json', json=data)
        print(response)
        print(response.json())

    def query_product(self, client):
        data = {
            "query":
            '''
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
        }

        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2023-07/graphql.json', json=data)
        print(response)
        print(response.json())

    def create_product(self, client):
        data = {"query":
                '''
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

    def generate_staged_target(self, client):
        data = {"query":
                '''
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
        }

        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2023-07/graphql.json', json=data)
        print(response)
        print(response.json())
        return response.json()

    def create_products(self, client, csv_filename, jsonl_filename):
        self.csv_to_jsonl(csv_filename=csv_filename, jsonl_filename=jsonl_filename)
        self.generate_staged_target(client)
        self.upload_jsonl(client)
        self.import_bulk_data(client)
        data = {"query":
                '''
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

    def csv_to_jsonl(self, csv_filename, jsonl_filename):
        csvfile = pd.read_csv(os.path.join(os.getcwd(), csv_filename), encoding='utf-16')

        jsonfile = open(os.path.join(os.getcwd(), jsonl_filename), 'w')
        print(csvfile.to_json(orient='records', lines=True), file=jsonfile, flush=False)
        jsonfile.close()

    def upload_jsonl(self, staged_target, jsonl_path):
        url = staged_target['data']['stagedUploadsCreate']['stagedTargets'][0]['url']
        parameters = staged_target['data']['stagedUploadsCreate']['stagedTargets'][0]['parameters']
        files = dict()
        # for parameter in parameters:
        #     files[f"{parameter['name']}"] = parameter['value']
        # files['file'] = open(jsonl_path, 'rb')
        files = {
            'key': (None, 'tmp/79384576314/bulk/81c018e3-95b6-4bb7-8806-bbcc83bc559e/bulk_op_vars.jsonl'),
            'x-goog-credential': (
            None, 'merchant-assets@shopify-tiers.iam.gserviceaccount.com/20230714/auto/storage/goog4_request'),
            'x-goog-algorithm': (None, 'GOOG4-RSA-SHA256'),
            'x-goog-date': (None, '20230714T214628Z'),
            'x-goog-signature': (None,
                                 '9a7e6fe4d4812d5ff62386ea5efc0981866e6f5d9f72de2495d464e355a4cb5aafdfa10f89a121766428c14bbccdf28d2670d2566971501f7cebb9ae32c6353aa16fafdbfea2421196432bdb49b7dde79f912d9f801645d96d085aa6431830c0c9f1fd5390514438c01819826bc9c91d2a33af98c07e94c81796fb4d9bb1f4652791a689c532df1fe61c60ebd0e12431108ed9b8867a97ea0278972b3da7ed2e5edacc9b47c9b22c3acc39ce54f98a587328b650b8a6fca7582a896f15746e2aba733439498a998fd544b06bfc4871e6ffd11d66bcbdc702eeda3da81d3fd48f0fc6662d0bbb561893783efe4b682be8ae6be22beed1c9aefd5d8b7c4a6e4355'),
            'policy': (None,
                       'eyJjb25kaXRpb25zIjpbeyJDb250ZW50LVR5cGUiOiJ0ZXh0XC9qc29ubCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0seyJhY2wiOiJwcml2YXRlIn0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMSwyMDk3MTUyMF0seyJidWNrZXQiOiJzaG9waWZ5LXN0YWdlZC11cGxvYWRzIn0seyJrZXkiOiJ0bXBcLzc5Mzg0NTc2MzE0XC9idWxrXC84MWMwMThlMy05NWI2LTRiYjctODgwNi1iYmNjODNiYzU1OWVcL2J1bGtfb3BfdmFycy5qc29ubCJ9LHsieC1nb29nLWRhdGUiOiIyMDIzMDcxNFQyMTQ2MjhaIn0seyJ4LWdvb2ctY3JlZGVudGlhbCI6Im1lcmNoYW50LWFzc2V0c0BzaG9waWZ5LXRpZXJzLmlhbS5nc2VydmljZWFjY291bnQuY29tXC8yMDIzMDcxNFwvYXV0b1wvc3RvcmFnZVwvZ29vZzRfcmVxdWVzdCJ9LHsieC1nb29nLWFsZ29yaXRobSI6IkdPT0c0LVJTQS1TSEEyNTYifV0sImV4cGlyYXRpb24iOiIyMDIzLTA3LTE1VDIxOjQ2OjI4WiJ9'),
            'acl': (None, 'private'),
            'Content-Type': (None, 'text/jsonl'),
            'success_action_status': (None, '201'),
            'file': open('D:/Naru/shopifyAPI/bulk_op_vars.jsonl', 'rb'),
        }

        with httpx.Client() as sess:
            response = sess.post(url, files=files)

        print(response)
        print(response.content)
        print(response.request.headers)

    def import_bulk_data(self, client):
        pass


if __name__ == '__main__':
    s = ShopifyApp()
    client = s.create_session()
    # s.query_shop(client)
    # s.query_product(client)
    # s.create_product(client)
    # s.csv_to_jsonl(input_filename='result.csv', output_filename='bulk_op_vars.jsonl')
    staged_target = s.generate_staged_target(client)
    s.upload_jsonl(staged_target=staged_target, jsonl_path="D:/Naru/shopifyAPI/bulk_op_vars.jsonl")
