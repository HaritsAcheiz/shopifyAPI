import httpx
from dataclasses import dataclass
import creds
import json

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

    def main(self):
        client = self.create_session()
        data = {

            "query": '''{
                shop{
                    name
                }
            }'''

            # "query": '''{
            #     products(first: 3) {
            #         edges {
            #             node {
            #                 id
            #                 title
            #             }
            #         }
            #     }
            # }'''
            # "variables": '''{
            #
            # }'''

        }
        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2023-07/graphql.json', json=data)
        print(response)
        print(response.json())


if __name__ == '__main__':
    s = ShopifyApp()
    s.main()