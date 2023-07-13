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

    # def create_product(self, client):
    #     data = '''
    #         mutation {
    #             productCreate(
    #                 input: {
    #                     title: "Xmas Rocks Beavis And Butt-Head Hoodie",
    #                     productType: "Hoodies",
    #                     vendor: "My Store",
    #                     variants: [
    #                         {
    #                             title: "Default",
    #                             price: "79.99",
    #                             inventoryManagement: SHOPIFY,
    #                             inventoryPolicy: DENY,
    #                             inventoryQuantity: 10
    #                         }
    #                     ]
    #                 },
    #                 media: {
    #                     originalSource: "//80steess3.imgix.net/production/products/BAB061/xmas-rocks-beavis-and-butt-head-hoodie.master.png?w=500&h=750&fit=fill&usm=12&sat=15&fill-color=00FFFFFF&auto=compress,format&q=40&nr=15",
    #                     mediaContentType: IMAGE
    #                 }
    #             ) {
    #                 product {
    #                     id
    #                     title
    #                 }
    #             }
    #         }
    #     '''
    #     response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2023-07/graphql.json', json=data)
    #     print(response)
    #     print(response.json())

if __name__ == '__main__':
    s = ShopifyApp()
    client = s.create_session()
    s.query_shop(client)
    s.query_product(client)
    s.create_product(client)
