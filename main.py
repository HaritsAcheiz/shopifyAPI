from httpx import Client
from dataclasses import dataclass
import json
import os
import pandas as pd
from urllib.parse import urljoin
from datetime import datetime
from dotenv import load_dotenv


@dataclass
class ShopifyApp:
    store_name: str = None
    access_token: str = None
    client: Client() = None
    api_version: str = '2025-07'

    # Support
    def send_request(self, query, variables=None):
        if self.client:
            response = self.client.post(
                f'https://{self.store_name}.myshopify.com/admin/api/{self.api_version}/graphql.json',
                json={"query": query, "variables": variables}
            )

            print(response)
            print(response.json())
            print('')

            return response.json()

        else:
            print('Please create session before execute the function')

    # Create
    # ===================================== Session ====================================
    def create_session(self):
        print("Creating session...")
        client = Client()
        headers = {
            'X-Shopify-Access-Token': self.access_token,
            'Content-Type': 'application/json'
        }
        client.headers.update(headers)
        self.client = client

    # ===================================== Products ===================================
    def create_product(self, variables):
        print("Creating product...")
        mutation = '''
            mutation (
                $handle: String,
                $title: String,
                $descriptionHtml: String,
                $vendor: String,
                $category: ID,
                $productType: String,
                $tags: [String!],
                $productOptions:[OptionCreateInput!],
                $media: [CreateMediaInput!],
                $giftCard:Boolean,
                $seo: SEOInput,
                $status: ProductStatus
            )

            {
                productCreate(
                    product: {
                        handle: $handle,
                        title: $title,
                        descriptionHtml: $descriptionHtml,
                        vendor: $vendor,
                        category: $category,
                        productType: $productType,
                        tags: $tags,
                        productOptions: $productOptions,
                        giftCard: $giftCard,
                        seo: $seo,
                        status: $status
                    }
                    media: $media
                )

                {
                    product {
                        id
                    }
                }
            }
        '''

        response = self.send_request(query=mutation, variables=variables)
        product_data = response

        if variables['published'] is True:
            publication_data = self.query_publication()
            publication_input = [{'publicationId': item['id']} for item in publication_data['data']['publications']['nodes']]
            self.publish_product(product_id=product_data['data']['productCreate']['product']['id'], publication_input=publication_input)

        self.create_variants(product_id=product_data['data']['productCreate']['product']['id'], variants=variables['variants'], media=variables['media'], strategy='REMOVE_STANDALONE_VARIANT')

    # =================================== staged_target ================================
    def generate_staged_target(self):
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

        if self.client:
            response = self.client.post(
                f'https://{self.store_name}.myshopify.com/admin/api/{self.api_version}/graphql.json',
                json={"query": mutation}
            )

            print(response)
            print(response.json())
            print('')

            return response.json()

        else:
            print('Please create session before execute the function')

    # ================================== Create Variants ===============================
    def create_variants(self, product_id, variants, media, strategy='DEFAULT'):
        print('Creating Variants...')
        mutation = '''
            mutation productVariantsBulkCreate($productId: ID!, $variants: [ProductVariantsBulkInput!]!, $media: [CreateMediaInput!], $strategy: ProductVariantsBulkCreateStrategy) {
                productVariantsBulkCreate(productId: $productId, variants: $variants, media: $media, strategy: $strategy) {
                    product {
                        id
                    }
                    productVariants {
                        id
                        metafields(first: 1) {
                            edges {
                                node {
                                    namespace
                                    key
                                    value
                                }
                            }
                        }
                    }
                    userErrors {
                      field
                      message
                    }
                }
            }
        '''

        variables = {
            'productId': product_id,
            'variants': variants,
            'media': media,
            'strategy': strategy
        }

        return self.send_request(query=mutation, variables=variables)

    # Read
    # ====================================== Shop ======================================
    def query_shop(self):
        print("Fetching shop data...")
        query = '''
                {
                    shop{
                        name
                    }
                }
                '''

        self.send_request(query=query)

    # ===================================== Products ===================================
    def query_products(self):
        print("Fetching product data...")
        query = '''
                {
                    products(first: 250) {
                        edges {
                            node {
                                handle
                                id
                                title
                            }
                        }
                    }
                }
                '''

        self.send_request(query=query)

    # ============================= get_products_id_by_handle ==========================
    def get_products_id_by_handle(self, handles):
        print('Getting product id...')
        f_handles = ','.join(handles)
        query = '''
            query(
                $query: String
            )
            {
                products(first: 250, query: $query) {
                    edges {
                        node {
                            handle
                            id
                        }
                    }
                    pageInfo {
                        endCursor
                        hasNextPage
                    }
                }
            }
        '''
        variables = {'query': "handle:{}".format(f_handles)}

        return self.send_request(query=query, variables=variables)

    # =================================== Publications =================================
    def query_publication(self):
        print("Fetching publications data...")
        query = '''
            {
                publications(first: 250) {
                    nodes{
                        id
                    }
                }
            }
        '''

        return self.send_request(query=query)

    # Update
    # ===================================== Publish Product ====================================
    def publish_product(self, product_id, publication_input):
        print("Publishing product...")
        publish_mutation = '''
            mutation publishablePublish($id: ID!, $input: [PublicationInput!]!) {
                publishablePublish(id: $id, input: $input) {
                    publishable {
                        availablePublicationsCount {
                            count
                        }
                        resourcePublicationsCount {
                            count
                        }
                    }
                    shop {
                        publicationCount
                    }
                    userErrors {
                        field
                        message
                    }
                }
            }
        '''

        publish_variables = {
            "id": product_id,
            "input": publication_input
        }

        return self.send_request(query=publish_mutation, variables=publish_variables)

    # Delete
    # ===================================== Product ====================================
    def delete_products_by_handle(self, handles):
        print('Deleting Product...')

        response = self.get_products_id_by_handle(handles=handles)
        try:
            found_flag = response['data']['products']['edges'][0]
            product_ids = [item['node']['id'] for item in response['data']['products']['edges']]
            for product_id in product_ids:
                mutation = '''
                    mutation productDelete($input: ProductDeleteInput!) {
                        productDelete(input: $input) {
                            deletedProductId
                        }
                    }
                '''

                variables = {
                    'input': {
                        'id': product_id
                    }
                }

                self.send_request(query=mutation, variables=variables)

        except IndexError:
            print('Item Not Found')

    def create_products(self, client, staged_target):
        print('Creating products...')
        # mutation = '''
        #             mutation ($stagedUploadPath: String!){
        #                 bulkOperationRunMutation(
        #                     mutation: "mutation call($input: ProductInput!)
        #                     { productCreate(input: $input) { product {id title variants(first: 10) {edges {node {id title inventoryQuantity }}}} userErrors { message field } } }",
        #                     stagedUploadPath: $stagedUploadPath
        #                 )
        #                 {
        #                     bulkOperation {
        #                         id
        #                         url
        #                         status
        #                     }
        #                     userErrors {
        #                         message
        #                         field
        #                     }
        #                 }
        #             }
        #             '''

        mutation = '''
                            mutation ($stagedUploadPath: String!){
                                bulkOperationRunMutation(
                                    mutation: "mutation call($input: ProductInput!, $media: [CreateMediaInput!])
                                    { productCreate(input: $input, media: $media) { product {id title variants(first: 10) {edges {node {id title inventoryQuantity }}}} userErrors { message field } } }",
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
            "stagedUploadPath": staged_target['data']['stagedUploadsCreate']['stagedTargets'][0]['parameters'][3]['value']
        }

        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2023-07/graphql.json',
                               json={"query": mutation, "variables": variables})

        print(response)
        print(response.json())
        print('')

    def csv_to_jsonl(self, csv_filename, jsonl_filename):
        print("Converting csv to jsonl file...")
        df = pd.read_csv(os.path.join(os.getcwd(), csv_filename), encoding='utf-16')
        pd.options.display.max_columns = 100

        # get product taxonomy node
        taxonomy_list = []
        with open("D:/Naru/shopifyAPI/product_taxonomy_node.txt", "r") as taxonomy:
            for i, x in enumerate(taxonomy):
                if i > 0:
                    taxonomy_list.append(x.split('-')[1].strip())

        # Create formatted dictionary
        datas = []
        for index in df.index:
            data_dict = {"input": dict(), "media": dict()}
            data_dict['input']['title'] = df.iloc[index]['Title']
            data_dict['input']['descriptionHtml'] = df.iloc[index]['Body(HTML)']
            data_dict['input']['vendor'] = df.iloc[index]['Vendor']
            if df.iloc[index]['Product Category'] in taxonomy_list:
                taxonomy_id = taxonomy_list.index(df.iloc[index]['Product Category']) + 1
            data_dict['input']['productCategory'] = {'productTaxonomyNodeId': f"gid://shopify/ProductTaxonomyNode/{str(taxonomy_id)}"}
            data_dict['input']['productType'] = df.iloc[index]['Type']
            data_dict['input']['tags'] = df.iloc[index]['Tags']
            data_dict['input']['options'] = [df.iloc[index]['Option1 Name'],
                                             df.iloc[index]['Option2 Name'],
                                             df.iloc[index]['Option3 Name']
                                             ]

            if df.iloc[index]['Variant Weight Unit'] == "g":
                df.loc[index, 'Variant Weight Unit'] = "GRAMS"
            elif df.iloc[index]['Variant Weight Unit'] == "kg":
                df.loc[index, 'Variant Weight Unit'] = "KILOGRAMS"

            data_dict['input']['variants'] = [
                {'sku': df.iloc[index]['Variant SKU'],
                 'options': [
                     df.iloc[index]['Option1 Value'],
                     df.iloc[index]['Option2 Value'],
                     df.iloc[index]['Option3 Value']
                 ],
                 'weight': int(df.iloc[index]['Variant Grams']),
                 'weightUnit': df.iloc[index]['Variant Weight Unit'],
                 'inventoryManagement': df.iloc[index]['Variant Inventory Tracker'].upper(),
                 'inventoryPolicy': df.iloc[index]['Variant Inventory Policy'].upper(),
                 'price': str(df.iloc[index]['Variant Price']),
                 'compareAtPrice': str(df.iloc[index]['Variant Compare At Price']),
                 'requiresShipping': bool(df.iloc[index]['Variant Requires Shipping']),
                 'taxable': bool(df.iloc[index]['Variant Taxable']),
                 'imageSrc': f"https:{df.iloc[index]['Image Src']}",
                 'title': 'Default'
                 }
            ]
            data_dict['input']['giftCard'] = bool(df.iloc[index]['Gift Card'])
            data_dict['input']['status'] = df.iloc[index]['Status'].upper()
            data_dict['media'] = {'originalSource': f"https:{df.iloc[index]['Image Src']}", 'mediaContentType': 'IMAGE'}

            datas.append(data_dict.copy())
        print(datas)
        with open(os.path.join(os.getcwd(), jsonl_filename), 'w') as jsonlfile:
            for item in datas:
                json.dump(item, jsonlfile)
                jsonlfile.write('\n')


        # csvfile = pd.read_csv(os.path.join(os.getcwd(), csv_filename), encoding='utf-16')
        #
        #
        #     print(csvfile.to_json(orient='records', lines=True), file=jsonfile, flush=False)
        # print('')

    def upload_jsonl(self, staged_target, jsonl_path):
        print("Uploading jsonl file to staged path...")
        url = staged_target['data']['stagedUploadsCreate']['stagedTargets'][0]['url']
        parameters = staged_target['data']['stagedUploadsCreate']['stagedTargets'][0]['parameters']
        files = dict()
        for parameter in parameters:
            files[f"{parameter['name']}"] = (None, parameter['value'])
        files['file'] = open(jsonl_path, 'rb')

        # with httpx.Client(timeout=None, follow_redirects=True) as sess:
        response = httpx.post(url, files=files)

        print(response)
        print(response.content)
        print('')

    def webhook_subscription(self, client):
        print("Subscribing webhook...")
        mutation = '''
                    mutation {
                        webhookSubscriptionCreate(
                            topic: BULK_OPERATIONS_FINISH
                            webhookSubscription: {
                                format: JSON,
                                callbackUrl: "https://12345.ngrok.io/"
                                }
                        )
                        {
                            userErrors {
                                field
                                message
                            }
                            webhookSubscription {
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

    def pool_operation_status(self, client):
        print("Pooling operation status...")
        query = '''
                    query {
                        currentBulkOperation(type: MUTATION) {
                            id
                            status
                            errorCode
                            createdAt
                            completedAt
                            objectCount
                            fileSize
                            url
                            partialDataUrl
                        }
                    }
                '''

        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2023-07/graphql.json',
                               json={"query": query})
        print(response)
        print(response.json())
        print('')

    def import_bulk_data(self, client, csv_filename, jsonl_filename):
        self.csv_to_jsonl(csv_filename=csv_filename, jsonl_filename=jsonl_filename)
        staged_target = self.generate_staged_target(client)
        self.upload_jsonl(staged_target=staged_target, jsonl_path=jsonl_filename)
        self.create_products(client, staged_target=staged_target)

    def create_collection(self, client):
        print('Creating collection...')
        mutation = '''
        mutation ($descriptionHtml: String!, $title: String!){
            collectionCreate(
                input: {
                    descriptionHtml: $descriptionHtml
                    title: $title
                }
            )
            {
                collection{
                    id
                    productsCount
                }
                userErrors{
                    field
                    message
                }   
            }
        }    
        '''

        variables = {
            'descriptionHtml': "<p>This Collection is created as a training material</p>",
            'title': "Collection1"
        }

        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2023-07/graphql.json',
                               json={"query": mutation, 'variables': variables})
        print(response)
        print(response.json())
        print('')

    def publish_collection(self, client):
        print('Publishing collection...')
        mutation = '''
        mutation {
            collectionPublish(
                input: {
                    id: "",
                    collectionPublications: {
                        publicationId: "gid://shopify/Publication/178396725562"
                        }
                    }
                )
            )
            {
                collectionPublications{
                    publishDate
                }
                userErrors{
                    field
                    message
            }
        }    
        '''

        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2023-07/graphql.json',
                               json={"query": mutation})
        print(response)
        print(response.json())
        print('')

    def get_collections(self, client):
        print('Getting collection list...')
        query = '''
                query {
                    collections(first: 10){
                        edges{
                            node{
                                id
                                title
                                handle
                                updatedAt
                                productsCount
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

    def check_bulk_operation_status(self, client, bulk_operation_id):
        query = f'''
            query {{
                node(id: "{bulk_operation_id}") {{
                    ... on BulkOperation {{
                        id
                        status
                    }}
                }}
            }}
        '''

        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2023-07/graphql.json',
                               json={"query": query})

        response_data = response.json()
        status = response_data['data']['node']['status']
        return status


if __name__ == '__main__':
    # Example
    load_dotenv()

    # ============================== Create Session ====================================
    s = ShopifyApp(
        store_name=os.getenv('STORE_NAME'),
        access_token=os.getenv('ACCESS_TOKEN')
    )

    s.create_session()

    # ================================== Query Shop ====================================
    # s.query_shop()

    # ================================ Query Product ===================================
    # s.query_products()

    # ============================== Query Publications ================================
    # s.query_publication()

    # ================================ Create Product ==================================
    variables = {
        'handle': 'christmas-gift-electric-ride-on-toy-car-for-kids-12v-battery-powered-with-remote-control',
        'title': 'Christmas Gift Electric Ride-On Toy Car for Kids - 12V Battery Powered with Remote Control',
        'descriptionHtml': '''The X-Mas Ride On Kids Truck is the perfect toy car for kids during the holiday season. With its sleek design and exciting features, it is sure to bring joy and adventure to children of all ages.<br><br>This ride-on truck offers two modes of play. In the parental remote control mode, parents can take control and enjoy the ride with their little ones. In the battery-operated mode, children can operate the truck themselves using the pedal for acceleration and the steering wheel for navigation. Please note that this <a href="https://www.magiccars.com">ride-on</a> truck comes in two boxes, so please wait patiently for both boxes to be delivered before assembly.<br><br>One of the most attractive and fun functions of this ride-on truck is its ability to be connected to devices such as MP3 players, AUX inputs, USB ports, and TF card slots. This means that children can enjoy their favorite music or stories while cruising around in style.<br><br>Safety is a top priority with this ride-on truck. It features a soft start function, ensuring a smooth and gradual acceleration. The four wear-resistant wheels are made of superior PP materials, providing durability and eliminating the need for inflating tires. This guarantees a safer and smoother driving experience for kids.<br><br>The X-Mas Ride On Kids Truck also boasts a cool and realistic appearance. With bright front and rear lights and double doors with magnetic locks, children will feel like they are driving a real truck. This attention to detail creates an authentic and immersive driving experience.<br><br>This ride-on truck is not only fun but also makes for a perfect gift for children. Whether it's for a birthday or Christmas, this scientifically designed toy car will bring smiles and laughter to any child's face.<br><br>Key Features:<br>- Two modes of play: parental remote control mode and battery-operated mode<br>- Forward and reverse functions with three adjustable speeds<br>- MP3 player, AUX input, USB port, and TF card slot for music and stories<br>- Soft start function for a smooth and gradual acceleration<br>- Four wear-resistant wheels made of superior PP materials<br>- Bright front and rear lights for a realistic driving experience<br>- Double doors with magnetic locks<br>- Overall dimension: 46.5"×31"×29"(L×W×H)<br>- Recommended for ages: 3-7 years old<br>- Assembly required<br><br>Specifications:<br>- Brand: Unbranded<br>- Year: 2022<br>- Theme: Cars<br>- Age Level: 3-4 Years, 4-7<br>- Character Family: Toy Story<br>- Color: White<br><br>Get ready for a thrilling and adventurous ride with the X-Mas Ride On Kids Truck. Order now and make this holiday season one to remember!''',
        'vendor': 'Magic Cars',
        'category': 'gid://shopify/TaxonomyCategory/tg-5-20-1',
        'productType': '',
        'tags': ['12v Ride On Toy'],
        'published': True,
        'productOptions': [
            {
                'name': 'Warranty',
                'values': [
                    {
                        'name': 'None - $0'
                    },
                    {
                        'name': '1 year - $89'
                    }
                ]
            },
            {
                'name': 'Custom license plate',
                'values': [
                    {
                        'name': 'None - $0'
                    },
                    {
                        'name': 'Custom license plate - $39'
                    }
                ]
            }
        ],
        'media': [
            {
                'originalSource': 'https://cdn.shopify.com/s/files/1/2245/9711/products/s-l500_cf86f1f2-7469-4029-bce6-110c5d81b77d.png?v=1694748618',
                'mediaContentType': 'IMAGE'
            },
            {
                'originalSource': 'https://cdn.shopify.com/s/files/1/2245/9711/products/s-l1600_950bb713-e831-4794-9d27-ce6e5a233362.jpg?v=1694748618',
                'mediaContentType': 'IMAGE'
            },
            {
                'originalSource': 'https://cdn.shopify.com/s/files/1/2245/9711/products/s-l1600_b8a2df74-6906-4896-a67d-363080b6e664.jpg?v=1694748618',
                'mediaContentType': 'IMAGE'
            },
            {
                'originalSource': 'https://cdn.shopify.com/s/files/1/2245/9711/products/s-l1600_caec825f-9f2c-4fbf-b52e-8a10556386eb.jpg?v=1694748618',
                'mediaContentType': 'IMAGE'
            },
            {
                'originalSource': 'https://cdn.shopify.com/s/files/1/2245/9711/products/s-l1600_2d958d03-9672-49bf-a26e-96b89c9b1c71.jpg?v=1694748618',
                'mediaContentType': 'IMAGE'
            }
        ],
        'variants': [
            {
                'optionValues': [
                    {
                        'name': 'None - $0',
                        'optionName': 'Warranty'
                    },
                    {
                        'name': 'None - $0',
                        'optionName': 'Custom license plate'
                    }
                ],
                'inventoryItem': {
                    'sku': '25-00001',
                    'measurement': {
                        'weight': {
                            'unit': 'GRAMS',
                            'value': 38555.986479318
                        }
                    },
                    'tracked': True,
                    'requiresShipping': True,
                    'cost': 398.88
                },
                'inventoryPolicy': 'DENY',
                'price': 598.88,
                'compareAtPrice': 778.54,
                'taxable': True,
                'barcode': '315584591043',
                'mediaSrc': 'https://cdn.shopify.com/s/files/1/2245/9711/products/s-l500_cf86f1f2-7469-4029-bce6-110c5d81b77d.png?v=1694748618',

            },
            {
                'optionValues': [
                    {
                        'name': 'None - $0',
                        'optionName': 'Warranty'
                    },
                    {
                        'name': 'Custom license plate - $39',
                        'optionName': 'Custom license plate'
                    }
                ],
                'inventoryItem': {
                    'sku': '25-00002',
                    'measurement': {
                        'weight': {
                            'unit': 'GRAMS',
                            'value': 38555.986479318
                        }
                    },
                    'tracked': True,
                    'requiresShipping': True,
                    'cost': 437.88
                },
                'inventoryPolicy': 'DENY',
                'price': 637.88,
                'compareAtPrice': 829.24,
                'taxable': True,
                'barcode': '315584591050',
                'mediaSrc': 'https://cdn.shopify.com/s/files/1/2245/9711/products/s-l1600_950bb713-e831-4794-9d27-ce6e5a233362.jpg?v=1694748618',
            },
            {
                'optionValues': [
                    {
                        'name': '1 year - $89',
                        'optionName': 'Warranty'
                    },
                    {
                        'name': 'None - $0',
                        'optionName': 'Custom license plate'
                    }
                ],
                'inventoryItem': {
                    'sku': '25-00003',
                    'measurement': {
                        'weight': {
                            'unit': 'GRAMS',
                            'value': 38555.986479318
                        }
                    },
                    'tracked': True,
                    'requiresShipping': True,
                    'cost': 487.88
                },
                'inventoryPolicy': 'DENY',
                'price': 687.88,
                'compareAtPrice': 894.24,
                'taxable': True,
                'barcode': '315584591067',
                'mediaSrc': 'https://cdn.shopify.com/s/files/1/2245/9711/products/s-l1600_b8a2df74-6906-4896-a67d-363080b6e664.jpg?v=1694748618',

            },
            {
                'optionValues': [
                    {
                        'name': '1 year - $89',
                        'optionName': 'Warranty'
                    },
                    {
                        'name': 'Custom license plate - $39',
                        'optionName': 'Custom license plate'
                    }
                ],
                'inventoryItem': {
                    'sku': '25-00004',
                    'measurement': {
                        'weight': {
                            'unit': 'GRAMS',
                            'value': 38555.986479318
                        }
                    },
                    'tracked': True,
                    'requiresShipping': True,
                    'cost': 526.88
                },
                'inventoryPolicy': 'DENY',
                'price': 726.88,
                'compareAtPrice': 944.94,
                'taxable': True,
                'barcode': '315584591074',
                'mediaSrc': 'https://cdn.shopify.com/s/files/1/2245/9711/products/s-l1600_caec825f-9f2c-4fbf-b52e-8a10556386eb.jpg?v=1694748618',
            },
        ],
        'giftCard': False,
        'seo': {
            'title': '',
            'description': ''
        },
        'status': 'ACTIVE'
    }

    s.create_product(variables=variables)

    # =========================== Delete Products By Handle ============================
    # handles = ['christmas-gift-electric-ride-on-toy-car-for-kids-12v-battery-powered-with-remote-control']
    # s.delete_products_by_handle(handles=handles)

    # =========================== Get Product Id By Handle ============================
    # handles = ['christmas-gift-electric-ride-on-toy-car-for-kids-12v-battery-powered-with-remote-control']
    # s.get_products_id_by_handle(handles=handles)

    # s.csv_to_jsonl(csv_filename='result.csv', jsonl_filename='test2.jsonl')
    # staged_target = s.generate_staged_target(client)
    # s.upload_jsonl(staged_target=staged_target, jsonl_path="D:/Naru/shopifyAPI/bulk_op_vars.jsonl")
    # s.create_products(client, staged_target=staged_target)
    # s.import_bulk_data(client=client, csv_filename='result.csv', jsonl_filename='bulk_op_vars.jsonl')
    # s.webhook_subscription(client)
    # s.create_collection(client)
    # s.get_publications(client)
    # s.get_collections(client)
    # s.pool_operation_status(client)
    # print(s.check_bulk_operation_status(client, bulk_operation_id='gid://shopify/BulkOperation/3252439023930'))