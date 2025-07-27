from httpx import Client
from dataclasses import dataclass
import json
import os
import pandas as pd
from urllib.parse import urljoin
from datetime import datetime
from dotenv import load_dotenv
import numpy as np
import httpx
import time
import ast

pd.options.display.max_columns = 100

@dataclass
class ShopifyApp:
    store_name: str = None
    access_token: str = None
    client: Client = None
    api_version: str = '2025-07'

    # Support
    # ==================================== Send Request ================================
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

    # def clean_and_collect_tags(self, series):
    #     """
    #     Takes a pandas Series of comma-separated strings,
    #     splits them, strips whitespace, and returns a sorted list of unique tags.
    #     """
    #     print(f'series: {series}')
    #     for tag_string in series.dropna(): # Handle NaN values if any
    #         # Split by comma, then strip each resulting part
    #         cleaned_parts = [part.strip() for part in tag_string.split(',')]
    #         print(f'cleaned_parts: {cleaned_parts}')
    #     # Return unique and sorted tags
    #     return cleaned_parts

    # ==================================== CSV to JSONL ================================
    def csv_to_jsonl(self, csv_file_path, jsonl_file_path, mode):
        """
        Converts a CSV file containing Shopify product data into a JSONL format
        suitable for Shopify's bulk import using the GraphQL Admin API.

        Args:
            csv_file_path (str): The path to the input CSV file.
            jsonl_file_path (str): The path where the output JSONL file will be saved.
        """
        try:
            # Using keep_default_na=False to prevent pandas from interpreting empty strings as NaN,
            # and then filling any actual NaN values (from other reasons) with empty strings.
            df = pd.read_csv(csv_file_path, keep_default_na=False).fillna('')
        except FileNotFoundError:
            print(f"Error: CSV file not found at '{csv_file_path}'")
            return
        except Exception as e:
            print(f"Error reading CSV file: {e}")
            return

        datas = []

        # Group by 'Handle' first to process all rows for a product together
        # This simplifies gathering all options, media, and variants for a single product.
        grouped_df = df.groupby('Handle').agg(
            {
                'Title': 'first',
                'Body (HTML)': 'first',
                'Vendor': 'first',
                'Product Category': 'first',
                'Type': 'first',
                'Tags': 'first',
                'Published': 'first',
                'Option1 Name': 'first',
                'Option1 Value': list,
                'Option1 Linked To': list,
                'Option2 Name': 'first',
                'Option2 Value': list,
                'Option2 Linked To': list,
                'Option3 Name': 'first',
                'Option3 Value': list,
                'Option3 Linked To': list,
                'Variant SKU': list,
                'Variant Grams': list,
                'Variant Inventory Tracker': list,
                'Variant Inventory Policy': list,
                'Variant Fulfillment Service': list,
                'Variant Price': list,
                'Variant Compare At Price': list,
                'Variant Requires Shipping': list,
                'Variant Taxable': list,
                'Variant Barcode': list,
                'Image Src': list,
                'Image Position': list,
                'Image Alt Text': list,
                'Gift Card': 'first',
                'SEO Title': 'first',
                'SEO Description': 'first',
                'Cost per item': list,
                'Status': 'first'
            }
        ).reset_index()

        if mode == 'product':
            for index, row in grouped_df.iterrows():
                product_entry = {
                    'product': { 
                        'handle': row['Handle'],
                        'title': str(row['Title']).strip() if row['Title'] else '',
                        'descriptionHtml': str(row['Body (HTML)']).strip() if row['Body (HTML)'] else '',
                        'vendor': str(row['Vendor']).strip() if row['Vendor'] else '',
                        # 'category': str(row['Product Category']).strip() if row['Product Category'] else '', # Changed from 'Standard Product Category' based on your provided code
                        'category': 'gid://shopify/TaxonomyCategory/tg-5-20-1',
                        'productType': str(row['Type']).strip() if row['Type'] else '', # Changed from 'Product Type' based on your provided code
                        'tags': [tag.strip() for tag in str(row['Tags']).split(',') if tag.strip()] if row['Tags'] else [],
                        'productOptions': [],
                        'giftCard': str(row['Gift Card']).strip().lower() == 'true',
                        'seo': {
                            'title': str(row['SEO Title']).strip() if row['SEO Title'] else '',
                            'description': str(row['SEO Description']).strip() if row['SEO Description'] else ''
                        },
                        'status': str(row['Status']).strip().upper() if row['Status'] else 'ACTIVE'
                    },
                    'media': []
                }

                # --- Process Product Options (Gather all unique options and their values for this product) ---
                productOptions = list()
                
                for i in range(1, 4):
                    productOption = {'name': None, 'values':[]}
                    option_name_col = f'Option{i} Name'
                    option_value_col = f'Option{i} Value'
                    if option_name_col in grouped_df.columns:
                        productOption['name'] = row[option_name_col]
                    if option_value_col in grouped_df.columns:
                        option_value_list = row[option_value_col]
                        unique_values = []
                        for item in option_value_list:
                            if item not in unique_values and item != '':
                                unique_values.append(item)
                        for item in unique_values:
                            productOption['values'].append({'name': item})
                    if productOption['name'] != '':
                        productOptions.append(productOption)
                
                product_entry['product']['productOptions'] = productOptions

                media_list = list()
                for i in range(len(row['Image Src'])): 
                    media = {
                        # 'alt': str(row['Image Alt Text'][i]).strip() if row['Image Alt Text'][i] else '',
                        'mediaContentType': 'IMAGE',
                        'originalSource': str(row['Image Src'][i]).strip() if row['Image Src'][i] else ''
                    }
                    media_list.append(media)
                
                print(f'media_list:{media_list}')
                product_entry['media'] = media_list

                datas.append(product_entry)

        if mode == 'variant':
            handles = grouped_df['Handle'].tolist()
            response = self.get_products_id_by_handle(handles=handles)
            edges = response['data']['products']['edges']
            records = [x['node'] for x in edges]
            product_id_df = pd.DataFrame.from_records(records)
            grouped_id_df = pd.merge(grouped_df, product_id_df, how='left', left_on='Handle', right_on='handle')
            for index, row in grouped_id_df.iterrows():
                variants_entry = {
                    'productId': row['id'],
                    'variants': []
                }

                variants = list()
                
                for i in range(len(row['Variant SKU'])):
                    variant = {
                        'optionValues': []
                    }
                    
                    for j in range(1, 4):
                        optionValue = {}
                        option_name_col = f'Option{j} Name'
                        option_value_col = f'Option{j} Value'
                        if row[option_name_col] != '':
                            optionValue['optionName'] = row[option_name_col]
                        if row[option_value_col] != '':
                            optionValue['name'] = row[option_value_col][i]
                        if optionValue['name'] != '':
                            variant['optionValues'].append(optionValue)

                    if len(variant['optionValues']) > 0:
                        variants.append(variant)
                    print(variants)

                variants_entry['variants'] = variants
                datas.append(variants_entry)

        #     publish_entry = {
        #         'publish': {
        #             'published': str(row['Published']).strip().lower() == 'true',
        #         }
        #     }

        # Write product data to JSONL file
        with open(jsonl_file_path, 'w', encoding='utf-8') as outfile:
            for data in datas:
                outfile.write(json.dumps(data, ensure_ascii=False) + '\n')
        print(f"Successfully converted '{csv_file_path}' to '{jsonl_file_path}'")

        #     # --- Process Media (Images) ---
        #     # Collect all unique image sources across all rows for this product
        #     collected_media_sources = set()
        #     for _, row in product_rows.iterrows():
        #         if row['Image Src']:
        #             image_src = str(row['Image Src']).strip()
        #             if image_src and image_src not in collected_media_sources:
        #                 product_entry['media'].append({
        #                     'originalSource': image_src,
        #                     'mediaContentType': 'IMAGE'
        #                     # 'alt': str(row['Image Alt Text']).strip() # Add if Image Alt Text is available and desired
        #                 })
        #                 collected_media_sources.add(image_src)

        #     # --- Process Variants ---
        #     # First, collect all option names from the first row that has them
        #     option_names_mapping = {}  # Maps option position to option name
        #     for i in range(1, 4):
        #         option_name_col = f'Option{i} Name'
        #         for _, row in product_rows.iterrows():
        #             if row[option_name_col]:
        #                 option_names_mapping[i] = str(row[option_name_col]).strip()
        #                 break  # Take the first non-empty value

        #     # Process each row as a potential variant
        #     variants_list = []
        #     processed_skus = set()  # Track processed SKUs to avoid duplicates

        #     for _, row in product_rows.iterrows():
        #         variant_sku = str(row['Variant SKU']).strip()

        #         # Skip rows that don't have variant data (like image-only rows)
        #         if not variant_sku:
        #             continue

        #         # Skip duplicate SKUs (in case of multiple image rows for same variant)
        #         if variant_sku and variant_sku in processed_skus:
        #             continue

        #         option_values_for_current_row = []

        #         # First try to get option values from the row itself
        #         for i in range(1, 4):
        #             option_name_col = f'Option{i} Name'
        #             option_value_col = f'Option{i} Value'
        #             if option_name_col in row and row[option_name_col] and \
        #                option_value_col in row and row[option_value_col]:
        #                 option_values_for_current_row.append({
        #                     'name': str(row[option_value_col]).strip(),
        #                     'optionName': str(row[option_name_col]).strip()
        #                 })

        #         # If no option values found in row, try to extract from SKU pattern
        #         if not option_values_for_current_row and variant_sku:
        #             # SKU pattern appears to be: basesku-option1-option2-option3
        #             sku_parts = variant_sku.split('-')
        #             if len(sku_parts) >= 4:  # base + 3 options
        #                 option_values_from_sku = sku_parts[1:]  # Skip the base SKU part

        #                 # Map the SKU option values to the actual option names
        #                 for i, option_value in enumerate(option_values_from_sku[:3], 1):  # Max 3 options
        #                     if i in option_names_mapping:
        #                         # Clean up the option value and convert known patterns
        #                         clean_option_value = option_value.replace('customlicenseplate', 'Custom license plate - $39')
        #                         clean_option_value = clean_option_value.replace('none', 'None - $0')
        #                         clean_option_value = clean_option_value.replace('1year', '1 year - $89')

        #                         option_values_for_current_row.append({
        #                             'name': clean_option_value,
        #                             'optionName': option_names_mapping[i]
        #                         })

        #         # Create variant data for this row
        #         variant_data = {
        #             'optionValues': option_values_for_current_row,
        #             'inventoryItem': {
        #                 'sku': variant_sku,
        #                 'measurement': {
        #                     'weight': {
        #                         'unit': str(row['Variant Weight Unit']).strip().upper() if row['Variant Weight Unit'] else 'GRAMS',
        #                         'value': float(row['Variant Grams']) if row['Variant Grams'] else 0.0
        #                     }
        #                 },
        #                 'tracked': str(row['Variant Inventory Tracker']).strip().lower() == 'shopify',
        #                 'requiresShipping': str(row['Variant Requires Shipping']).strip().lower() == 'true',
        #                 'cost': float(row['Cost per item']) if row['Cost per item'] else 0.0
        #             },
        #             'inventoryPolicy': 'DENY',
        #             'price': float(row['Variant Price']) if row['Variant Price'] else 0.0,
        #             'compareAtPrice': float(row['Variant Compare At Price']) if row['Variant Compare At Price'] else None,
        #             'taxable': str(row['Variant Taxable']).strip().lower() == 'true',
        #             'barcode': str(row['Variant Barcode']).strip() if row['Variant Barcode'] else '',
        #             'mediaSrc': str(row['Image Src']).strip() if row['Image Src'] else ''
        #         }

        #         # Clean up None values in compareAtPrice
        #         if variant_data['compareAtPrice'] is None:
        #             del variant_data['compareAtPrice']

        #         variants_list.append(variant_data)

        #         # Mark this SKU as processed
        #         if variant_sku:
        #             processed_skus.add(variant_sku)

        #     # Add all collected variants to the product entry
        #     product_entry['variants'] = variants_list

        #     # If after processing all rows, no variants were explicitly found (e.g., very simple product with no SKU/options)
        #     # Create a default variant if needed. Shopify typically requires at least one variant.
        #     if not product_entry['variants']:
        #         # Create a basic default variant using data from the first row of the product
        #         product_entry['variants'].append({
        #             'optionValues': [],
        #             'inventoryItem': {
        #                 'sku': str(row['Variant SKU']).strip(), # Use SKU from first row if available
        #                 'measurement': {
        #                     'weight': {
        #                         'unit': str(row['Variant Weight Unit']).strip().upper() if row['Variant Weight Unit'] else 'GRAMS',
        #                         'value': float(row['Variant Grams']) if row['Variant Grams'] else 0.0
        #                     }
        #                 },
        #                 'tracked': str(row['Variant Inventory Tracker']).strip().lower() == 'shopify',
        #                 'requiresShipping': str(row['Variant Requires Shipping']).strip().lower() == 'true',
        #                 'cost': float(row['Cost per item']) if row['Cost per item'] else 0.0
        #             },
        #             'inventoryPolicy': 'DENY',
        #             'price': float(row['Variant Price']) if row['Variant Price'] else 0.0,
        #             'compareAtPrice': float(row['Variant Compare At Price']) if row['Variant Compare At Price'] else None,
        #             'taxable': str(row['Variant Taxable']).strip().lower() == 'true',
        #             'barcode': str(row['Variant Barcode']).strip(),
        #             'mediaSrc': str(row['Image Src']).strip()
        #         })

        #     products_data[handle] = product_entry

        # # Write to JSONL file
        # with open(jsonl_file_path, 'w', encoding='utf-8') as outfile:
        #     for handle, product_data in products_data.items():
        #         # Final cleanup of empty lists/dicts if desired, though Shopify import handles this.
        #         if not product_data['productOptions']:
        #             del product_data['productOptions']
        #         if not product_data['media']:
        #             del product_data['media']

        #         outfile.write(json.dumps(product_data, ensure_ascii=False) + '\n')
        # print(f"Successfully converted '{csv_file_path}' to '{jsonl_file_path}'")

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
                        handle
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

        return self.send_request(query=mutation)

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

    def create_products(self, staged_target):
        print('Creating products...')
        mutation = '''
            mutation ($stagedUploadPath: String!){
                bulkOperationRunMutation(
                    mutation: "mutation call($product: ProductCreateInput!, $media: [CreateMediaInput!]){
                        productCreate(product: $product, media: $media){
                            product{
                                handle
                                id
                            }
                            userErrors {
                                message
                                field
                            } 
                        }
                    }",
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

        response = self.send_request(query=mutation, variables=variables)

        print(response)
        print('')

        return response
    
    def create_variants(self, staged_target):
        print('Creating products...')
        mutation = '''
            mutation ($stagedUploadPath: String!){
                bulkOperationRunMutation(
                    mutation: "mutation call($productId: ID!, $variants: [ProductVariantsBulkInput!]!){
                        productVariantsBulkCreate(product: $productId, variants: $variants, strategy: REMOVE_STANDALONE_VARIANT){
                            product{
                                handle
                                id
                            }
                            productVariants{
                                sku
                                id
                            }
                            userErrors {
                                message
                                field
                            } 
                        }
                    }",
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

        response = self.send_request(query=mutation, variables=variables)

        print(response)
        print('')

        return response

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
        # response = self.client.post(url, files=files)

        print(response)
        print(response.content)
        print('')

    def webhook_subscription(self):
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

        return self.send_request(query=mutation)

    def pool_operation_status(self):
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

        return self.send_request(query=query)

    def import_bulk_data(self, csv_file_path, jsonl_file_path):
        # Create products
        # self.csv_to_jsonl(csv_file_path=csv_file_path, jsonl_file_path=jsonl_file_path, mode='product')
        # staged_target = self.generate_staged_target()
        # self.upload_jsonl(staged_target=staged_target, jsonl_path=jsonl_file_path)
        # self.create_products(staged_target=staged_target)
        # completed = False
        # while not completed:
        #     time.sleep(60)
        #     response = self.pool_operation_status()
        #     if response['data']['status'] == 'COMPLETED':
        #         completed = True
        # Create variants
        self.csv_to_jsonl(csv_file_path=csv_file_path, jsonl_file_path=jsonl_file_path, mode='variant')
        # staged_target = self.generate_staged_target()
        # self.upload_jsonl(staged_target=staged_target, jsonl_path=jsonl_file_path)
        # self.create_products(staged_target=staged_target)
        # completed = False
        # while not completed:
        #     time.sleep(60)
        #     response = self.pool_operation_status()
        #     if response['data']['status'] == 'COMPLETED':
        #         completed = True
        print('Product import is completed')

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
    # variables = {
    #     'handle': 'christmas-gift-electric-ride-on-toy-car-for-kids-12v-battery-powered-with-remote-control',
    #     'title': 'Christmas Gift Electric Ride-On Toy Car for Kids - 12V Battery Powered with Remote Control',
    #     'descriptionHtml': '''The X-Mas Ride On Kids Truck is the perfect toy car for kids during the holiday season. With its sleek design and exciting features, it is sure to bring joy and adventure to children of all ages.<br><br>This ride-on truck offers two modes of play. In the parental remote control mode, parents can take control and enjoy the ride with their little ones. In the battery-operated mode, children can operate the truck themselves using the pedal for acceleration and the steering wheel for navigation. Please note that this <a href="https://www.magiccars.com">ride-on</a> truck comes in two boxes, so please wait patiently for both boxes to be delivered before assembly.<br><br>One of the most attractive and fun functions of this ride-on truck is its ability to be connected to devices such as MP3 players, AUX inputs, USB ports, and TF card slots. This means that children can enjoy their favorite music or stories while cruising around in style.<br><br>Safety is a top priority with this ride-on truck. It features a soft start function, ensuring a smooth and gradual acceleration. The four wear-resistant wheels are made of superior PP materials, providing durability and eliminating the need for inflating tires. This guarantees a safer and smoother driving experience for kids.<br><br>The X-Mas Ride On Kids Truck also boasts a cool and realistic appearance. With bright front and rear lights and double doors with magnetic locks, children will feel like they are driving a real truck. This attention to detail creates an authentic and immersive driving experience.<br><br>This ride-on truck is not only fun but also makes for a perfect gift for children. Whether it's for a birthday or Christmas, this scientifically designed toy car will bring smiles and laughter to any child's face.<br><br>Key Features:<br>- Two modes of play: parental remote control mode and battery-operated mode<br>- Forward and reverse functions with three adjustable speeds<br>- MP3 player, AUX input, USB port, and TF card slot for music and stories<br>- Soft start function for a smooth and gradual acceleration<br>- Four wear-resistant wheels made of superior PP materials<br>- Bright front and rear lights for a realistic driving experience<br>- Double doors with magnetic locks<br>- Overall dimension: 46.5"×31"×29"(L×W×H)<br>- Recommended for ages: 3-7 years old<br>- Assembly required<br><br>Specifications:<br>- Brand: Unbranded<br>- Year: 2022<br>- Theme: Cars<br>- Age Level: 3-4 Years, 4-7<br>- Character Family: Toy Story<br>- Color: White<br><br>Get ready for a thrilling and adventurous ride with the X-Mas Ride On Kids Truck. Order now and make this holiday season one to remember!''',
    #     'vendor': 'Magic Cars',
    #     'category': 'gid://shopify/TaxonomyCategory/tg-5-20-1',
    #     'productType': '',
    #     'tags': ['12v Ride On Toy'],
    #     'published': True,
    #     'productOptions': [
    #         {
    #             'name': 'Warranty',
    #             'values': [
    #                 {
    #                     'name': 'None - $0'
    #                 },
    #                 {
    #                     'name': '1 year - $89'
    #                 }
    #             ]
    #         },
    #         {
    #             'name': 'Custom license plate',
    #             'values': [
    #                 {
    #                     'name': 'None - $0'
    #                 },
    #                 {
    #                     'name': 'Custom license plate - $39'
    #                 }
    #             ]
    #         }
    #     ],
    #     'media': [
    #         {
    #             'originalSource': 'https://cdn.shopify.com/s/files/1/2245/9711/products/s-l500_cf86f1f2-7469-4029-bce6-110c5d81b77d.png?v=1694748618',
    #             'mediaContentType': 'IMAGE'
    #         },
    #         {
    #             'originalSource': 'https://cdn.shopify.com/s/files/1/2245/9711/products/s-l1600_950bb713-e831-4794-9d27-ce6e5a233362.jpg?v=1694748618',
    #             'mediaContentType': 'IMAGE'
    #         },
    #         {
    #             'originalSource': 'https://cdn.shopify.com/s/files/1/2245/9711/products/s-l1600_b8a2df74-6906-4896-a67d-363080b6e664.jpg?v=1694748618',
    #             'mediaContentType': 'IMAGE'
    #         },
    #         {
    #             'originalSource': 'https://cdn.shopify.com/s/files/1/2245/9711/products/s-l1600_caec825f-9f2c-4fbf-b52e-8a10556386eb.jpg?v=1694748618',
    #             'mediaContentType': 'IMAGE'
    #         },
    #         {
    #             'originalSource': 'https://cdn.shopify.com/s/files/1/2245/9711/products/s-l1600_2d958d03-9672-49bf-a26e-96b89c9b1c71.jpg?v=1694748618',
    #             'mediaContentType': 'IMAGE'
    #         }
    #     ],
    #     'variants': [
    #         {
    #             'optionValues': [
    #                 {
    #                     'name': 'None - $0',
    #                     'optionName': 'Warranty'
    #                 },
    #                 {
    #                     'name': 'None - $0',
    #                     'optionName': 'Custom license plate'
    #                 }
    #             ],
    #             'inventoryItem': {
    #                 'sku': '25-00001',
    #                 'measurement': {
    #                     'weight': {
    #                         'unit': 'GRAMS',
    #                         'value': 38555.986479318
    #                     }
    #                 },
    #                 'tracked': True,
    #                 'requiresShipping': True,
    #                 'cost': 398.88
    #             },
    #             'inventoryPolicy': 'DENY',
    #             'price': 598.88,
    #             'compareAtPrice': 778.54,
    #             'taxable': True,
    #             'barcode': '315584591043',
    #             'mediaSrc': 'https://cdn.shopify.com/s/files/1/2245/9711/products/s-l500_cf86f1f2-7469-4029-bce6-110c5d81b77d.png?v=1694748618',

    #         },
    #         {
    #             'optionValues': [
    #                 {
    #                     'name': 'None - $0',
    #                     'optionName': 'Warranty'
    #                 },
    #                 {
    #                     'name': 'Custom license plate - $39',
    #                     'optionName': 'Custom license plate'
    #                 }
    #             ],
    #             'inventoryItem': {
    #                 'sku': '25-00002',
    #                 'measurement': {
    #                     'weight': {
    #                         'unit': 'GRAMS',
    #                         'value': 38555.986479318
    #                     }
    #                 },
    #                 'tracked': True,
    #                 'requiresShipping': True,
    #                 'cost': 437.88
    #             },
    #             'inventoryPolicy': 'DENY',
    #             'price': 637.88,
    #             'compareAtPrice': 829.24,
    #             'taxable': True,
    #             'barcode': '315584591050',
    #             'mediaSrc': 'https://cdn.shopify.com/s/files/1/2245/9711/products/s-l1600_950bb713-e831-4794-9d27-ce6e5a233362.jpg?v=1694748618',
    #         },
    #         {
    #             'optionValues': [
    #                 {
    #                     'name': '1 year - $89',
    #                     'optionName': 'Warranty'
    #                 },
    #                 {
    #                     'name': 'None - $0',
    #                     'optionName': 'Custom license plate'
    #                 }
    #             ],
    #             'inventoryItem': {
    #                 'sku': '25-00003',
    #                 'measurement': {
    #                     'weight': {
    #                         'unit': 'GRAMS',
    #                         'value': 38555.986479318
    #                     }
    #                 },
    #                 'tracked': True,
    #                 'requiresShipping': True,
    #                 'cost': 487.88
    #             },
    #             'inventoryPolicy': 'DENY',
    #             'price': 687.88,
    #             'compareAtPrice': 894.24,
    #             'taxable': True,
    #             'barcode': '315584591067',
    #             'mediaSrc': 'https://cdn.shopify.com/s/files/1/2245/9711/products/s-l1600_b8a2df74-6906-4896-a67d-363080b6e664.jpg?v=1694748618',

    #         },
    #         {
    #             'optionValues': [
    #                 {
    #                     'name': '1 year - $89',
    #                     'optionName': 'Warranty'
    #                 },
    #                 {
    #                     'name': 'Custom license plate - $39',
    #                     'optionName': 'Custom license plate'
    #                 }
    #             ],
    #             'inventoryItem': {
    #                 'sku': '25-00004',
    #                 'measurement': {
    #                     'weight': {
    #                         'unit': 'GRAMS',
    #                         'value': 38555.986479318
    #                     }
    #                 },
    #                 'tracked': True,
    #                 'requiresShipping': True,
    #                 'cost': 526.88
    #             },
    #             'inventoryPolicy': 'DENY',
    #             'price': 726.88,
    #             'compareAtPrice': 944.94,
    #             'taxable': True,
    #             'barcode': '315584591074',
    #             'mediaSrc': 'https://cdn.shopify.com/s/files/1/2245/9711/products/s-l1600_caec825f-9f2c-4fbf-b52e-8a10556386eb.jpg?v=1694748618',
    #         },
    #     ],
    #     'giftCard': False,
    #     'seo': {
    #         'title': '',
    #         'description': ''
    #     },
    #     'status': 'ACTIVE'
    # }

    # s.create_product(variables=variables)

    # =========================== Delete Products By Handle ============================
    # handles = ['christmas-gift-electric-ride-on-toy-car-for-kids-12v-battery-powered-with-remote-control']
    # s.delete_products_by_handle(handles=handles)

    # =========================== Get Product Id By Handle ============================
    # handles = ['christmas-gift-electric-ride-on-toy-car-for-kids-12v-battery-powered-with-remote-control']
    # s.get_products_id_by_handle(handles=handles)

    # ================================= CSV to JSONL ==================================
    # s.csv_to_jsonl(csv_file_path='data/test.csv', jsonl_file_path='./data/product_bulk_op_vars.jsonl')

    # ================================= Staged Target ==================================
    # staged_target = s.generate_staged_target()

    # ================================= Upload JSONL ==================================
    # s.upload_jsonl(staged_target=staged_target, jsonl_path="./data/product_bulk_op_vars.jsonl")

    # create_products_flag = input('Press any key to continue')

    # =============================== bulk create products =============================
    # s.create_products(staged_target=staged_target)

    # =============================== bulk import products =============================
    s.import_bulk_data(csv_file_path='./data/test.csv', jsonl_file_path='./data/bulk_op_vars.jsonl')

    # ============================== pull operation status =============================
    # stopper = '0'
    # while stopper != '1':
    #     s.pool_operation_status()
    #     stopper = input('Do you want to stop monitoring? [1(Yes) or 0(No)]')

    # =============================== webhook_subscription =============================
    # s.webhook_subscription()

    # s.create_collection(client)
    # s.get_publications(client)
    # s.get_collections(client)
    # print(s.check_bulk_operation_status(client, bulk_operation_id='gid://shopify/BulkOperation/3252439023930'))