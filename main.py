from httpx import Client, HTTPError
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
    # def send_request(self, query, variables=None):
    #     if self.client:
    #         response = self.client.post(
    #             f'https://{self.store_name}.myshopify.com/admin/api/{self.api_version}/graphql.json',
    #             json={"query": query, "variables": variables}
    #         )

    #         print(response)
    #         print(response.json())
    #         print('')

    #         return response.json()

    #     else:
    #         print('Please create session before execute the function')

    def send_request(self, query, variables=None):
        if not self.client:
            print('Error: Please create a session before executing the function.')
            return None

        url = f'https://{self.store_name}.myshopify.com/admin/api/{self.api_version}/graphql.json'
        payload = {"query": query, "variables": variables}

        max_retries = 3
        retries = 0

        while retries < max_retries:
            try:
                response = self.client.post(url, json=payload)

                # A 2xx status code indicates success
                if 200 <= response.status_code < 400:
                    print(f"Request successful after {retries + 1} attempt(s).")
                    data = response.json()
                    
                    # Check for GraphQL errors within the response body
                    if 'errors' in data:
                        print(f"GraphQL Errors: {data['errors']}")
                        return None
                    
                    print(data)

                    return data

                # If the status code is not 200, it's a non-retriable error
                else:
                    print(f"HTTP Error {response.status_code}: {response.reason_phrase}")
                    print(f"Attempt {retries + 1}/{max_retries} failed. Retrying...")
                    retries += 1
                    time.sleep(2 ** retries) # Exponential backoff delay
                    
            except httpx.HTTPError as e:
                print(f"Request failed: {e}")
                retries += 1
                print(f"Attempt {retries}/{max_retries} failed. Retrying...")
                time.sleep(2 ** retries) # Exponential backoff delay
                
            except json.JSONDecodeError:
                print("Failed to decode JSON from response.")
                print(f"Response content: {response.text}")
                return None # Non-retriable error

        print(f"All {max_retries} attempts failed. Giving up.")
        return None

    # ==================================== Clean Tags ================================
    def clean_and_collect_tags(self, series):
        """
        Takes a pandas Series of comma-separated strings,
        splits them, strips whitespace, and returns a sorted list of unique tags.
        """
        print(f'series: {series}')
        for tag_string in series.dropna(): # Handle NaN values if any
            # Split by comma, then strip each resulting part
            cleaned_parts = [part.strip() for part in tag_string.split(',')]
            print(f'cleaned_parts: {cleaned_parts}')
        # Return unique and sorted tags
        return cleaned_parts
    
    # ==================================== Chunk Data ================================
    def chunk_shopify_csv_by_product(self, input_csv_path, output_directory="shopify_product_chunks_by_handle", products_per_chunk=200):
        """
        Reads a Shopify product CSV, chunks it into smaller files ensuring
        that each file contains complete products (all variants of a handle).

        Args:
            input_csv_path (str): Path to the input CSV file.
            output_directory (str): Directory to save the chunked CSV files.
            products_per_chunk (int): Maximum number of unique products (Handles) per chunk file.
        """

        if not os.path.exists(output_directory):
            os.makedirs(output_directory)

        print(f"Reading entire CSV file: {input_csv_path}...")
        df = pd.read_csv(input_csv_path)
        print(f"Total rows read: {len(df)}")

        # Get unique handles in order of appearance
        unique_handles = df['Handle'].unique()
        print(f"Total unique products (handles): {len(unique_handles)}")

        file_number = 1
        current_product_count = 0
        start_index = 0
        head, tail = os.path.split(input_csv_path)
        filename_split = tail

        for i, handle in enumerate(unique_handles):
            rows_for_handle = df[df['Handle'] == handle]

            if current_product_count >= products_per_chunk and i > 0:
                # Extract the rows for the current chunk
                chunk_df = df.iloc[start_index : rows_for_handle.index[0]]

                # Define the output path and save
                # output_filename = os.path.join(output_directory, f"{repr(input_csv_path).split('\\')[-1].split('.')[0]}_{file_number:03d}.csv")
                output_filename = os.path.join(output_directory, f"{filename_split.split('.')[0]}_{file_number:03d}.csv")
                chunk_df.to_csv(output_filename, index=False)
                print(f"Saved {len(chunk_df)} rows ({current_product_count} products) to {output_filename}")

                # Reset for the next chunk
                file_number += 1
                current_product_count = 0
                start_index = rows_for_handle.index[0] # New start is where this handle begins

            current_product_count += 1

        # Save the last remaining chunk
        if current_product_count > 0:
            chunk_df = df.iloc[start_index:]
            # output_filename = os.path.join(output_directory, f"{repr(input_csv_path).split('\\')[-1].split('.')[0]}_{file_number:03d}.csv")
            output_filename = os.path.join(output_directory, f"{filename_split.split('.')[0]}_{file_number:03d}.csv")
            chunk_df.to_csv(output_filename, index=False)
            print(f"Saved {len(chunk_df)} rows ({current_product_count} products) to {output_filename}")

        print(f"\nFinished chunking. Total {file_number} files created in '{output_directory}'.")

    def chunk_list(self, input_list, chunk_size=249):
        """
        Chunks a list into smaller lists of a specified size and returns
        a list containing all the chunks.

        Args:
            input_list (list): The list to be chunked.
            chunk_size (int): The maximum size of each chunk. Defaults to 200.

        Returns:
            list: A list of lists, where each inner list is a chunk of the
                original list.
        """
        result_list = []
        for i in range(0, len(input_list), chunk_size):
            chunk = input_list[i:i + chunk_size]
            result_list.append(chunk)
        
        return result_list

    # ==================================== CSV to JSONL ================================
    def csv_to_jsonl(self, csv_file_path, jsonl_file_path, mode, locationId=None):
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
                'Variant Image': list,
                'Variant Weight Unit': list,
                'Cost per item': list,
                'Status': 'first',
                'Available Qty': list,
                'Vendor SKU': 'first',
                'enable_best_price (product.metafields.custom.enable_best_price)': 'first'
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
                        'status': str(row['Status']).strip().upper() if row['Status'] else 'ACTIVE',
                        'metafields': []
                    },
                    # 'media': []
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

                metafields = []
                if row['Vendor SKU']:
                    vendorSKU = {'namespace': 'custom', 'key': 'vendor_sku', 'value': str(int(float(row['Vendor SKU']))), 'type': 'single_line_text_field'}
                    metafields.append(vendorSKU)
                if row['enable_best_price (product.metafields.custom.enable_best_price)']:
                    enableBestPrice = {'namespace': 'custom', 'key': 'enable_best_price', 'value': row['enable_best_price (product.metafields.custom.enable_best_price)'], 'type': 'boolean'}
                    metafields.append(enableBestPrice)
                product_entry['product']['metafields'] = metafields

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
                    'media': [],
                    'productId': row['id'],
                    'variants': [],
                    'strategy': 'REMOVE_STANDALONE_VARIANT'
                }

                variants = list()

                for i in range(len(row['Variant SKU']) - 1):
                    if row['Variant SKU'][i] != '' and row['Variant SKU'][i] is not None:
                        listMediaSrc = list()
                        listMediaSrc.append(str(row['Variant Image'][i]).strip() if row['Variant Image'][i] else ''),
                        variant = {
                            'optionValues': [],
                            'inventoryItem': {
                                'sku': row['Variant SKU'][i],
                                'tracked' : str(row['Variant Inventory Tracker'][i]).strip().lower() == 'shopify',
                                'requiresShipping': str(row['Variant Requires Shipping'][i]).strip().lower() == 'true',
                                'cost': float(row['Cost per item'][i]) if row['Cost per item'][i] else 0.0,
                                'measurement':{
                                    'weight':{
                                        # 'unit': str(row['Variant Weight Unit'][i]).strip().upper() if row['Variant Weight Unit'][i] else 'GRAMS',
                                        'unit': 'GRAMS',
                                        'value': float(row['Variant Grams'][i]) if row['Variant Grams'][i] else 0.0
                                    }
                                }
                            },
                            'inventoryPolicy': 'DENY',
                            'inventoryQuantities': [],
                            'price': float(row['Variant Price'][i]) if row['Variant Price'][i] else 0.0,
                            'compareAtPrice': float(row['Variant Compare At Price'][i]) if row['Variant Compare At Price'][i] else None,
                            'taxable': str(row['Variant Taxable'][i]).strip().lower() == 'true',
                            'barcode': str(int(float(row['Variant Barcode'][i]))).strip() if row['Variant Barcode'][i] else '',
                            'mediaSrc': listMediaSrc
                        }

                        # Clean up None values in compareAtPrice
                        if variant['compareAtPrice'] is None:
                            del variant['compareAtPrice']
                        
                        option_values = []
                        for j in range(1, 4):
                            optionValue = {}
                            option_name_col = f'Option{j} Name'
                            option_value_col = f'Option{j} Value'
                            if row[option_name_col] != '':
                                optionValue['optionName'] = row[option_name_col]
                            if len(row[option_value_col]) > 0:
                                optionValue['name'] = row[option_value_col][i]
                            if optionValue['name'] != '':
                                if len(optionValue) > 0:
                                    option_values.append(optionValue)
                                    
                        variant['optionValues'] = option_values.copy()

                        quantities = []
                        for qty in row['Available Qty']:
                            if qty != '':
                                quantity = {
                                    'availableQuantity': int(qty),
                                    'locationId': locationId
                                }
                            
                                quantities.append(quantity)

                        variant['inventoryQuantities'] = quantities.copy()

                        variants.append(variant)

                variants_entry['variants'] = variants
                                
                media_list = list()
                for i in range(len(row['Image Src'])): 
                    media = {
                        'mediaContentType': 'IMAGE',
                        'originalSource': str(row['Image Src'][i]).strip() if row['Image Src'][i] else ''
                    }
                    if media['originalSource'] != '' and media['originalSource'] not in media_list:
                        media_list.append(media)
                
                variant_medias = [{'mediaContentType': 'IMAGE', 'originalSource': row.strip()} for row in list(set(listMediaSrc))]
                media_list.extend(variant_medias)          
                variants_entry['media'] = media_list.copy()     
                datas.append(variants_entry.copy())
        
        if mode == 'publish':
            handles = grouped_df['Handle'].tolist()
            response = self.get_products_id_by_handle(handles=handles)
            edges = response['data']['products']['edges']
            records = [x['node'] for x in edges]
            product_id_df = pd.DataFrame.from_records(records)
            grouped_id_df = pd.merge(grouped_df, product_id_df, how='left', left_on='Handle', right_on='handle')
            
            response = self.query_publication()
            nodes = response['data']['publications']['nodes']
            publication_ids = [x['id'] for x in nodes]
            
            publications = []
            for publication_id in publication_ids: 
                publication = {
                    'publicationId': publication_id
                }
                publications.append(publication)

            for index, row in grouped_id_df.iterrows():
                if str(row['Published']).strip().lower() == 'true':
                    publish_entry = {
                        'id': row['id'],
                        'input': []
                    }
                    publish_entry['input'] = publications
                    datas.append(publish_entry)

        # Write product data to JSONL file
        with open(jsonl_file_path, 'w', encoding='utf-8') as outfile:
            for data in datas:
                outfile.write(json.dumps(data, ensure_ascii=False) + '\n')
        print(f"Successfully converted '{csv_file_path}' to '{jsonl_file_path}'")

    # Create
    # ===================================== Session ====================================
    def create_session(self):
        print("Creating session...")
        client = Client(timeout=None)
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

        self.create_variant(product_id=product_data['data']['productCreate']['product']['id'], variants=variables['variants'], media=variables['media'], strategy='REMOVE_STANDALONE_VARIANT')

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

    # ================================== Create Variant ===============================
    def create_variant(self, product_id, variants, media, strategy='DEFAULT'):
        print('Creating Variant...')
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
                            title
                            variants(first: 30){
                                nodes{
                                    sku
                                    media(first: 1){
                                        nodes{
                                            id
                                            alt
                                            preview{
                                                image{
                                                    url
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            createdAt
                            media(first: 30){
                                nodes{
                                    id
                                    alt
                                    preview{
                                        image{
                                            url
                                        }
                                    }
                                }
                            }
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

    # ============================= get_products_with_query ==========================
    # def get_products_with_query(self, variable_query):
    #     print('Getting products..')
    #     query = '''
    #         query(
    #             $query: String
    #         )
    #         {
    #             products(first: 250, query: $query) {
    #                 edges {
    #                     node {
    #                         handle
    #                         id
    #                         title
    #                         metafield(key: "vendor_sku"){
    #                             value
    #                         }
    #                     }
    #                 }
    #                 pageInfo {
    #                     endCursor
    #                     hasNextPage
    #                 }
    #             }
    #         }
    #     '''
    #     variables = variable_query

    #     return self.send_request(query=query, variables=variables)

    def get_products_with_pagination(self, variable_query, after=None):
        print('Getting products...')
        query = '''
            query(
                $query: String,
                $after: String
            )
            {
                products(first: 250, query: $query, after: $after) {
                    edges {
                        node {
                            handle
                            id
                            title
                            metafield(key: "vendor_sku", namespace: "custom") {
                                value
                            }
                            tags
                        }
                    }
                    pageInfo {
                        endCursor
                        hasNextPage
                    }
                }
            }
        '''

        variables = variable_query
        if after:
            variables['after'] = after

        return self.send_request(query=query, variables=variables)

    def get_product_variants_by_sku(self, variable_query, after=None):
        print('Getting products...')
        query = '''
            query(
                $query: String
            )
            {
                products(first: 3, query: $query) {
                    edges {
                        node {
                            handle
                            id
                            title
                            description
                            variants(first: 20){
                                nodes{
                                    compareAtPrice
                                    displayName
                                    inventoryItem{
                                        sku
                                    }
                                    inventoryQuantity
                                    price

                                }
                            }
                        }
                    }
                    pageInfo {
                        endCursor
                        hasNextPage
                    }
                }
            }
        '''

        variables = variable_query

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
    
    # =================================== Locations =================================
    def query_locations(self):
        print('Getting location...')
        query = '''
            {
                locations(first: 250) {
                    nodes{
                        id
                        name
                        activatable
                        hasActiveInventory
                        inventoryLevels(first: 250){
                            nodes{
                                id
                                item{
                                    sku
                                }
                                quantities(names: ["available"]){
                                    id
                                    name
                                    quantity
                                }
                            }
                        }
                        isActive
                    }
                }
            }
        '''

        return self.send_request(query=query)

    def get_product_tags(self):
        print('Getting product tags...')
        query = '''
            {
                productTags (first: 250){
                    edges{
                        node
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

    def publish_products(self, staged_target):
        print('Publishing products...')
        mutation = '''
            mutation ($stagedUploadPath: String!){
                bulkOperationRunMutation(
                    mutation: "mutation call($id: ID!, $input: [PublicationInput!]!){
                        publishablePublish(id: $id, input: $input){
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

        return response

    def update_quantities(self, staged_target):
        print('Set Quantities...')
        mutation = '''
            mutation ($stagedUploadPath: String!){
                bulkOperationRunMutation(
                    mutation: "mutation call($productId: ID!, $variants: [ProductVariantsBulkInput!]!, $strategy: ProductVariantsBulkCreateStrategy){
                        productVariantsBulkCreate(productId: $productId, variants: $variants, strategy: $strategy){
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

        return response
    
    def update_variants(self, staged_target):
        print('Update Variants...')
        mutation = '''
            mutation ($stagedUploadPath: String!){
                bulkOperationRunMutation(
                    mutation: "mutation call($productId: ID!, $variants: [ProductVariantsBulkInput!]!){
                        productVariantsBulkUpdate(productId: $productId, variants: $variants){
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

        return response

    # =========================== Update Files By Ids ============================
    def update_file(self, file_variables):
        """
        file_variables format [
            {
                "id": id,
                "filename": filename,
                "alt": alt 
            }
        ]
        """
        print('Updating Files...')
        file_mutation = '''
            mutation fileUpdate($files: [FileUpdateInput!]!) {
                fileUpdate(files: $files) {
                    files {
                        id
                        fileStatus
                    }
                    userErrors {
                        field
                        message
                    }
                }
            }
        '''

        return self.send_request(query=file_mutation, variables=file_variables)
    
    # def update_files(self, staged_target):
    #     print('Update Files...')
    #     mutation = '''
    #         mutation ($stagedUploadPath: String!){
    #             bulkOperationRunMutation(
    #                 mutation: "mutation call($files: [FileUpdateInput!]!){
    #                     fileUpdate(files: $files){
    #                         files {
    #                             id
    #                             fileStatus
    #                         }
    #                         userErrors {
    #                             message
    #                             field
    #                         } 
    #                     }
    #                 }",
    #                 stagedUploadPath: $stagedUploadPath
    #             )
    #             {
    #                 bulkOperation {
    #                     id
    #                     url
    #                     status
    #                 }
    #                 userErrors {
    #                     message
    #                     field
    #                 }
    #             }
    #         }
    #     '''

    #     variables = {
    #         "stagedUploadPath": staged_target['data']['stagedUploadsCreate']['stagedTargets'][0]['parameters'][3]['value']
    #     }

    #     response = self.send_request(query=mutation, variables=variables)

    #     return response

    def update_files(self, staged_target):
        print('Update Files...')
        mutation = '''
            mutation fileUpdateBulk($stagedUploadPath: String!) {
                bulkOperationRunMutation(
                    mutation: "mutation($id: ID!, $filename: String, $alt: String) { fileUpdate(files: {id: $id, filename: $filename, alt: $alt}) { files { id } userErrors { message field } } }",
                    stagedUploadPath: $stagedUploadPath
                ) {
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

        return response

    def update_files_for_import(self, csv_file_path, jsonl_file_path, bulk=False):
        df = pd.read_csv(csv_file_path)
        handles = df['Handle'].unique().tolist()
        
        product_response = s.get_products_id_by_handle(handles=handles)
        
        edges = product_response['data']['products']['edges']
        files = []
        for i, edge in enumerate(edges):
            for j, variant in enumerate(edge['node']['variants']['nodes']):
                for k, variant_media in enumerate(variant['media']['nodes']):
                    variant_file = {
                        'handle': edge['node']['handle'],
                        'id': variant_media['id'],
                        'alt': edge['node']['title'] + ' ' + 'Magic Cars Variant ' + str(j),
                        'url': variant_media['preview']['image']['url'],
                        'seq': str(j),
                        'src': 'magiccars-variant'
                    }
                    files.append(variant_file.copy())
                    
            for l, media in enumerate(edge['node']['media']['nodes']):
                media_file = {
                    'handle': edge['node']['handle'],
                    'id': media['id'],
                    'alt': edge['node']['title'] + ' ' + 'Magic Cars ' + str(l),
                    'url': media['preview']['image']['url'],
                    'seq': str(l),
                    'src': 'magiccars'
                }
                files.append(media_file.copy())

        file_list_raw = []
        for file in files: 
            file_variable = {
                "id": file['id'],
                "filename": file['handle'] + '-' + file['src'] + '-' + file['seq'] + '.' + file['url'].split('.')[-1].split("?")[0],
                "alt": file['alt']
            }
            file_list_raw.append(file_variable.copy())
        df = pd.DataFrame(file_list_raw)
        unique_df = df.drop_duplicates('id')
        file_list = unique_df.to_dict('records')

        chunked_file_list = self.chunk_list(file_list, chunk_size=50)
        if not bulk:
            for item in chunked_file_list:
                file_variables = {'files': item}
                self.update_file(file_variables)
                time.sleep(1)
        else:
            for item in chunked_file_list:
                with open(jsonl_file_path, 'w', encoding='utf-8') as outfile:
                    for data in item:
                        outfile.write(json.dumps(data, ensure_ascii=False) + '\n')
                print(f"Successfully converted file list to '{jsonl_file_path}'")
                staged_target = self.generate_staged_target()
                self.upload_jsonl(staged_target=staged_target, jsonl_path=jsonl_file_path)
                self.update_files(staged_target=staged_target)
                completed = False
                while not completed:
                    time.sleep(3)
                    response = self.pool_operation_status()
                    if response['data']['currentBulkOperation']['status'] == 'COMPLETED':
                        completed = True

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

        return response
    
    def create_variants(self, staged_target):
        print('Creating variants...')
        mutation = '''
            mutation ($stagedUploadPath: String!){
                bulkOperationRunMutation(
                    mutation: "mutation call($productId: ID!, $variants: [ProductVariantsBulkInput!]!, $strategy: ProductVariantsBulkCreateStrategy, $media: [CreateMediaInput!]){
                        productVariantsBulkCreate(productId: $productId, variants: $variants, strategy: $strategy, media: $media){
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

        return response

    def remove_tags(self, product_id, tags):
        print("Removing Tags...")
        mutation = '''
            mutation removeTags($id: ID!, $tags: [String!]!) {
                tagsRemove(id: $id, tags: $tags) {
                    node {
                        id
                    }
                    userErrors {
                        message
                    }
                }
            }
        '''
        remove_tags_variables = {
            "id": product_id,
            "tags": tags
        }

        return self.send_request(query=mutation, variables=remove_tags_variables)

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

    def import_bulk_data(self, csv_file_path, jsonl_file_path, locationId):
        print(f'Importing product from file {csv_file_path}')
        # Create products
        self.csv_to_jsonl(csv_file_path=csv_file_path, jsonl_file_path=jsonl_file_path, mode='product')
        staged_target = self.generate_staged_target()
        self.upload_jsonl(staged_target=staged_target, jsonl_path=jsonl_file_path)
        self.create_products(staged_target=staged_target)
        completed = False
        while not completed:
            time.sleep(3)
            response = self.pool_operation_status()
            if response['data']['currentBulkOperation']['status'] == 'COMPLETED':
                completed = True

        # Create variants
        self.csv_to_jsonl(csv_file_path=csv_file_path, jsonl_file_path=jsonl_file_path, mode='variant', locationId=locationId)
        staged_target = self.generate_staged_target()
        self.upload_jsonl(staged_target=staged_target, jsonl_path=jsonl_file_path)
        self.create_variants(staged_target=staged_target)
        completed = False
        while not completed:
            time.sleep(3)
            response = self.pool_operation_status()
            if response['data']['currentBulkOperation']['status'] == 'COMPLETED':
                completed = True

        # Publish product
        self.csv_to_jsonl(csv_file_path=csv_file_path, jsonl_file_path=jsonl_file_path, mode='publish')
        staged_target = self.generate_staged_target()
        self.upload_jsonl(staged_target=staged_target, jsonl_path=jsonl_file_path)
        self.publish_products(staged_target=staged_target)
        completed = False
        while not completed:
            time.sleep(3)
            response = self.pool_operation_status()
            if response['data']['currentBulkOperation']['status'] == 'COMPLETED':
                completed = True

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
    # Usage
    load_dotenv('./.prd.env')

    # ============================== Create Session ====================================
    s = ShopifyApp(
        store_name=os.getenv('STORE_NAME'),
        access_token=os.getenv('ACCESS_TOKEN'),
        api_version=os.getenv('SHOPIFY_API_VERSION')
    )

    s.create_session()

    # ================================== Query Shop ====================================
    # s.query_shop()

    # ================================ Query Product ===================================
    # s.query_products()

    # ============================== Query Publications ================================
    # s.query_publication()

    # ============================== Query Locations ================================
    # s.query_locations()

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
    # handles = ['1-seaters-ride-on-car-truck-battery-power-kids-electric-car-with-remote-12v-12709']
    # s.get_products_id_by_handle(handles=handles)

    # ============================== Update Files by IDs ===============================
    # s.update_files_for_import('./data/import200.csv')

    # ================================= CSV to JSONL ==================================
    # s.csv_to_jsonl(csv_file_path='./data/import201_test.csv', jsonl_file_path='./data/bulk_op_vars_test.jsonl', mode='variant', locationId='gid://shopify/Location/47387978')

    # ================================= Staged Target ==================================
    # staged_target = s.generate_staged_target()

    # ================================= Upload JSONL ===================================
    # s.upload_jsonl(staged_target=staged_target, jsonl_path="./data/bulk_op_vars.jsonl")

    # create_products_flag = input('Press any key to continue')

    # =============================== Bulk Create Products =============================
    # s.create_products(staged_target=staged_target)

    # ==================================== Chunk Data ==================================
    # s.chunk_shopify_csv_by_product(input_csv_path='./data/products_export_2.csv', output_directory='./data/chunked', products_per_chunk=200)
    
    # =============================== bulk import products =============================
    # s.import_bulk_data(csv_file_path='./data/import201_test.csv', jsonl_file_path='./data/bulk_op_vars.jsonl', locationId='gid://shopify/Location/76200411326')
    # s.import_bulk_data(csv_file_path='./data/import201_test.csv', jsonl_file_path='./data/bulk_op_vars.jsonl', locationId='gid://shopify/Location/47387978') # prod

    # =============================== Update Files =============================
    # s.update_files_for_import(csv_file_path='./data/import200.csv', jsonl_file_path='./data/bulk_op_vars.jsonl', bulk=False)

    # ============================== pull operation status =============================
    # stopper = '0'
    # while stopper != '1':
    #     s.pool_operation_status()
    #     stopper = input('Do you want to stop monitoring? [1(Yes) or 0(No)]')

    # =============================== webhook_subscription =============================
    # s.webhook_subscription()

    # s.create_collection(client)
    # s.get_collections(client)
    # print(s.check_bulk_operation_status(client, bulk_operation_id='gid://shopify/BulkOperation/3252439023930'))

    # Fetch products with date filter
    # var_query = {'query': "created_at>:{}".format('2025-08-15T04:24:54Z')}
    # records = []
    # var_query = {'query': "created_at:>'2025-08-15T04:24:54Z'"}
    # s.get_products_id_by_handle(['weelye-24v-30000rpm-gearbox-24v-dc-motor-for-kids-ride-on-car-suv-parts-electr-33505'])
    # response = s.get_products_with_query(variable_query=var_query)
    # edges = response['data']['products']['edges']
    # record = [i['node'] for i in edges]
    # records.extend(record.copy())
    # has_next_page = response['data']['products']['pageInfo']['hasNextPage']
    # cursor = response['data']['products']['pageInfo']['endCursor']
    # while has_next_page:
    #     var_query = {'query': "created_at:>'2025-08-15T04:24:54Z'", 'after': cursor}
    #     response = s.get_products_with_query(variable_query=var_query)
    #     edges = response['data']['products']['edges']
    #     record = [i['node'] for i in edges]
    #     records.extend(record.copy())
    #     has_next_page = response['data']['products']['pageInfo']['hasNextPage']
    #     cursor = response['data']['products']['pageInfo']['endCursor']

    # df = pd.DataFrame.from_records(records)
    # df.to_csv('data/uploaded_data.csv', index=False)

    # =============================== get products using filter =============================
    records = []
    var_query = {'query': "created_at:>'2025-08-15T04:24:54Z'"}
    cursor = None
    has_next_page = True

    while has_next_page:
        response = s.get_products_with_pagination(variable_query=var_query, after=cursor)
        edges = response['data']['products']['edges']
        record = [i['node'] for i in edges]
        records.extend(record)
        has_next_page = response['data']['products']['pageInfo']['hasNextPage']
        cursor = response['data']['products']['pageInfo']['endCursor']

    df = pd.DataFrame.from_records(records)
    df.to_csv('data/uploaded_data.csv', index=False)

    # =============================== get_tags =============================
    # s.get_product_tags()