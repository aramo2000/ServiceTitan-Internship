#!/usr/bin/env python
# coding: utf-8

# In[110]:


import pandas as pd
import numpy as np
from datetime import datetime
import pickle

class DataExtractor:
    def __init__(self, data_file, expired_invoices_file):
        self.data_file = data_file
        self.data = self.load_data()
        self.expired_invoices_file = expired_invoices_file
        self.expired_invoices = self.load_expired_invoices()
        
    def load_data(self):
        with open(self.data_file, 'rb') as file:
            data = pickle.load(file)
        return data

    def load_expired_invoices(self):
        with open(self.expired_invoices_file, 'r') as file:
            expired_invoices = file.read().split(", ")
        return expired_invoices

    def transform_data(self):
        type_conversion = {'O': 'Material', 0: 'Material', 1: 'Equipment', 2: 'Service', 3: 'Other'}
        
        transformed_data = []

        for invoice in self.data:
            invoice_id = str(invoice['id']).replace('O', '')
            try:
                created_on = pd.to_datetime(invoice['created_on'])
            except ValueError:
                # Set created_on to NaT if date parsing fails
                created_on = pd.NaT

            if 'items' in invoice:  # Check if 'items' key exists in the invoice
                for item in invoice['items']:
                    if item['quantity'] == 'ten':
                        item['quantity'] = 10
                    if item['quantity'] == 'five':
                        item['quantity'] = 5
            if 'items' in invoice:
                invoice_total = sum(item['item']['unit_price'] * item['quantity'] for item in invoice['items'])
            else:
                invoice_total = 0
        
            for item in invoice.get('items', []):
                invoiceitem_id = item['item'].get('id')
                invoiceitem_name = item['item'].get('name')
                type_str = type_conversion.get(item['item'].get('type'))
                unit_price = item['item'].get('unit_price')
                quantity = item['quantity']
                total_price = unit_price * quantity if unit_price is not None and quantity is not None else None
                percentage_in_invoice = total_price / invoice_total if total_price is not None else None
                is_expired = invoice_id in self.expired_invoices

                transformed_data.append({
                    'invoice_id': int(invoice_id),
                    'created_on': created_on,
                    'invoiceitem_id': invoiceitem_id,
                    'invoiceitem_name': invoiceitem_name,
                    'type': type_str,
                    'unit_price': unit_price,
                    'total_price': total_price,
                    'percentage_in_invoice': percentage_in_invoice,
                    'is_expired': is_expired
                })

            # Append empty entry if there are no items in the invoice
            if not invoice.get('items'):
                transformed_data.append({
                    'invoice_id': int(invoice_id),
                    'created_on': created_on,
                    'invoiceitem_id': None,
                    'invoiceitem_name': None,
                    'type': None,
                    'unit_price': None,
                    'total_price': None,
                    'percentage_in_invoice': None,
                    'is_expired': invoice_id in self.expired_invoices
                })

        # Create DataFrame
        df = pd.DataFrame(transformed_data)
        
        data_types = {
            'invoice_id': 'int64',
            'created_on': 'datetime64[ns]',
            'invoiceitem_id': 'float64',
            'invoiceitem_name': 'str',
            'type': 'str',
            'unit_price': 'float64',
            'total_price': 'float64',
            'percentage_in_invoice': 'float64',
            'is_expired': 'bool'
        }
        
        if df['invoiceitem_id'].isnull().all():
            data_types.update({
                'invoiceitem_id': 'object',
                'unit_price': 'object',
                'total_price': 'object'
            })

        df = df.astype(data_types)
        
        df['invoiceitem_name'] = df.apply(lambda row: None if pd.isnull(row['invoiceitem_id']) else row['invoiceitem_name'], axis=1)
        df['type'] = df.apply(lambda row: None if pd.isnull(row['invoiceitem_id']) else row['type'], axis=1)

        # Sort by invoice_id and invoiceitem_id
        df = df.sort_values(by=['invoice_id', 'invoiceitem_id'])
        
        return df
    
    def export_data(self, path):
        file_name = path
        df.to_csv(file_name, index=False)


# In[111]:


extractor = DataExtractor('invoices_new.pkl', "expired_invoices.txt")
df = extractor.transform_data()
print(df)
extractor.export_data("output.csv")

