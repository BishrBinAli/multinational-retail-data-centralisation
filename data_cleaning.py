import pandas as pd
import re
import numpy as np

class DataCleaning:
    def clean_user_data(self, user_df):

        # Removing rows with no user_uuid
        cleaned_user_df = user_df[user_df['user_uuid'] != 'NULL']

        # Converting date columns to datetime
        cleaned_user_df['date_of_birth'] = pd.to_datetime(
            cleaned_user_df['date_of_birth'], infer_datetime_format=True, errors='coerce')
        cleaned_user_df['join_date'] = pd.to_datetime(
            cleaned_user_df['join_date'], infer_datetime_format=True, errors='coerce')

        # Removing rows with data that does not make sense
        cleaned_user_df = cleaned_user_df[cleaned_user_df['date_of_birth'].notna(
        )]

        # Removing rows with join date before date of birth
        cleaned_user_df = cleaned_user_df[~(cleaned_user_df['date_of_birth'] > cleaned_user_df['join_date'])]

        # Converting country and country code columns to category type
        cleaned_user_df['country'] = cleaned_user_df['country'].astype('category')
        cleaned_user_df['country_code'] = cleaned_user_df['country_code'].astype('category')
        
        # Setting country code values according to country
        country_code_dict = {
            'United Kingdom' : 'GB',
            'United States' : 'US',
            'Germany' : 'DE'
        }
        cleaned_user_df['country_code'] = cleaned_user_df['country'].map(country_code_dict)

        # Making all the phone numbers to a single format and
        # seperating extensions to another column
        def format_phone_num(x):
            phone_num = x['phone_number']
            country = x['country']
            extension = ''
            extension_index = phone_num.find('x')
            if extension_index != -1:
                extension = phone_num[extension_index+1:]
                phone_num = phone_num[:extension_index]
            else:
                extension = np.nan

            phone_num = re.sub('\D',"", phone_num)
            phone_num = phone_num[-10:]
            dial_code_dict = {
                'United States': '+1',
                'United Kingdom': '+44',
                'Germany': '+49'
            }
            phone_num = dial_code_dict[country] + phone_num

            return [phone_num, extension]

        cleaned_user_df[['phone_number', 'phone_extension']] = cleaned_user_df.apply(format_phone_num, axis=1, result_type='expand')

        # Resetting index
        cleaned_user_df.reset_index(inplace=True, drop=True)
        
        return cleaned_user_df


    def clean_card_data(self, card_df):

        # Removing rows with NULL values
        card_df = card_df[card_df['card_number'] != 'NULL']

        # Removing rows with data that does not make sense
        # Removing rows with alphabets in card number
        card_df = card_df[~card_df['card_number'].str.contains('[a-zA-z]', na=False, regex=True)]

        # Removing '?' from card_numbers
        card_df['card_number'] = card_df['card_number'].astype('str').str.replace("?", "")

        # Convert date column to datetype
        card_df['date_payment_confirmed'] = pd.to_datetime(card_df['date_payment_confirmed'], infer_datetime_format=True, errors='coerce')

        # Convert card_provider to category type 
        card_df['card_provider'] = card_df['card_provider'].astype('category')

        return card_df


    def clean_store_data(self, store_df):

        # Replacing NULL and N/A values with np.nan
        store_df = store_df.replace(dict.fromkeys(['NULL', 'N/A'], np.nan))
        # Removing rows with all NULL values
        store_df.dropna(how='all', inplace=True)

        # Removing rows with nonsensical data
        store_df = store_df[~store_df['locality'].str.contains('\d', na=False)]

        # Correct continent values with extra characters
        store_df.loc[store_df['continent'].str.contains('Europe', na=False), 'continent'] = 'Europe'
        store_df.loc[store_df['continent'].str.contains('America', na=False), 'continent'] = 'America'
        
        # Remove alphabets from staff_numbers
        store_df['staff_numbers'] = store_df['staff_numbers'].str.replace('\D',"", regex=True)

        # Convert country_code, continent, store_type columns to category type 
        store_df['country_code'] = store_df['country_code'].astype('category')
        store_df['continent'] = store_df['continent'].astype('category')
        store_df['store_type'] =store_df['store_type'].astype('category')

        # Convert opening_date column to datetype
        store_df['opening_date'] = pd.to_datetime(store_df['opening_date'], infer_datetime_format=True, errors='coerce')

        # Replacing \n in the address column with ,
        store_df['address'] = store_df['address'].str.replace('\\n', ", ")

        return store_df

        